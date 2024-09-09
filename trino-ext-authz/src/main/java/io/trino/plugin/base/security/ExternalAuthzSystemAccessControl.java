package io.trino.plugin.base.security;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.inject.Injector;
import io.airlift.bootstrap.Bootstrap;
import io.airlift.bootstrap.LifeCycleManager;
import io.airlift.log.Logger;
import io.airlift.units.Duration;
import io.trino.spi.QueryId;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.*;
import io.trino.spi.eventlistener.EventListener;
import io.trino.spi.function.FunctionKind;
import io.trino.spi.function.SchemaFunctionName;
import io.trino.spi.security.Identity;
import io.trino.spi.security.Privilege;
import io.trino.spi.security.SystemAccessControl;
import io.trino.spi.security.SystemAccessControlFactory;
import io.trino.spi.security.SystemSecurityContext;
import io.trino.spi.security.TrinoPrincipal;
import io.trino.spi.security.ViewExpression;
import io.trino.spi.type.Type;

import java.nio.file.Paths;
import java.security.Principal;
import java.util.*;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.regex.Pattern;

import static com.google.common.base.Suppliers.memoizeWithExpiration;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.airlift.configuration.ConfigBinder.configBinder;
import static io.trino.plugin.base.security.CatalogAccessControlRule.AccessMode.*;
import static io.trino.plugin.base.security.CatalogAccessControlRule.AccessMode.OWNER;
import static io.trino.plugin.base.security.FileBasedAccessControlConfig.SECURITY_REFRESH_PERIOD;
import static io.trino.plugin.base.security.TableAccessControlRule.TablePrivilege.*;
import static io.trino.plugin.base.security.TableAccessControlRule.TablePrivilege.GRANT_SELECT;
import static io.trino.plugin.base.util.JsonUtils.parseJson;
import static io.trino.spi.StandardErrorCode.CONFIGURATION_INVALID;
import static io.trino.spi.StandardErrorCode.INVALID_COLUMN_MASK;
import static io.trino.spi.security.AccessDeniedException.*;
import static io.trino.spi.security.AccessDeniedException.denyGrantTablePrivilege;
import static io.trino.spi.security.AccessDeniedException.denyRevokeSchemaPrivilege;
import static io.trino.spi.security.AccessDeniedException.denyRevokeTablePrivilege;
import static java.lang.String.format;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class ExternalAuthzSystemAccessControl implements SystemAccessControl {
    private static final Logger LOG = Logger.get(ExternalAuthzSystemAccessControl.class);
    public static final String NAME = "extauthz";
    private static final String INFORMATION_SCHEMA_NAME = "information_schema";


    private final LifeCycleManager lifeCycleManager;
    private final List<CatalogAccessControlRule> catalogRules;
    private final Optional<List<QueryAccessRule>> queryAccessRules;
    private final Optional<List<ImpersonationRule>> impersonationRules;
    private final Optional<List<PrincipalUserMatchRule>> principalUserMatchRules;
    private final Optional<List<SystemInformationRule>> systemInformationRules;
    private final List<AuthorizationRule> authorizationRules;
    private final List<CatalogSchemaAccessControlRule> schemaRules;
    private final List<CatalogTableAccessControlRule> tableRules;
    private final List<SessionPropertyAccessControlRule> sessionPropertyRules;
    private final List<CatalogSessionPropertyAccessControlRule> catalogSessionPropertyRules;
    private final List<CatalogFunctionAccessControlRule> functionRules;
    private final List<CatalogProcedureAccessControlRule> procedureRules;
    private final Set<AnyCatalogPermissionsRule> anyCatalogPermissionsRules;
    private final Set<AnyCatalogSchemaPermissionsRule> anyCatalogSchemaPermissionsRules;

    private ExternalAuthzSystemAccessControl(
            LifeCycleManager lifeCycleManager,
            List<CatalogAccessControlRule> catalogRules,
            Optional<List<QueryAccessRule>> queryAccessRules,
            Optional<List<ImpersonationRule>> impersonationRules,
            Optional<List<PrincipalUserMatchRule>> principalUserMatchRules,
            Optional<List<SystemInformationRule>> systemInformationRules,
            List<AuthorizationRule> authorizationRules,
            List<CatalogSchemaAccessControlRule> schemaRules,
            List<CatalogTableAccessControlRule> tableRules,
            List<SessionPropertyAccessControlRule> sessionPropertyRules,
            List<CatalogSessionPropertyAccessControlRule> catalogSessionPropertyRules,
            List<CatalogFunctionAccessControlRule> functionRules,
            List<CatalogProcedureAccessControlRule> procedureRules)
    {
        this.lifeCycleManager = requireNonNull(lifeCycleManager, "lifeCycleManager is null");
        this.catalogRules = catalogRules;
        this.queryAccessRules = queryAccessRules;
        this.impersonationRules = impersonationRules;
        this.principalUserMatchRules = principalUserMatchRules;
        this.systemInformationRules = systemInformationRules;
        this.authorizationRules = authorizationRules;
        this.schemaRules = schemaRules;
        this.tableRules = tableRules;
        this.sessionPropertyRules = sessionPropertyRules;
        this.catalogSessionPropertyRules = catalogSessionPropertyRules;
        this.functionRules = functionRules;
        this.procedureRules = procedureRules;

        ImmutableSet.Builder<AnyCatalogPermissionsRule> anyCatalogPermissionsRules = ImmutableSet.builder();
        schemaRules.stream()
                .map(CatalogSchemaAccessControlRule::toAnyCatalogPermissionsRule)
                .flatMap(Optional::stream)
                .forEach(anyCatalogPermissionsRules::add);
        tableRules.stream()
                .map(CatalogTableAccessControlRule::toAnyCatalogPermissionsRule)
                .flatMap(Optional::stream)
                .forEach(anyCatalogPermissionsRules::add);
        catalogSessionPropertyRules.stream()
                .map(CatalogSessionPropertyAccessControlRule::toAnyCatalogPermissionsRule)
                .flatMap(Optional::stream)
                .forEach(anyCatalogPermissionsRules::add);
        functionRules.stream()
                .map(CatalogFunctionAccessControlRule::toAnyCatalogPermissionsRule)
                .flatMap(Optional::stream)
                .forEach(anyCatalogPermissionsRules::add);
        procedureRules.stream()
                .map(CatalogProcedureAccessControlRule::toAnyCatalogPermissionsRule)
                .flatMap(Optional::stream)
                .forEach(anyCatalogPermissionsRules::add);
        this.anyCatalogPermissionsRules = anyCatalogPermissionsRules.build();

        ImmutableSet.Builder<AnyCatalogSchemaPermissionsRule> anyCatalogSchemaPermissionsRules = ImmutableSet.builder();
        schemaRules.stream()
                .map(CatalogSchemaAccessControlRule::toAnyCatalogSchemaPermissionsRule)
                .flatMap(Optional::stream)
                .forEach(anyCatalogSchemaPermissionsRules::add);
        tableRules.stream()
                .map(CatalogTableAccessControlRule::toAnyCatalogSchemaPermissionsRule)
                .flatMap(Optional::stream)
                .forEach(anyCatalogSchemaPermissionsRules::add);
        functionRules.stream()
                .map(CatalogFunctionAccessControlRule::toAnyCatalogSchemaPermissionsRule)
                .flatMap(Optional::stream)
                .forEach(anyCatalogSchemaPermissionsRules::add);
        procedureRules.stream()
                .map(CatalogProcedureAccessControlRule::toAnyCatalogSchemaPermissionsRule)
                .flatMap(Optional::stream)
                .forEach(anyCatalogSchemaPermissionsRules::add);
        this.anyCatalogSchemaPermissionsRules = anyCatalogSchemaPermissionsRules.build();
    }

    public static class Factory
            implements SystemAccessControlFactory
    {
        @Override
        public String getName()
        {
            return NAME;
        }

        @Override
        public SystemAccessControl create(Map<String, String> config)
        {
            requireNonNull(config, "config is null");

            Bootstrap bootstrap = new Bootstrap(
                    binder -> configBinder(binder).bindConfig(FileBasedAccessControlConfig.class),
                    new FileBasedSystemAccessControlModule());

            Injector injector = bootstrap
                    .doNotInitializeLogging()
                    .setRequiredConfigurationProperties(config)
                    .initialize();

            return injector.getInstance(SystemAccessControl.class);
        }
    }

    @Override
    public void checkCanImpersonateUser(Identity identity, String userName)
    {
        if (impersonationRules.isEmpty()) {
            // if there are principal user match rules, we assume that impersonation checks are
            // handled there; otherwise, impersonation must be manually configured
            if (principalUserMatchRules.isEmpty()) {
                denyImpersonateUser(identity.getUser(), userName);
            }
            return;
        }

        for (ImpersonationRule rule : impersonationRules.get()) {
            Optional<Boolean> allowed = rule.match(identity.getUser(), identity.getEnabledRoles(), userName);
            if (allowed.isPresent()) {
                if (allowed.get()) {
                    return;
                }
                denyImpersonateUser(identity.getUser(), userName);
            }
        }

        denyImpersonateUser(identity.getUser(), userName);
    }

    @Override
    public void checkCanSetUser(Optional<Principal> principal, String userName)
    {
        requireNonNull(principal, "principal is null");
        requireNonNull(userName, "userName is null");

        if (principalUserMatchRules.isEmpty()) {
            return;
        }

        if (principal.isEmpty()) {
            denySetUser(principal, userName);
        }

        String principalName = principal.get().getName();

        for (PrincipalUserMatchRule rule : principalUserMatchRules.get()) {
            Optional<Boolean> allowed = rule.match(principalName, userName);
            if (allowed.isPresent()) {
                if (allowed.get()) {
                    return;
                }
                denySetUser(principal, userName);
            }
        }

        denySetUser(principal, userName);
    }

    @Override
    public void checkCanExecuteQuery(Identity identity)
    {
        if (!canAccessQuery(identity, Optional.empty(), QueryAccessRule.AccessMode.EXECUTE)) {
            denyExecuteQuery();
        }
    }

    @Override
    public void checkCanExecuteQuery(Identity identity, QueryId queryId)
    {
        checkCanExecuteQuery(identity);
    }

    @Override
    public void checkCanViewQueryOwnedBy(Identity identity, Identity queryOwner)
    {
        if (!canAccessQuery(identity, Optional.of(queryOwner.getUser()), QueryAccessRule.AccessMode.VIEW)) {
            denyViewQuery();
        }
    }

    @Override
    public Collection<Identity> filterViewQueryOwnedBy(Identity identity, Collection<Identity> queryOwners)
    {
        if (queryAccessRules.isEmpty()) {
            return queryOwners;
        }
        return queryOwners.stream()
                .filter(owner -> canAccessQuery(identity, Optional.of(owner.getUser()), QueryAccessRule.AccessMode.VIEW))
                .collect(toImmutableSet());
    }

    @Override
    public void checkCanKillQueryOwnedBy(Identity identity, Identity queryOwner)
    {
        if (!canAccessQuery(identity, Optional.of(queryOwner.getUser()), QueryAccessRule.AccessMode.KILL)) {
            denyKillQuery();
        }
    }

    private boolean canAccessQuery(Identity identity, Optional<String> queryOwner, QueryAccessRule.AccessMode requiredAccess)
    {
        if (queryAccessRules.isEmpty()) {
            return true;
        }
        for (QueryAccessRule rule : queryAccessRules.get()) {
            Optional<Set<QueryAccessRule.AccessMode>> accessMode = rule.match(identity.getUser(), identity.getEnabledRoles(), identity.getGroups(), queryOwner);
            if (accessMode.isPresent()) {
                return accessMode.get().contains(requiredAccess);
            }
        }
        return false;
    }

    @Override
    public void checkCanReadSystemInformation(Identity identity)
    {
        if (!checkCanSystemInformation(identity, SystemInformationRule.AccessMode.READ)) {
            denyReadSystemInformationAccess();
        }
    }

    @Override
    public void checkCanWriteSystemInformation(Identity identity)
    {
        if (!checkCanSystemInformation(identity, SystemInformationRule.AccessMode.WRITE)) {
            denyWriteSystemInformationAccess();
        }
    }

    private boolean checkCanSystemInformation(Identity identity, SystemInformationRule.AccessMode requiredAccess)
    {
        for (SystemInformationRule rule : systemInformationRules.orElseGet(ImmutableList::of)) {
            Optional<Set<SystemInformationRule.AccessMode>> accessMode = rule.match(identity.getUser(), identity.getEnabledRoles());
            if (accessMode.isPresent()) {
                return accessMode.get().contains(requiredAccess);
            }
        }
        return false;
    }

    @Override
    public void checkCanSetSystemSessionProperty(Identity identity, String propertyName)
    {
        boolean allowed = sessionPropertyRules.stream()
                .map(rule -> rule.match(identity.getUser(), identity.getEnabledRoles(), identity.getGroups(), propertyName))
                .flatMap(Optional::stream)
                .findFirst()
                .orElse(false);
        if (!allowed) {
            denySetSystemSessionProperty(propertyName);
        }
    }

    @Override
    public void checkCanSetSystemSessionProperty(Identity identity, QueryId queryId, String propertyName)
    {
        checkCanSetSystemSessionProperty(identity, propertyName);
    }

    @Override
    public boolean canAccessCatalog(SystemSecurityContext context, String catalogName)
    {
        return canAccessCatalog(context, catalogName, READ_ONLY);
    }

    @Override
    public void checkCanCreateCatalog(SystemSecurityContext context, String catalogName)
    {
        if (!canAccessCatalog(context, catalogName, OWNER)) {
            denyCreateCatalog(catalogName);
        }
    }

    @Override
    public void checkCanDropCatalog(SystemSecurityContext context, String catalogName)
    {
        if (!canAccessCatalog(context, catalogName, OWNER)) {
            denyDropCatalog(catalogName);
        }
    }

    @Override
    public Set<String> filterCatalogs(SystemSecurityContext context, Set<String> catalogs)
    {
        ImmutableSet.Builder<String> filteredCatalogs = ImmutableSet.builder();
        for (String catalog : catalogs) {
            if (checkAnyCatalogAccess(context, catalog)) {
                filteredCatalogs.add(catalog);
            }
        }
        return filteredCatalogs.build();
    }

    @Override
    public void checkCanCreateSchema(SystemSecurityContext context, CatalogSchemaName schema, Map<String, Object> properties)
    {
        if (!isSchemaOwner(context, schema)) {
            denyCreateSchema(schema.toString());
        }
    }

    @Override
    public void checkCanDropSchema(SystemSecurityContext context, CatalogSchemaName schema)
    {
        if (!isSchemaOwner(context, schema)) {
            denyDropSchema(schema.toString());
        }
    }

    @Override
    public void checkCanRenameSchema(SystemSecurityContext context, CatalogSchemaName schema, String newSchemaName)
    {
        if (!isSchemaOwner(context, schema) || !isSchemaOwner(context, new CatalogSchemaName(schema.getCatalogName(), newSchemaName))) {
            denyRenameSchema(schema.toString(), newSchemaName);
        }
    }

    @Override
    public void checkCanSetSchemaAuthorization(SystemSecurityContext context, CatalogSchemaName schema, TrinoPrincipal principal)
    {
        if (!isSchemaOwner(context, schema)) {
            denySetSchemaAuthorization(schema.toString(), principal);
        }
        if (!checkCanSetAuthorization(context, principal)) {
            denySetSchemaAuthorization(schema.toString(), principal);
        }
    }

    @Override
    public void checkCanShowSchemas(SystemSecurityContext context, String catalogName)
    {
        if (!checkAnyCatalogAccess(context, catalogName)) {
            denyShowSchemas();
        }
    }

    @Override
    public Set<String> filterSchemas(SystemSecurityContext context, String catalogName, Set<String> schemaNames)
    {
        return schemaNames.stream()
                .filter(schemaName -> checkAnySchemaAccess(context, catalogName, schemaName))
                .collect(toImmutableSet());
    }

    @Override
    public void checkCanShowCreateTable(SystemSecurityContext context, CatalogSchemaTableName table)
    {
        if (!checkTablePermission(context, table, OWNERSHIP)) {
            denyShowCreateTable(table.toString());
        }
    }

    @Override
    public void checkCanShowCreateSchema(SystemSecurityContext context, CatalogSchemaName schemaName)
    {
        if (!isSchemaOwner(context, schemaName)) {
            denyShowCreateSchema(schemaName.toString());
        }
    }

    @Override
    public void checkCanCreateTable(SystemSecurityContext context, CatalogSchemaTableName table, Map<String, Object> properties)
    {
        // check if user will be an owner of the table after creation
        if (!checkTablePermission(context, table, OWNERSHIP)) {
            denyCreateTable(table.toString());
        }
    }

    @Override
    public void checkCanDropTable(SystemSecurityContext context, CatalogSchemaTableName table)
    {
        if (!checkTablePermission(context, table, OWNERSHIP)) {
            denyDropTable(table.toString());
        }
    }

    @Override
    public void checkCanTruncateTable(SystemSecurityContext context, CatalogSchemaTableName table)
    {
        if (!checkTablePermission(context, table, DELETE)) {
            denyTruncateTable(table.toString());
        }
    }

    @Override
    public void checkCanRenameTable(SystemSecurityContext context, CatalogSchemaTableName table, CatalogSchemaTableName newTable)
    {
        // check if user is an owner current table and will be an owner of the renamed table
        if (!checkTablePermission(context, table, OWNERSHIP) || !checkTablePermission(context, newTable, OWNERSHIP)) {
            denyRenameTable(table.toString(), newTable.toString());
        }
    }

    @Override
    public void checkCanSetTableProperties(SystemSecurityContext context, CatalogSchemaTableName table, Map<String, Optional<Object>> properties)
    {
        if (!checkTablePermission(context, table, OWNERSHIP)) {
            denySetTableProperties(table.toString());
        }
    }

    @Override
    public void checkCanSetTableComment(SystemSecurityContext context, CatalogSchemaTableName table)
    {
        if (!checkTablePermission(context, table, OWNERSHIP)) {
            denyCommentTable(table.toString());
        }
    }

    @Override
    public void checkCanSetViewComment(SystemSecurityContext context, CatalogSchemaTableName view)
    {
        if (!checkTablePermission(context, view, OWNERSHIP)) {
            denyCommentView(view.toString());
        }
    }

    @Override
    public void checkCanSetColumnComment(SystemSecurityContext context, CatalogSchemaTableName table)
    {
        if (!checkTablePermission(context, table, OWNERSHIP)) {
            denyCommentColumn(table.toString());
        }
    }

    @Override
    public void checkCanShowTables(SystemSecurityContext context, CatalogSchemaName schema)
    {
        if (!checkAnySchemaAccess(context, schema.getCatalogName(), schema.getSchemaName())) {
            denyShowTables(schema.toString());
        }
    }

    @Override
    public Set<SchemaTableName> filterTables(SystemSecurityContext context, String catalogName, Set<SchemaTableName> tableNames)
    {
        return tableNames.stream()
                .filter(tableName -> isSchemaOwner(context, new CatalogSchemaName(catalogName, tableName.getSchemaName())) ||
                        checkAnyTablePermission(context, new CatalogSchemaTableName(catalogName, tableName)))
                .collect(toImmutableSet());
    }

    @Override
    public void checkCanShowColumns(SystemSecurityContext context, CatalogSchemaTableName table)
    {
        if (!checkAnyTablePermission(context, table)) {
            denyShowColumns(table.toString());
        }
    }

    @Override
    public Set<String> filterColumns(SystemSecurityContext context, CatalogSchemaTableName tableName, Set<String> columns)
    {
        if (!checkAnyTablePermission(context, tableName)) {
            return ImmutableSet.of();
        }

        if (INFORMATION_SCHEMA_NAME.equals(tableName.getSchemaTableName().getSchemaName())) {
            return columns;
        }

        Identity identity = context.getIdentity();
        CatalogTableAccessControlRule rule = tableRules.stream()
                .filter(tableRule -> tableRule.matches(identity.getUser(), identity.getEnabledRoles(), identity.getGroups(), tableName))
                .findFirst()
                .orElse(null);
        if (rule == null || rule.getPrivileges().isEmpty()) {
            return ImmutableSet.of();
        }

        // if user has privileges other than select, show all columns
        if (rule.getPrivileges().stream().anyMatch(privilege -> SELECT != privilege && GRANT_SELECT != privilege)) {
            return columns;
        }

        Set<String> restrictedColumns = rule.getRestrictedColumns();
        return columns.stream()
                .filter(column -> !restrictedColumns.contains(column))
                .collect(toImmutableSet());
    }

    @Override
    public Map<SchemaTableName, Set<String>> filterColumns(SystemSecurityContext context, String catalogName, Map<SchemaTableName, Set<String>> tableColumns)
    {
        // Default implementation is good enough. Explicit implementation is expected by the test though.
        return SystemAccessControl.super.filterColumns(context, catalogName, tableColumns);
    }

    @Override
    public void checkCanAddColumn(SystemSecurityContext context, CatalogSchemaTableName table)
    {
        if (!checkTablePermission(context, table, OWNERSHIP)) {
            denyAddColumn(table.toString());
        }
    }

    @Override
    public void checkCanDropColumn(SystemSecurityContext context, CatalogSchemaTableName table)
    {
        if (!checkTablePermission(context, table, OWNERSHIP)) {
            denyDropColumn(table.toString());
        }
    }

    @Override
    public void checkCanRenameColumn(SystemSecurityContext context, CatalogSchemaTableName table)
    {
        if (!checkTablePermission(context, table, OWNERSHIP)) {
            denyRenameColumn(table.toString());
        }
    }

    @Override
    public void checkCanAlterColumn(SystemSecurityContext context, CatalogSchemaTableName table)
    {
        if (!checkTablePermission(context, table, OWNERSHIP)) {
            denyAlterColumn(table.toString());
        }
    }

    @Override
    public void checkCanSetTableAuthorization(SystemSecurityContext context, CatalogSchemaTableName table, TrinoPrincipal principal)
    {
        if (!checkTablePermission(context, table, OWNERSHIP)) {
            denySetTableAuthorization(table.toString(), principal);
        }
        if (!checkCanSetAuthorization(context, principal)) {
            denySetTableAuthorization(table.toString(), principal);
        }
    }

    @Override
    public void checkCanSelectFromColumns(SystemSecurityContext context, CatalogSchemaTableName table, Set<String> columns)
    {
        if (!canAccessCatalog(context, table.getCatalogName(), READ_ONLY)) {
            denySelectTable(table.toString());
        }

        if (INFORMATION_SCHEMA_NAME.equals(table.getSchemaTableName().getSchemaName())) {
            return;
        }

        Identity identity = context.getIdentity();
        boolean allowed = tableRules.stream()
                .filter(rule -> rule.matches(identity.getUser(), identity.getEnabledRoles(), identity.getGroups(), table))
                .map(rule -> rule.canSelectColumns(columns))
                .findFirst()
                .orElse(false);
        if (!allowed) {
            denySelectTable(table.toString());
        }
    }

    @Override
    public void checkCanInsertIntoTable(SystemSecurityContext context, CatalogSchemaTableName table)
    {
        if (!checkTablePermission(context, table, INSERT)) {
            denyInsertTable(table.toString());
        }
    }

    @Override
    public void checkCanDeleteFromTable(SystemSecurityContext context, CatalogSchemaTableName table)
    {
        if (!checkTablePermission(context, table, DELETE)) {
            denyDeleteTable(table.toString());
        }
    }

    @Override
    public void checkCanUpdateTableColumns(SystemSecurityContext context, CatalogSchemaTableName table, Set<String> updatedColumnNames)
    {
        if (!checkTablePermission(context, table, UPDATE)) {
            denyUpdateTableColumns(table.toString(), updatedColumnNames);
        }
    }

    @Override
    public void checkCanCreateView(SystemSecurityContext context, CatalogSchemaTableName view)
    {
        // check if user will be an owner of the view after creation
        if (!checkTablePermission(context, view, OWNERSHIP)) {
            denyCreateView(view.toString());
        }
    }

    @Override
    public void checkCanRenameView(SystemSecurityContext context, CatalogSchemaTableName view, CatalogSchemaTableName newView)
    {
        // check if user owns the existing view, and if they will be an owner of the view after the rename
        if (!checkTablePermission(context, view, OWNERSHIP) || !checkTablePermission(context, newView, OWNERSHIP)) {
            denyRenameView(view.toString(), newView.toString());
        }
    }

    @Override
    public void checkCanSetViewAuthorization(SystemSecurityContext context, CatalogSchemaTableName view, TrinoPrincipal principal)
    {
        if (!checkTablePermission(context, view, OWNERSHIP)) {
            denySetViewAuthorization(view.toString(), principal);
        }
        if (!checkCanSetAuthorization(context, principal)) {
            denySetViewAuthorization(view.toString(), principal);
        }
    }

    @Override
    public void checkCanDropView(SystemSecurityContext context, CatalogSchemaTableName view)
    {
        if (!checkTablePermission(context, view, OWNERSHIP)) {
            denyDropView(view.toString());
        }
    }

    @Override
    public void checkCanCreateViewWithSelectFromColumns(SystemSecurityContext context, CatalogSchemaTableName table, Set<String> columns)
    {
        if (!canAccessCatalog(context, table.getCatalogName(), ALL)) {
            denySelectTable(table.toString());
        }

        if (INFORMATION_SCHEMA_NAME.equals(table.getSchemaTableName().getSchemaName())) {
            return;
        }

        Identity identity = context.getIdentity();
        CatalogTableAccessControlRule rule = tableRules.stream()
                .filter(tableRule -> tableRule.matches(identity.getUser(), identity.getEnabledRoles(), identity.getGroups(), table))
                .findFirst()
                .orElse(null);
        if (rule == null || !rule.canSelectColumns(columns)) {
            denySelectTable(table.toString());
        }
        if (!rule.getPrivileges().contains(GRANT_SELECT)) {
            denyCreateViewWithSelect(table.toString(), context.getIdentity());
        }
    }

    @Override
    public void checkCanCreateMaterializedView(SystemSecurityContext context, CatalogSchemaTableName materializedView, Map<String, Object> properties)
    {
        // check if user will be an owner of the materialize view after creation
        if (!checkTablePermission(context, materializedView, OWNERSHIP)) {
            denyCreateMaterializedView(materializedView.toString());
        }
    }

    @Override
    public void checkCanRefreshMaterializedView(SystemSecurityContext context, CatalogSchemaTableName materializedView)
    {
        if (!checkTablePermission(context, materializedView, UPDATE)) {
            denyRefreshMaterializedView(materializedView.toString());
        }
    }

    @Override
    public void checkCanDropMaterializedView(SystemSecurityContext context, CatalogSchemaTableName materializedView)
    {
        if (!checkTablePermission(context, materializedView, OWNERSHIP)) {
            denyDropMaterializedView(materializedView.toString());
        }
    }

    @Override
    public void checkCanRenameMaterializedView(SystemSecurityContext context, CatalogSchemaTableName view, CatalogSchemaTableName newView)
    {
        // check if user owns the existing materialized view, and if they will be an owner of the materialized view after the rename
        if (!checkTablePermission(context, view, OWNERSHIP) || !checkTablePermission(context, newView, OWNERSHIP)) {
            denyRenameMaterializedView(view.toString(), newView.toString());
        }
    }

    @Override
    public void checkCanSetMaterializedViewProperties(SystemSecurityContext context, CatalogSchemaTableName materializedView, Map<String, Optional<Object>> properties)
    {
        if (!checkTablePermission(context, materializedView, OWNERSHIP)) {
            denySetMaterializedViewProperties(materializedView.toString());
        }
    }

    @Override
    public void checkCanSetCatalogSessionProperty(SystemSecurityContext context, String catalogName, String propertyName)
    {
        Identity identity = context.getIdentity();
        boolean allowed = canAccessCatalog(context, catalogName, READ_ONLY) && catalogSessionPropertyRules.stream()
                .map(rule -> rule.match(identity.getUser(), identity.getEnabledRoles(), identity.getGroups(), catalogName, propertyName))
                .flatMap(Optional::stream)
                .findFirst()
                .orElse(false);
        if (!allowed) {
            denySetCatalogSessionProperty(propertyName);
        }
    }

    @Override
    public void checkCanGrantSchemaPrivilege(SystemSecurityContext context, Privilege privilege, CatalogSchemaName schema, TrinoPrincipal grantee, boolean grantOption)
    {
        if (!canAccessCatalog(context, schema.getCatalogName(), ALL)) {
            denyGrantSchemaPrivilege(privilege.name(), schema.toString());
        }
        if (!isSchemaOwner(context, schema)) {
            denyGrantSchemaPrivilege(privilege.name(), schema.toString());
        }
    }

    @Override
    public void checkCanDenySchemaPrivilege(SystemSecurityContext context, Privilege privilege, CatalogSchemaName schema, TrinoPrincipal grantee)
    {
        if (!canAccessCatalog(context, schema.getCatalogName(), ALL)) {
            denyDenySchemaPrivilege(privilege.name(), schema.toString());
        }
        if (!isSchemaOwner(context, schema)) {
            denyDenySchemaPrivilege(privilege.name(), schema.toString());
        }
    }

    @Override
    public void checkCanRevokeSchemaPrivilege(SystemSecurityContext context, Privilege privilege, CatalogSchemaName schema, TrinoPrincipal revokee, boolean grantOption)
    {
        if (!canAccessCatalog(context, schema.getCatalogName(), ALL)) {
            denyRevokeSchemaPrivilege(privilege.name(), schema.toString());
        }
        if (!isSchemaOwner(context, schema)) {
            denyRevokeSchemaPrivilege(privilege.name(), schema.toString());
        }
    }

    @Override
    public void checkCanGrantTablePrivilege(SystemSecurityContext context, Privilege privilege, CatalogSchemaTableName table, TrinoPrincipal grantee, boolean grantOption)
    {
        if (!checkTablePermission(context, table, OWNERSHIP)) {
            denyGrantTablePrivilege(privilege.name(), table.toString());
        }
    }

    @Override
    public void checkCanDenyTablePrivilege(SystemSecurityContext context, Privilege privilege, CatalogSchemaTableName table, TrinoPrincipal grantee)
    {
        if (!checkTablePermission(context, table, OWNERSHIP)) {
            denyDenyTablePrivilege(privilege.name(), table.toString());
        }
    }

    @Override
    public void checkCanRevokeTablePrivilege(SystemSecurityContext context, Privilege privilege, CatalogSchemaTableName table, TrinoPrincipal revokee, boolean grantOption)
    {
        if (!checkTablePermission(context, table, OWNERSHIP)) {
            denyRevokeTablePrivilege(privilege.name(), table.toString());
        }
    }

    @Override
    public void checkCanGrantEntityPrivilege(SystemSecurityContext context, EntityPrivilege privilege, EntityKindAndName entity, TrinoPrincipal grantee, boolean grantOption)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void checkCanDenyEntityPrivilege(SystemSecurityContext context, EntityPrivilege privilege, EntityKindAndName entity, TrinoPrincipal grantee)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void checkCanRevokeEntityPrivilege(SystemSecurityContext context, EntityPrivilege privilege, EntityKindAndName entity, TrinoPrincipal revokee, boolean grantOption)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void checkCanCreateRole(SystemSecurityContext context, String role, Optional<TrinoPrincipal> grantor)
    {
        // file based
        denyCreateRole(role);
    }

    @Override
    public void checkCanDropRole(SystemSecurityContext context, String role)
    {
        denyDropRole(role);
    }

    @Override
    public void checkCanGrantRoles(SystemSecurityContext context,
                                   Set<String> roles,
                                   Set<TrinoPrincipal> grantees,
                                   boolean adminOption,
                                   Optional<TrinoPrincipal> grantor)
    {
        denyGrantRoles(roles, grantees);
    }

    @Override
    public void checkCanRevokeRoles(SystemSecurityContext context,
                                    Set<String> roles,
                                    Set<TrinoPrincipal> grantees,
                                    boolean adminOption,
                                    Optional<TrinoPrincipal> grantor)
    {
        denyRevokeRoles(roles, grantees);
    }

    @Override
    public void checkCanShowCurrentRoles(SystemSecurityContext context)
    {
        // users can see their currently enabled roles
    }

    @Override
    public void checkCanShowRoleGrants(SystemSecurityContext context)
    {
        // users can see their role grants
    }

    @Override
    public void checkCanShowRoles(SystemSecurityContext context)
    {
        // allow, no roles are supported so show will always be empty
    }

    @Override
    public void checkCanExecuteProcedure(SystemSecurityContext systemSecurityContext, CatalogSchemaRoutineName procedure)
    {
        Identity identity = systemSecurityContext.getIdentity();
        boolean allowed = canAccessCatalog(systemSecurityContext, procedure.getCatalogName(), READ_ONLY) &&
                procedureRules.stream()
                        .filter(rule -> rule.matches(identity.getUser(), identity.getEnabledRoles(), identity.getGroups(), procedure))
                        .findFirst()
                        .filter(CatalogProcedureAccessControlRule::canExecuteProcedure)
                        .isPresent();
        if (!allowed) {
            denyExecuteProcedure(procedure.toString());
        }
    }

    @Override
    public boolean canExecuteFunction(SystemSecurityContext systemSecurityContext, CatalogSchemaRoutineName functionName)
    {
        return checkFunctionPermission(systemSecurityContext, functionName, CatalogFunctionAccessControlRule::canExecuteFunction);
    }

    @Override
    public boolean canCreateViewWithExecuteFunction(SystemSecurityContext systemSecurityContext, CatalogSchemaRoutineName functionName)
    {
        return checkFunctionPermission(systemSecurityContext, functionName, CatalogFunctionAccessControlRule::canGrantExecuteFunction);
    }

    @Override
    public void checkCanExecuteTableProcedure(SystemSecurityContext systemSecurityContext, CatalogSchemaTableName table, String procedure)
    {
    }

    @Override
    public void checkCanShowFunctions(SystemSecurityContext context, CatalogSchemaName schema)
    {
        if (!checkAnySchemaAccess(context, schema.getCatalogName(), schema.getSchemaName())) {
            denyShowFunctions(schema.toString());
        }
    }

    @Override
    public Set<SchemaFunctionName> filterFunctions(SystemSecurityContext context, String catalogName, Set<SchemaFunctionName> functionNames)
    {
        return functionNames.stream()
                .filter(functionName -> {
                    CatalogSchemaRoutineName routineName = new CatalogSchemaRoutineName(catalogName, functionName.getSchemaName(), functionName.getFunctionName());
                    return isSchemaOwner(context, new CatalogSchemaName(catalogName, functionName.getSchemaName())) ||
                            checkAnyFunctionPermission(context, routineName, CatalogFunctionAccessControlRule::canExecuteFunction);
                })
                .collect(toImmutableSet());
    }

    @Override
    public void checkCanCreateFunction(SystemSecurityContext systemSecurityContext, CatalogSchemaRoutineName functionName)
    {
        if (!checkFunctionPermission(systemSecurityContext, functionName, CatalogFunctionAccessControlRule::hasOwnership)) {
            denyCreateFunction(functionName.toString());
        }
    }

    @Override
    public void checkCanDropFunction(SystemSecurityContext systemSecurityContext, CatalogSchemaRoutineName functionName)
    {
        if (!checkFunctionPermission(systemSecurityContext, functionName, CatalogFunctionAccessControlRule::hasOwnership)) {
            denyDropFunction(functionName.toString());
        }
    }

    @Override
    public Iterable<EventListener> getEventListeners()
    {
        return ImmutableSet.of();
    }

    @Override
    public List<ViewExpression> getRowFilters(SystemSecurityContext context, CatalogSchemaTableName table)
    {
        SchemaTableName tableName = table.getSchemaTableName();
        if (INFORMATION_SCHEMA_NAME.equals(tableName.getSchemaName())) {
            return ImmutableList.of();
        }

        Identity identity = context.getIdentity();
        return tableRules.stream()
                .filter(rule -> rule.matches(identity.getUser(), identity.getEnabledRoles(), identity.getGroups(), table))
                .map(rule -> rule.getFilter(table.getCatalogName(), tableName.getSchemaName()))
                // we return the first one we find
                .findFirst()
                .stream()
                .flatMap(Optional::stream)
                .collect(toImmutableList());
    }

    @Override
    public Optional<ViewExpression> getColumnMask(SystemSecurityContext context, CatalogSchemaTableName table, String columnName, Type type)
    {
        SchemaTableName tableName = table.getSchemaTableName();
        if (INFORMATION_SCHEMA_NAME.equals(tableName.getSchemaName())) {
            return Optional.empty();
        }

        Identity identity = context.getIdentity();
        List<ViewExpression> masks = tableRules.stream()
                .filter(rule -> rule.matches(identity.getUser(), identity.getEnabledRoles(), identity.getGroups(), table))
                .map(rule -> rule.getColumnMask(table.getCatalogName(), table.getSchemaTableName().getSchemaName(), columnName))
                // we return the first one we find
                .findFirst()
                .stream()
                .flatMap(Optional::stream)
                .toList();

        if (masks.size() > 1) {
            throw new TrinoException(INVALID_COLUMN_MASK, format("Multiple masks defined for %s.%s", table, columnName));
        }

        return masks.stream().findFirst();
    }

    private boolean checkAnyCatalogAccess(SystemSecurityContext context, String catalogName)
    {
        if (canAccessCatalog(context, catalogName, OWNER)) {
            return true;
        }

        Identity identity = context.getIdentity();
        return canAccessCatalog(context, catalogName, READ_ONLY) &&
                anyCatalogPermissionsRules.stream().anyMatch(rule -> rule.match(identity.getUser(), identity.getEnabledRoles(), identity.getGroups(), catalogName));
    }

    private boolean canAccessCatalog(SystemSecurityContext context, String catalogName, CatalogAccessControlRule.AccessMode requiredAccess)
    {
        Identity identity = context.getIdentity();
        for (CatalogAccessControlRule rule : catalogRules) {
            Optional<CatalogAccessControlRule.AccessMode> accessMode = rule.match(identity.getUser(), identity.getEnabledRoles(), identity.getGroups(), catalogName);
            if (accessMode.isPresent()) {
                return accessMode.get().implies(requiredAccess);
            }
        }
        return false;
    }

    private boolean checkAnySchemaAccess(SystemSecurityContext context, String catalogName, String schemaName)
    {
        Identity identity = context.getIdentity();
        return canAccessCatalog(context, catalogName, READ_ONLY) &&
                anyCatalogSchemaPermissionsRules.stream().anyMatch(rule -> rule.match(identity.getUser(), identity.getEnabledRoles(), identity.getGroups(), catalogName, schemaName));
    }

    private boolean isSchemaOwner(SystemSecurityContext context, CatalogSchemaName schema)
    {
        if (!canAccessCatalog(context, schema.getCatalogName(), ALL)) {
            return false;
        }

        Identity identity = context.getIdentity();
        for (CatalogSchemaAccessControlRule rule : schemaRules) {
            Optional<Boolean> owner = rule.match(identity.getUser(), identity.getEnabledRoles(), identity.getGroups(), schema);
            if (owner.isPresent()) {
                return owner.get();
            }
        }
        return false;
    }

    private boolean checkAnyTablePermission(SystemSecurityContext context, CatalogSchemaTableName table)
    {
        return checkTablePermission(context, table, READ_ONLY, privileges -> !privileges.isEmpty());
    }

    private boolean checkTablePermission(SystemSecurityContext context, CatalogSchemaTableName table, TableAccessControlRule.TablePrivilege requiredPrivilege)
    {
        CatalogAccessControlRule.AccessMode requiredCatalogAccess = requiredPrivilege == SELECT || requiredPrivilege == GRANT_SELECT ? READ_ONLY : ALL;
        return checkTablePermission(context, table, requiredCatalogAccess, privileges -> privileges.contains(requiredPrivilege));
    }

    private boolean checkTablePermission(
            SystemSecurityContext context,
            CatalogSchemaTableName table,
            CatalogAccessControlRule.AccessMode requiredCatalogAccess,
            Predicate<Set<TableAccessControlRule.TablePrivilege>> checkPrivileges)
    {
        if (!canAccessCatalog(context, table.getCatalogName(), requiredCatalogAccess)) {
            return false;
        }

        if (INFORMATION_SCHEMA_NAME.equals(table.getSchemaTableName().getSchemaName())) {
            return true;
        }

        Identity identity = context.getIdentity();
        for (CatalogTableAccessControlRule rule : tableRules) {
            if (rule.matches(identity.getUser(), identity.getEnabledRoles(), identity.getGroups(), table)) {
                return checkPrivileges.test(rule.getPrivileges());
            }
        }
        return false;
    }

    private boolean checkFunctionPermission(SystemSecurityContext context, CatalogSchemaRoutineName functionName, Predicate<CatalogFunctionAccessControlRule> executePredicate)
    {
        Identity identity = context.getIdentity();
        return canAccessCatalog(context, functionName.getCatalogName(), READ_ONLY) &&
                functionRules.stream()
                        .filter(rule -> rule.matches(identity.getUser(), identity.getEnabledRoles(), identity.getGroups(), functionName))
                        .findFirst()
                        .filter(executePredicate)
                        .isPresent();
    }

    private boolean checkAnyFunctionPermission(SystemSecurityContext context, CatalogSchemaRoutineName functionName, Predicate<CatalogFunctionAccessControlRule> executePredicate)
    {
        Identity identity = context.getIdentity();
        return canAccessCatalog(context, functionName.getCatalogName(), READ_ONLY) &&
                functionRules.stream()
                        .filter(rule -> rule.matches(identity.getUser(), identity.getEnabledRoles(), identity.getGroups(), functionName))
                        .findFirst()
                        .filter(executePredicate)
                        .isPresent();
    }

    private boolean checkCanSetAuthorization(SystemSecurityContext context, TrinoPrincipal principal)
    {
        Identity identity = context.getIdentity();
        return authorizationRules.stream()
                .flatMap(rule -> rule.match(identity.getUser(), identity.getGroups(), identity.getEnabledRoles(), principal).stream())
                .findFirst()
                .orElse(false);
    }

    @Override
    public final void shutdown()
    {
        lifeCycleManager.stop();
    }

    public static ExternalAuthzSystemAccessControl.Builder builder()
    {
        return new ExternalAuthzSystemAccessControl.Builder();
    }

    public static final class Builder
    {
        private LifeCycleManager lifeCycleManager;
        private List<CatalogAccessControlRule> catalogRules = ImmutableList.of(CatalogAccessControlRule.ALLOW_ALL);
        private Optional<List<QueryAccessRule>> queryAccessRules = Optional.empty();
        private Optional<List<ImpersonationRule>> impersonationRules = Optional.empty();
        private Optional<List<PrincipalUserMatchRule>> principalUserMatchRules = Optional.empty();
        private Optional<List<SystemInformationRule>> systemInformationRules = Optional.empty();
        private List<AuthorizationRule> authorizationRules = ImmutableList.of();
        private List<CatalogSchemaAccessControlRule> schemaRules = ImmutableList.of(CatalogSchemaAccessControlRule.ALLOW_ALL);
        private List<CatalogTableAccessControlRule> tableRules = ImmutableList.of(CatalogTableAccessControlRule.ALLOW_ALL);
        private List<SessionPropertyAccessControlRule> sessionPropertyRules = ImmutableList.of(SessionPropertyAccessControlRule.ALLOW_ALL);
        private List<CatalogSessionPropertyAccessControlRule> catalogSessionPropertyRules = ImmutableList.of(CatalogSessionPropertyAccessControlRule.ALLOW_ALL);
        private List<CatalogFunctionAccessControlRule> functionRules = ImmutableList.of(CatalogFunctionAccessControlRule.ALLOW_BUILTIN);
        private List<CatalogProcedureAccessControlRule> procedureRules = ImmutableList.of(CatalogProcedureAccessControlRule.ALLOW_BUILTIN);

        public ExternalAuthzSystemAccessControl.Builder setLifeCycleManager(LifeCycleManager lifeCycleManager)
        {
            this.lifeCycleManager = lifeCycleManager;
            return this;
        }

        @SuppressWarnings("unused")
        public ExternalAuthzSystemAccessControl.Builder denyAllAccess()
        {
            catalogRules = ImmutableList.of();
            queryAccessRules = Optional.of(ImmutableList.of());
            impersonationRules = Optional.of(ImmutableList.of());
            principalUserMatchRules = Optional.of(ImmutableList.of());
            systemInformationRules = Optional.of(ImmutableList.of());
            authorizationRules = ImmutableList.of();
            schemaRules = ImmutableList.of();
            tableRules = ImmutableList.of();
            sessionPropertyRules = ImmutableList.of();
            catalogSessionPropertyRules = ImmutableList.of();
            functionRules = ImmutableList.of();
            procedureRules = ImmutableList.of();
            return this;
        }

        public ExternalAuthzSystemAccessControl.Builder setCatalogRules(List<CatalogAccessControlRule> catalogRules)
        {
            this.catalogRules = catalogRules;
            return this;
        }

        public ExternalAuthzSystemAccessControl.Builder setQueryAccessRules(Optional<List<QueryAccessRule>> queryAccessRules)
        {
            this.queryAccessRules = queryAccessRules;
            return this;
        }

        public ExternalAuthzSystemAccessControl.Builder setImpersonationRules(Optional<List<ImpersonationRule>> impersonationRules)
        {
            this.impersonationRules = impersonationRules;
            return this;
        }

        public ExternalAuthzSystemAccessControl.Builder setPrincipalUserMatchRules(Optional<List<PrincipalUserMatchRule>> principalUserMatchRules)
        {
            this.principalUserMatchRules = principalUserMatchRules;
            return this;
        }

        public ExternalAuthzSystemAccessControl.Builder setSystemInformationRules(Optional<List<SystemInformationRule>> systemInformationRules)
        {
            this.systemInformationRules = systemInformationRules;
            return this;
        }

        public ExternalAuthzSystemAccessControl.Builder setAuthorizationRules(List<AuthorizationRule> authorizationRules)
        {
            this.authorizationRules = authorizationRules;
            return this;
        }

        public ExternalAuthzSystemAccessControl.Builder setSchemaRules(List<CatalogSchemaAccessControlRule> schemaRules)
        {
            this.schemaRules = schemaRules;
            return this;
        }

        public ExternalAuthzSystemAccessControl.Builder setTableRules(List<CatalogTableAccessControlRule> tableRules)
        {
            this.tableRules = tableRules;
            return this;
        }

        public ExternalAuthzSystemAccessControl.Builder setSessionPropertyRules(List<SessionPropertyAccessControlRule> sessionPropertyRules)
        {
            this.sessionPropertyRules = sessionPropertyRules;
            return this;
        }

        public ExternalAuthzSystemAccessControl.Builder setCatalogSessionPropertyRules(List<CatalogSessionPropertyAccessControlRule> catalogSessionPropertyRules)
        {
            this.catalogSessionPropertyRules = catalogSessionPropertyRules;
            return this;
        }

        public ExternalAuthzSystemAccessControl.Builder setFunctionRules(List<CatalogFunctionAccessControlRule> functionRules)
        {
            this.functionRules = functionRules;
            return this;
        }

        public ExternalAuthzSystemAccessControl.Builder setProcedureRules(List<CatalogProcedureAccessControlRule> procedureRules)
        {
            this.procedureRules = procedureRules;
            return this;
        }

        public ExternalAuthzSystemAccessControl build()
        {
            return new ExternalAuthzSystemAccessControl(
                    lifeCycleManager,
                    catalogRules,
                    queryAccessRules,
                    impersonationRules,
                    principalUserMatchRules,
                    systemInformationRules,
                    authorizationRules,
                    schemaRules,
                    tableRules,
                    sessionPropertyRules,
                    catalogSessionPropertyRules,
                    functionRules,
                    procedureRules);
        }
    }
}


