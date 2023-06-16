package com.github.skhatri.hive;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.ql.metadata.AuthorizationException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.Partition;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.security.HiveAuthenticationProvider;
import org.apache.hadoop.hive.ql.security.authorization.HiveAuthorizationProvider;
import org.apache.hadoop.hive.ql.security.authorization.Privilege;

import java.util.List;

public class HiveExtAuthorizer implements HiveAuthorizationProvider {
    @Override
    public void init(Configuration conf) throws HiveException {

    }

    @Override
    public HiveAuthenticationProvider getAuthenticator() {
        return null;
    }

    @Override
    public void setAuthenticator(HiveAuthenticationProvider authenticator) {

    }

    @Override
    public void authorize(Privilege[] readRequiredPriv, Privilege[] writeRequiredPriv) throws HiveException, AuthorizationException {

    }

    @Override
    public void authorize(Database db, Privilege[] readRequiredPriv, Privilege[] writeRequiredPriv) throws HiveException, AuthorizationException {

    }

    @Override
    public void authorize(Table table, Privilege[] readRequiredPriv, Privilege[] writeRequiredPriv) throws HiveException, AuthorizationException {

    }

    @Override
    public void authorize(Partition part, Privilege[] readRequiredPriv, Privilege[] writeRequiredPriv) throws HiveException, AuthorizationException {

    }

    @Override
    public void authorize(Table table, Partition part, List<String> columns, Privilege[] readRequiredPriv, Privilege[] writeRequiredPriv) throws HiveException, AuthorizationException {

    }

    @Override
    public void setConf(Configuration configuration) {

    }

    @Override
    public Configuration getConf() {
        return null;
    }
}
