package io.trino.plugin.base.security;

import com.google.common.collect.ImmutableSet;
import io.trino.spi.Plugin;
import io.trino.spi.eventlistener.EventListenerFactory;
import io.trino.spi.security.PasswordAuthenticatorFactory;
import io.trino.spi.security.SystemAccessControlFactory;

import java.util.Set;

public class ExternalAuthsPlugin implements Plugin {

    @Override
    public Iterable<SystemAccessControlFactory> getSystemAccessControlFactories() {
        return ImmutableSet.<SystemAccessControlFactory>builder()
            .add(new ExternalAuthzSystemAccessControl.Factory())
            .build();
    }

    @Override
    public Set<Class<?>> getFunctions() {
        return ImmutableSet.<Class<?>>builder()
            .add(StringFunctions.class)
            .build();
    }

    @Override
    public Iterable<EventListenerFactory> getEventListenerFactories() {
        return ImmutableSet.<EventListenerFactory>builder()
            .add(new AuditLoggingEventListenerFactory())
            .build();
    }

    @Override
    public Iterable<PasswordAuthenticatorFactory> getPasswordAuthenticatorFactories() {
        return ImmutableSet.<PasswordAuthenticatorFactory>builder()
            .add(new PlainPasswordAuthenticatorFactory())
            .build();
    }
}
