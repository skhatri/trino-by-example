package io.trino.plugin.base.security;

import io.trino.spi.security.BasicPrincipal;
import io.trino.spi.security.PasswordAuthenticator;
import io.trino.spi.security.PasswordAuthenticatorFactory;

import java.security.Principal;
import java.util.Map;

public class PlainPasswordAuthenticatorFactory implements PasswordAuthenticatorFactory {
    @Override
    public String getName() {
        return "plain-auth";
    }

    @Override
    public PasswordAuthenticator create(Map<String, String> config) {
        return new PlainPasswordAuthenticator();
    }

}

class PlainPasswordAuthenticator implements PasswordAuthenticator {
    @Override
    public Principal createAuthenticatedPrincipal(String user, String password) {
        System.out.println(user + " " + password);
        return new BasicPrincipal(user);
    }
}
