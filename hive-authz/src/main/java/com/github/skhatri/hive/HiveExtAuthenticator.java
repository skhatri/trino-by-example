package com.github.skhatri.hive;

import org.apache.hive.service.auth.PasswdAuthenticationProvider;

import javax.security.sasl.AuthenticationException;

//hive.server2.custom.authentication.class
public class HiveExtAuthenticator implements PasswdAuthenticationProvider {
    @Override
    public void Authenticate(String user, String password) throws AuthenticationException {
        if (!"password".equals(password)) {
            throw new AuthenticationException("invalid password");
        }
    }
}
