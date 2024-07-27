package org.apache.activemq.security;

import java.security.Principal;
import java.util.Set;

public class OIDCSecurityContext extends SecurityContext {
    private final Set<Principal> principals;

    public OIDCSecurityContext(String userName, Set<Principal> principals) {
        super(userName);
        this.principals = principals;
    }

    @Override
    public Set<Principal> getPrincipals() {
        return principals;
    }
}