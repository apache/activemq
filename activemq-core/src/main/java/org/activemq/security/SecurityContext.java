package org.activemq.security;

import java.util.HashSet;
import java.util.Set;

import edu.emory.mathcs.backport.java.util.concurrent.ConcurrentHashMap;

/**
 * Used to cache up authorizations so that subsequent requests are faster.
 * 
 * @version $Revision$
 */
abstract public class SecurityContext {

    final String userName;
    
    final ConcurrentHashMap authorizedReadDests = new ConcurrentHashMap();
    final ConcurrentHashMap authorizedWriteDests = new ConcurrentHashMap();

    public SecurityContext(String userName) {
        this.userName = userName;
    }

    public boolean isInOneOf(Set allowedPrincipals) {
        HashSet set = new HashSet(getPrincipals());
        set.retainAll(allowedPrincipals);
        return set.size()>0;
    }

    abstract public Set getPrincipals();
    
    public String getUserName() {
        return userName;
    }

    public ConcurrentHashMap getAuthorizedReadDests() {
        return authorizedReadDests;
    }
    public ConcurrentHashMap getAuthorizedWriteDests() {
        return authorizedWriteDests;
    }
    
}