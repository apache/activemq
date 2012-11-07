package org.apache.activemq.security;

import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.InitializingBean;

/**
 * A {@link DefaultAuthorizationMap} implementation which uses LDAP to initialize and update authorization
 * policy.
 *
 * @org.apache.xbean.XBean
 *
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
public class CachedLDAPAuthorizationMap extends SimpleCachedLDAPAuthorizationMap implements InitializingBean, DisposableBean {

    @Override
    public void afterPropertiesSet() throws Exception {
        super.afterPropertiesSet();
    }

    @Override
    public void destroy() throws Exception {
        super.destroy();
    }

}
