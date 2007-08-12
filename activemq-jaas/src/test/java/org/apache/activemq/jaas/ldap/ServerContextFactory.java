/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.activemq.jaas.ldap;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.Properties;
import javax.naming.Context;
import javax.naming.NamingException;
import javax.naming.directory.DirContext;
import javax.naming.ldap.Control;
import javax.naming.ldap.InitialLdapContext;
import javax.naming.ldap.LdapContext;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.kerberos.protocol.KerberosProtocolProvider;
import org.apache.kerberos.sam.SamSubsystem;
import org.apache.kerberos.service.KdcConfiguration;
import org.apache.kerberos.store.JndiPrincipalStoreImpl;
import org.apache.kerberos.store.PrincipalStore;
import org.apache.ldap.common.exception.LdapConfigurationException;
import org.apache.ldap.common.name.LdapName;
import org.apache.ldap.common.util.NamespaceTools;
import org.apache.ldap.common.util.PropertiesUtils;
import org.apache.ldap.server.jndi.ContextFactoryService;
import org.apache.ldap.server.jndi.CoreContextFactory;
import org.apache.ldap.server.protocol.LdapProtocolProvider;
import org.apache.mina.common.TransportType;
import org.apache.mina.registry.Service;
import org.apache.mina.registry.ServiceRegistry;

/**
 * Adds additional bootstrapping for server socket listeners when firing up the
 * server.
 * 
 * @version $Rev: 233391 $ $Date: 2005-08-18 16:38:47 -0600 (Thu, 18 Aug 2005) $
 * @see javax.naming.spi.InitialContextFactory
 */
public class ServerContextFactory extends CoreContextFactory {
    
    private static final Log LOG = LogFactory.getLog(ServerContextFactory.class);
    
    private static Service ldapService;
    private static Service kerberosService;
    private static ServiceRegistry minaRegistry;

    protected ServiceRegistry getMinaRegistry() {
        return minaRegistry;
    }

    public void afterShutdown(ContextFactoryService service) {
        if (minaRegistry != null) {
            if (ldapService != null) {
                minaRegistry.unbind(ldapService);
                if (LOG.isInfoEnabled()) {
                    LOG.info("Unbind of LDAP Service complete: " + ldapService);
                }
                ldapService = null;
            }

            if (kerberosService != null) {
                minaRegistry.unbind(kerberosService);
                if (LOG.isInfoEnabled()) {
                    LOG.info("Unbind of KRB5 Service complete: " + kerberosService);
                }
                kerberosService = null;
            }
        }
    }

    public void afterStartup(ContextFactoryService service) throws NamingException {
        ServerStartupConfiguration cfg = (ServerStartupConfiguration)service.getConfiguration().getStartupConfiguration();
        Hashtable env = service.getConfiguration().getEnvironment();

        if (cfg.isEnableNetworking()) {
            setupRegistry(cfg);
            startLdapProtocol(cfg, env);

            if (cfg.isEnableKerberos()) {
                startKerberosProtocol(env);
            }
        }
    }

    /**
     * Starts up the MINA registry so various protocol providers can be started.
     */
    private void setupRegistry(ServerStartupConfiguration cfg) {
        minaRegistry = cfg.getMinaServiceRegistry();
    }

    /**
     * Starts the Kerberos protocol provider which is backed by the LDAP store.
     * 
     * @throws NamingException if there are problems starting up the Kerberos
     *                 provider
     */
    private void startKerberosProtocol(Hashtable env) throws NamingException {
        /*
         * Looks like KdcConfiguration takes properties and we use Hashtable for
         * JNDI so I'm copying over the String based properties into a new
         * Properties obj.
         */
        Properties props = new Properties();
        Iterator list = env.keySet().iterator();
        while (list.hasNext()) {
            String key = (String)list.next();

            if (env.get(key) instanceof String) {
                props.setProperty(key, (String)env.get(key));
            }
        }

        // construct the configuration, get the port, create the service, and
        // prepare kdc objects
        KdcConfiguration config = new KdcConfiguration(props);
        int port = PropertiesUtils.get(env, KdcConfiguration.KERBEROS_PORT_KEY, KdcConfiguration.DEFAULT_KERBEROS_PORT);
        Service service = new Service("kerberos", TransportType.DATAGRAM, new InetSocketAddress(port));
        LdapContext ctx = getBaseRealmContext(config, env);
        PrincipalStore store = new JndiPrincipalStoreImpl(ctx, new LdapName("ou=Users"));
        SamSubsystem.getInstance().setUserContext((DirContext)ctx, "ou=Users");

        try {
            minaRegistry.bind(service, new KerberosProtocolProvider(config, store));
            kerberosService = service;
            if (LOG.isInfoEnabled()) {
                LOG.info("Successful bind of KRB5 Service completed: " + kerberosService);
            }
        } catch (IOException e) {
            LOG.error("Could not start the kerberos service on port " + KdcConfiguration.DEFAULT_KERBEROS_PORT, e);
        }
    }

    /**
     * Maps a Kerberos Realm name to a position within the DIT. The primary
     * realm of the KDC will use this area for configuration and for storing
     * user entries.
     * 
     * @param config the KDC's configuration
     * @param env the JNDI environment properties
     * @return the base context for the primary realm of the KDC
     * @throws NamingException
     */
    @SuppressWarnings("unchecked")
    private LdapContext getBaseRealmContext(KdcConfiguration config, Hashtable env) throws NamingException {
        Hashtable cloned = (Hashtable)env.clone();
        String dn = NamespaceTools.inferLdapName(config.getPrimaryRealm());
        cloned.put(Context.PROVIDER_URL, dn);

        if (LOG.isInfoEnabled()) {
            LOG.info("Getting initial context for realm base at " + dn + " for " + config.getPrimaryRealm());
        }

        return new InitialLdapContext(cloned, new Control[] {});
    }

    /**
     * Starts up the LDAP protocol provider to service LDAP requests
     * 
     * @throws NamingException if there are problems starting the LDAP provider
     */
    private void startLdapProtocol(ServerStartupConfiguration cfg, Hashtable env) throws NamingException {
        int port = cfg.getLdapPort();
        InetAddress host = cfg.getHost();
        Service service = new Service("ldap", TransportType.SOCKET, new InetSocketAddress(host, port));

        try {
            minaRegistry.bind(service, new LdapProtocolProvider((Hashtable)env.clone()));
            ldapService = service;
            if (LOG.isInfoEnabled()) {
                LOG.info("Successful bind of LDAP Service completed: " + ldapService);
            }
        } catch (IOException e) {
            String msg = "Failed to bind the LDAP protocol service to the service registry: " + service;
            LdapConfigurationException lce = new LdapConfigurationException(msg);
            lce.setRootCause(e);
            LOG.error(msg, e);
            throw lce;
        }
    }
}
