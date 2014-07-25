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
package org.apache.activemq.shiro;

import org.apache.activemq.broker.BrokerPlugin;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.MutableBrokerFilter;
import org.apache.activemq.shiro.authc.AuthenticationFilter;
import org.apache.activemq.shiro.authc.DefaultAuthenticationPolicy;
import org.apache.activemq.shiro.authz.AuthorizationFilter;
import org.apache.activemq.shiro.env.IniEnvironment;
import org.apache.activemq.shiro.subject.DefaultConnectionSubjectFactory;
import org.apache.activemq.shiro.subject.SubjectFilter;
import org.apache.activemq.test.JmsResourceProvider;
import org.apache.activemq.test.TestSupport;
import org.apache.shiro.authc.AuthenticationException;
import org.apache.shiro.config.Ini;
import org.apache.shiro.env.DefaultEnvironment;
import org.apache.shiro.env.Environment;
import org.apache.shiro.mgt.DefaultSecurityManager;
import org.apache.shiro.realm.text.IniRealm;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;

/**
 * @since 5.10.0
 */
public class ShiroPluginTest extends TestSupport {

    private static final Logger LOG = LoggerFactory.getLogger(ShiroPluginTest.class);

    protected BrokerService broker;
    protected SecureJmsResourceProvider resourceProvider;
    protected ConnectionFactory connectionFactory;
    protected Connection connection;
    protected Session session;
    protected Destination destination;
    protected MessageConsumer consumer;
    protected MessageProducer producer;

    @Override
    protected void setUp() throws Exception {
        resourceProvider = new SecureJmsResourceProvider();
    }

    @Override
    protected void tearDown() throws Exception {
        LOG.info("Shutting down broker...");

        if (session != null) {
            session.close();
        }
        session = null;

        if (connection != null) {
            connection.close();
        }
        connection = null;

        if (broker != null) {
            broker.stop();
            broker.waitUntilStopped();
        }
        broker = null;

        LOG.info("Broker shut down.");
    }

    protected void reconnect() throws Exception {
        reconnect(null, null);
    }

    protected void reconnect(String username, String password) throws Exception {
        if (connection != null) {
            // Close the prev connection.
            connection.close();
        }
        session = null;
        if (username == null && password == null) {
            connection = resourceProvider.createConnection(connectionFactory);
        } else {
            connection = resourceProvider.createConnection(connectionFactory, username, password);
        }
        reconnectSession();
        connection.start();
    }

    protected void reconnectSession() throws JMSException {
        if (session != null) {
            session.close();
        }
        session = resourceProvider.createSession(connection);
        destination = resourceProvider.createDestination(session, getSubject());
        producer = resourceProvider.createProducer(session, destination);
        consumer = resourceProvider.createConsumer(session, destination);
    }

    protected ConnectionFactory newConnectionFactory() throws Exception {
        return resourceProvider.createConnectionFactory();
    }

    protected void start() throws Exception {
        startBroker();
        topic = resourceProvider.isTopic();
        connectionFactory = newConnectionFactory();
    }

    protected void startBroker() throws Exception {
        broker.start();
        broker.waitUntilStarted();
    }

    protected BrokerService createBroker(BrokerPlugin... plugins) throws Exception {
        return createBroker(plugins, resourceProvider.getServerUri());
    }

    protected BrokerService createBroker(BrokerPlugin[] plugins, String... connectorUris) throws Exception {
        BrokerService brokerService = new BrokerService();
        if (plugins != null && plugins.length > 0) {
            brokerService.setPlugins(plugins);
        }
        if (connectorUris != null) {
            for (String uri : connectorUris) {
                brokerService.addConnector(uri);
            }
        }
        return brokerService;
    }

    protected ShiroPlugin createPlugin(String iniPath) {
        Ini ini = Ini.fromResourcePath(iniPath);
        Environment env = new IniEnvironment(ini);
        ShiroPlugin plugin = new ShiroPlugin();
        plugin.setEnvironment(env);
        return plugin;
    }

    public void testNoEnvironmentOrSecurityManager() throws Exception {
        //should build IniEnvironment from shiro.ini in the classpath at the least:
        ShiroPlugin plugin = new ShiroPlugin();
        plugin.installPlugin(new MutableBrokerFilter(null));

        Ini ini = Ini.fromResourcePath("classpath:shiro.ini");
        IniRealm realm = (IniRealm) ((DefaultSecurityManager) plugin.getEnvironment().getSecurityManager()).getRealms().iterator().next();
        assertEquals(ini, realm.getIni());
    }

    public void testSetIni() throws Exception {
        ShiroPlugin plugin = new ShiroPlugin();
        Ini ini = Ini.fromResourcePath("classpath:minimal.shiro.ini");
        plugin.setIni(ini);
        plugin.installPlugin(new MutableBrokerFilter(null));

        IniRealm realm = (IniRealm) ((DefaultSecurityManager) plugin.getEnvironment().getSecurityManager()).getRealms().iterator().next();
        assertSame(ini, realm.getIni());
    }

    public void testSetIniString() throws Exception {
        ShiroPlugin plugin = new ShiroPlugin();
        plugin.setIniConfig(
                "[users]\n" +
                "system = manager, system\n" +
                "[roles]\n" +
                "system = *");
        plugin.installPlugin(new MutableBrokerFilter(null));

        IniRealm realm = (IniRealm) ((DefaultSecurityManager) plugin.getEnvironment().getSecurityManager()).getRealms().iterator().next();
        Ini ini = realm.getIni();
        assertEquals(1, ini.getSection("users").size());
        assertEquals("manager, system", ini.getSection("users").get("system"));
        assertEquals(1, ini.getSection("roles").size());
        assertEquals("*", ini.getSection("roles").get("system"));
    }

    public void testSetIniResourcePath() throws Exception {
        ShiroPlugin plugin = new ShiroPlugin();

        String path = "classpath:minimal.shiro.ini";

        plugin.setIniResourcePath(path);
        plugin.installPlugin(new MutableBrokerFilter(null));

        Ini ini = Ini.fromResourcePath(path);

        IniRealm realm = (IniRealm) ((DefaultSecurityManager) plugin.getEnvironment().getSecurityManager()).getRealms().iterator().next();
        assertEquals(ini, realm.getIni());
    }

    public void testSetSubjectFilter() {
        ShiroPlugin plugin = new ShiroPlugin();
        SubjectFilter filter = new SubjectFilter();
        plugin.setSubjectFilter(filter);
        assertSame(filter, plugin.getSubjectFilter());
        //assert that the AuthenticationFilter is always the next filter in the chain after the SubjectFilter:
        assertSame(plugin.getAuthenticationFilter(), filter.getNext());
    }

    public void testSetAuthenticationFilter() {
        ShiroPlugin plugin = new ShiroPlugin();
        AuthenticationFilter filter = new AuthenticationFilter();
        plugin.setAuthenticationFilter(filter);
        assertSame(filter, plugin.getAuthenticationFilter());
        //assert that the AuthenticationFilter is always the next filter in the chain after the SubjectFilter:
        assertSame(plugin.getSubjectFilter().getNext(), filter);
    }

    public void testSetAuthorizationFilter() {
        ShiroPlugin plugin = new ShiroPlugin();
        AuthorizationFilter filter = new AuthorizationFilter();
        plugin.setAuthorizationFilter(filter);
        assertSame(filter, plugin.getAuthorizationFilter());
        //assert that the AuthenticationFilter is always the next filter in the chain after the AuthenticationFilter:
        assertSame(plugin.getAuthenticationFilter().getNext(), filter);
    }

    public void testSetEnvironment() {
        ShiroPlugin plugin = new ShiroPlugin();
        Environment env = new DefaultEnvironment();
        plugin.setEnvironment(env);
        assertSame(env, plugin.getEnvironment());
    }

    public void testSetSecurityManager() {
        ShiroPlugin plugin = new ShiroPlugin();
        org.apache.shiro.mgt.SecurityManager securityManager = new DefaultSecurityManager();
        plugin.setSecurityManager(securityManager);
        assertSame(securityManager, plugin.getSecurityManager());
    }

    public void testSecurityManagerWhenInstalled() throws Exception {
        ShiroPlugin plugin = new ShiroPlugin();
        org.apache.shiro.mgt.SecurityManager securityManager = new DefaultSecurityManager();
        plugin.setSecurityManager(securityManager);

        assertNull(plugin.getEnvironment()); //we will auto-create one when only a sm is provided

        plugin.installPlugin(new MutableBrokerFilter(null));

        assertSame(securityManager, plugin.getSecurityManager());
        assertNotNull(plugin.getEnvironment());
        assertSame(securityManager, plugin.getEnvironment().getSecurityManager());
    }

    public void testEnabledWhenNotInstalled() {
        ShiroPlugin plugin = new ShiroPlugin();
        assertTrue(plugin.isEnabled()); //enabled by default

        plugin.setEnabled(false);
        assertFalse(plugin.isEnabled());

        plugin.setEnabled(true);
        assertTrue(plugin.isEnabled());
    }

    public void testEnabledWhenInstalled() throws Exception {
        ShiroPlugin plugin = createPlugin("classpath:minimal.shiro.ini");
        this.broker = createBroker(plugin);
        start();
        assertTrue(plugin.isEnabled());

        plugin.setEnabled(false);
        assertFalse(plugin.isEnabled());

        plugin.setEnabled(true);
        assertTrue(plugin.isEnabled());
    }

    public void testAuthenticationEnabledWhenNotInstalled() {
        ShiroPlugin plugin = new ShiroPlugin();
        assertTrue(plugin.isAuthenticationEnabled());

        plugin.setAuthenticationEnabled(false);
        assertFalse(plugin.isAuthenticationEnabled());

        plugin.setAuthenticationEnabled(true);
        assertTrue(plugin.isAuthenticationEnabled());
    }

    public void testAuthenticationEnabledWhenInstalled() throws Exception {
        ShiroPlugin plugin = new ShiroPlugin();
        plugin.setEnvironment(new DefaultEnvironment());
        plugin.installPlugin(new MutableBrokerFilter(null));

        assertTrue(plugin.isAuthenticationEnabled());

        plugin.setAuthenticationEnabled(false);
        assertFalse(plugin.isAuthenticationEnabled());

        plugin.setAuthenticationEnabled(true);
        assertTrue(plugin.isAuthenticationEnabled());
    }

    public void testSetAuthenticationPolicy() {
        ShiroPlugin plugin = new ShiroPlugin();
        DefaultAuthenticationPolicy policy = new DefaultAuthenticationPolicy();
        plugin.setAuthenticationPolicy(policy);
        assertSame(policy, plugin.getAuthenticationPolicy());
        assertSame(policy, plugin.getAuthenticationFilter().getAuthenticationPolicy());
        assertSame(policy, ((DefaultConnectionSubjectFactory) plugin.getSubjectFilter().getConnectionSubjectFactory()).getAuthenticationPolicy());
    }

    public void testAuthorizationEnabledWhenNotInstalled() {
        ShiroPlugin plugin = new ShiroPlugin();
        assertTrue(plugin.isAuthorizationEnabled());

        plugin.setAuthorizationEnabled(false);
        assertFalse(plugin.isAuthorizationEnabled());

        plugin.setAuthorizationEnabled(true);
        assertTrue(plugin.isAuthorizationEnabled());
    }

    public void testAuthorizationEnabledWhenInstalled() throws Exception {
        ShiroPlugin plugin = new ShiroPlugin();
        plugin.setEnvironment(new DefaultEnvironment());
        plugin.installPlugin(new MutableBrokerFilter(null));

        assertTrue(plugin.isAuthorizationEnabled());

        plugin.setAuthorizationEnabled(false);
        assertFalse(plugin.isAuthorizationEnabled());

        plugin.setAuthorizationEnabled(true);
        assertTrue(plugin.isAuthorizationEnabled());
    }


    public void testSimple() throws Exception {
        ShiroPlugin plugin = createPlugin("classpath:minimal.shiro.ini");
        this.broker = createBroker(plugin);
        start();
        reconnect();
    }

    public void testDisabled() throws Exception {
        ShiroPlugin plugin = createPlugin("classpath:nosystem.shiro.ini");
        plugin.setEnabled(false);
        this.broker = createBroker(plugin);
        start();
    }

    public void testRuntimeDisableEnableChanges() throws Exception {
        ShiroPlugin plugin = createPlugin("classpath:nosystem.shiro.ini");
        ((DefaultAuthenticationPolicy) plugin.getAuthenticationPolicy()).setVmConnectionAuthenticationRequired(true);
        plugin.setEnabled(false);
        this.broker = createBroker(plugin);
        start();

        //connection has no credentials.  When disabled, this should succeed:
        reconnect();

        //now enable the plugin and assert that credentials are required:
        plugin.setEnabled(true);

        try {
            reconnect();
            fail("Connections without passwords in this configuration should fail.");
        } catch (JMSException expected) {
            assertTrue(expected.getCause() instanceof AuthenticationException);
        }

        //this should work now that we're authenticating:
        reconnect("foo", "bar");
    }

    static class SecureJmsResourceProvider extends JmsResourceProvider {

        /**
         * Creates a connection, authenticating with the specified username and password.
         *
         * @see org.apache.activemq.test.JmsResourceProvider#createConnection(javax.jms.ConnectionFactory)
         */
        public Connection createConnection(ConnectionFactory cf, String username, String password) throws JMSException {
            Connection connection = cf.createConnection(username, password);
            if (getClientID() != null) {
                connection.setClientID(getClientID());
            }
            return connection;
        }
    }
}
