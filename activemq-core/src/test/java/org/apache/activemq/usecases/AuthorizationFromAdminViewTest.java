/* 
 * AuthorizationFromAdminViewTest 
 * Date: Oct 21, 2010 
 * Copyright: Sony BMG 
 * Created by: baraza 
 */
package org.apache.activemq.usecases;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerFactory;
import org.apache.activemq.broker.BrokerPlugin;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.security.AuthorizationPlugin;
import org.apache.activemq.security.SimpleAuthorizationMap;

public class AuthorizationFromAdminViewTest extends org.apache.activemq.TestSupport {

    private BrokerService broker;

    protected ActiveMQConnectionFactory createConnectionFactory() throws Exception {
        return new ActiveMQConnectionFactory("vm://" + getName());
    }

    protected void setUp() throws Exception {
        createBroker();
        super.setUp();
    }

    protected void tearDown() throws Exception {
        super.tearDown();
        destroyBroker();
    }

    private void createBroker() throws Exception {
        broker = BrokerFactory.createBroker("broker:(vm://localhost)");
        broker.setPersistent(false);
        broker.setBrokerName(getName());

        AuthorizationPlugin plugin = new AuthorizationPlugin();
        plugin.setMap(new SimpleAuthorizationMap());
        BrokerPlugin[] plugins = new BrokerPlugin[] {plugin};
        broker.setPlugins(plugins);

        broker.start();
    }

    private void destroyBroker() throws Exception {
        if (broker != null)
            broker.stop();
    }

    public void testAuthorizationFromAdminView() throws Exception {
        broker.getAdminView().addQueue(getDestinationString());
    }
}
