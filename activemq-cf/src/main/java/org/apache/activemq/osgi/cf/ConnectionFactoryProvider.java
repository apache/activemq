/*
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
package org.apache.activemq.osgi.cf;

import java.util.Dictionary;
import java.util.Hashtable;

import javax.jms.ConnectionFactory;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.jms.pool.PooledConnectionFactory;
import org.osgi.framework.BundleContext;
import org.osgi.framework.ServiceRegistration;
import org.osgi.service.component.ComponentContext;
import org.osgi.service.component.annotations.Activate;
import org.osgi.service.component.annotations.Component;
import org.osgi.service.component.annotations.ConfigurationPolicy;
import org.osgi.service.component.annotations.Deactivate;

@Component //
( //
    name = "org.apache.activemq", //
    immediate = true, //
    configurationPolicy = ConfigurationPolicy.REQUIRE //
)
public class ConnectionFactoryProvider {

    private static final String OSGI_JNDI_SERVICE_NAME = "osgi.jndi.service.name";
    private ServiceRegistration<ConnectionFactory> reg;

    @Activate
    public void create(ComponentContext compContext) {
        BundleContext context = compContext.getBundleContext();
        Dictionary<String, Object> config = compContext.getProperties();
        String brokerURL = getString(config, "url", "tcp://localhost:61616");
        String jndiName = getString(config, OSGI_JNDI_SERVICE_NAME, "jms/local");
        String userName = getString(config, "userName", null);
        String password = getString(config, "password", null);
        long expiryTimeout = new Long(getString(config, "expiryTimeout", "0"));
        int idleTimeout = new Integer(getString(config, "idleTimeout", "30000"));
        int maxConnections = new Integer(getString(config, "maxConnections", "8"));
        ActiveMQConnectionFactory cf = new ActiveMQConnectionFactory(brokerURL);
        if (userName != null) {
            cf.setUserName(userName);
            cf.setPassword(password);
        }
        PooledConnectionFactory pcf = new PooledConnectionFactory();
        pcf.setConnectionFactory(cf);
        pcf.setExpiryTimeout(expiryTimeout);
        pcf.setIdleTimeout(idleTimeout);
        pcf.setMaxConnections(maxConnections);
        Dictionary<String, String> props = new Hashtable<String, String>();
        props.put(OSGI_JNDI_SERVICE_NAME, jndiName);
        reg = context.registerService(ConnectionFactory.class, pcf, props);
    }
    
    @Deactivate
    public void deactivate() {
        reg.unregister();
    }

    private String getString(Dictionary<String, Object> config, String key, String defaultValue) {
        Object value = config.get(key);
        return value != null ? value.toString() : defaultValue;
    }
}
