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
package org.apache.activemq.web.config;

import org.osgi.framework.BundleContext;
import org.osgi.framework.Constants;
import org.osgi.framework.FrameworkUtil;
import org.osgi.framework.ServiceRegistration;
import org.osgi.service.cm.ConfigurationException;
import org.osgi.service.cm.ManagedService;

import javax.jms.ConnectionFactory;
import javax.management.remote.JMXServiceURL;
import java.util.Collection;
import java.util.Dictionary;
import java.util.Hashtable;

public class OsgiConfiguration extends AbstractConfiguration implements ManagedService {

    private ServiceRegistration service;

    private String jmxUrl = "service:jmx:rmi:///jndi/rmi://localhost:1099/karaf-root";
    private String jmxUser;
    private String jmxPassword;

    private String jmsUrl = "tcp://localhost:61616";
    private String jmsUser;
    private String jmsPassword;

    public OsgiConfiguration() {

        BundleContext context = FrameworkUtil.getBundle(getClass()).getBundleContext();
        Dictionary<String, String> properties = new Hashtable<String, String>();
        properties.put(Constants.SERVICE_PID, "org.apache.activemq.webconsole");
        service = context.registerService(ManagedService.class.getName(),
            this, properties);

    }

    @Override
    public String getJmxPassword() {
        return jmxPassword;
    }

    @Override
    public Collection<JMXServiceURL> getJmxUrls() {
        return makeJmxUrls(jmxUrl);
    }

    @Override
    public String getJmxUser() {
        return jmxUser;
    }

    @Override
    public ConnectionFactory getConnectionFactory() {
        return makeConnectionFactory(jmsUrl, jmsUser, jmsPassword);
    }

    @Override
    public void updated(Dictionary dictionary) throws ConfigurationException {
        if (dictionary != null) {
            jmxUrl = (String) dictionary.get(SystemPropertiesConfiguration.PROPERTY_JMX_URL);
            if (jmxUrl == null) {
                throw new IllegalArgumentException("A JMS-url must be specified (system property " + SystemPropertiesConfiguration.PROPERTY_JMX_URL);
            }
            jmxUser = (String) dictionary.get(SystemPropertiesConfiguration.PROPERTY_JMX_USER);
            jmxPassword = (String) dictionary.get(SystemPropertiesConfiguration.PROPERTY_JMX_PASSWORD);
            jmsUrl = (String) dictionary.get(SystemPropertiesConfiguration.PROPERTY_JMS_URL);
            jmsUser = (String) dictionary.get(SystemPropertiesConfiguration.PROPERTY_JMS_USER);
            jmsPassword = (String) dictionary.get(SystemPropertiesConfiguration.PROPERTY_JMS_PASSWORD);
        }
    }
}
