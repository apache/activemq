/**
 *
 * Copyright 2005-2006 The Apache Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.activemq.tool;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.activemq.tool.spi.SPIConnectionFactory;

import javax.jms.ConnectionFactory;
import javax.jms.JMSException;
import java.util.Properties;
import java.util.Iterator;

public class JmsFactorySupport {
    private static final Log log = LogFactory.getLog(JmsFactorySupport.class);

    private static final String PREFIX_CONFIG_FACTORY = "factory.";

    private   SPIConnectionFactory spiFactory;
    protected ConnectionFactory jmsFactory;
    protected Properties jmsFactorySettings = new Properties();

    public ConnectionFactory createConnectionFactory(String spiClass) throws JMSException {
        jmsFactory = loadJmsFactory(spiClass, jmsFactorySettings);
        return jmsFactory;
    }

    protected ConnectionFactory loadJmsFactory(String spiClass, Properties factorySettings) throws JMSException {
        try {
            Class spi = Class.forName(spiClass);
            spiFactory = (SPIConnectionFactory)spi.newInstance();
            ConnectionFactory jmsFactory = spiFactory.createConnectionFactory(factorySettings);
            log.debug("Created: " + jmsFactory.getClass().getName() + " using SPIConnectionFactory: " + spiFactory.getClass().getName());
            return jmsFactory;
        } catch (Exception e) {
            e.printStackTrace();
            throw new JMSException(e.getMessage());
        }
    }

    public ConnectionFactory getJmsFactory() {
        return jmsFactory;
    }

    public Properties getJmsFactorySettings() {
        return jmsFactorySettings;
    }

    public void setJmsFactorySettings(Properties jmsFactorySettings) {
        this.jmsFactorySettings = jmsFactorySettings;
        try {
            spiFactory.configureConnectionFactory(jmsFactory, jmsFactorySettings);
        } catch (Exception e) {
            log.warn(e);
        }
    }

    public Properties getSettings() {
        return jmsFactorySettings;
    }

    public void setSettings(Properties settings) {
        for (Iterator i=settings.keySet().iterator(); i.hasNext();) {
            String key = (String)i.next();
            String val = settings.getProperty(key);
            setProperty(key, val);
        }
        try {
            spiFactory.configureConnectionFactory(jmsFactory, jmsFactorySettings);
        } catch (Exception e) {
            log.warn(e);
        }
    }

    public void setProperty(String key, String value) {
        if (key.startsWith(PREFIX_CONFIG_FACTORY)) {
            jmsFactorySettings.setProperty(key, value);
        } else {
            log.warn("Unknown setting: " + key + "=" + value);
        }
    }
}
