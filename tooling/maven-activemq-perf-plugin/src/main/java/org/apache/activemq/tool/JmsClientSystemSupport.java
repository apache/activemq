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

import java.util.Properties;
import java.util.Iterator;

public abstract class JmsClientSystemSupport {
    private static final Log log = LogFactory.getLog(JmsClientSystemSupport.class);

    public static final String PREFIX_CONFIG_SYSTEM_TEST = "sysTest.";

    protected Properties sysTestSettings   = new Properties();
    protected Properties samplerSettings   = new Properties();
    protected Properties jmsClientSettings = new Properties();
    protected ThreadGroup clientThreadGroup;
    protected PerfMeasurementTool performanceSampler;

    protected int numClients = 1;

    public void runSystemTest() {
        // Create a new copy of the settings to ensure immutability
        final Properties clientSettings = getJmsClientSettings();

        // Create performance sampler
        performanceSampler = new PerfMeasurementTool();
        performanceSampler.setSamplerSettings(samplerSettings);

        clientThreadGroup = new ThreadGroup(getThreadGroupName());
        for (int i=0; i<numClients; i++) {
            final String clientName = getClientName() + i;
            Thread t = new Thread(clientThreadGroup, new Runnable() {
                public void run() {
                    runJmsClient(clientName, clientSettings);
                }
            });
            t.setName(getThreadName() + i);
            t.start();
        }

        performanceSampler.startSampler();
    }

    public abstract void runJmsClient(String clientName, Properties clientSettings);

    public String getClientName() {
        return "JMS Client: ";
    }

    public String getThreadName() {
        return "JMS Client Thread: ";
    }

    public String getThreadGroupName() {
        return "JMS Clients Thread Group";
    }

    public PerfMeasurementTool getPerformanceSampler() {
        return performanceSampler;
    }

    public void setPerformanceSampler(PerfMeasurementTool performanceSampler) {
        this.performanceSampler = performanceSampler;
    }

    public Properties getSettings() {
        Properties allSettings = new Properties();
        allSettings.putAll(sysTestSettings);
        allSettings.putAll(samplerSettings);
        allSettings.putAll(jmsClientSettings);
        return allSettings;
    }

    public void setSettings(Properties settings) {
        for (Iterator i=settings.keySet().iterator(); i.hasNext();) {
            String key = (String)i.next();
            String val = settings.getProperty(key);
            setProperty(key, val);
        }
        ReflectionUtil.configureClass(this, sysTestSettings);
    }

    public void setProperty(String key, String value) {
        if (key.startsWith(PREFIX_CONFIG_SYSTEM_TEST)) {
            sysTestSettings.setProperty(key, value);
        } else if (key.startsWith(PerfMeasurementTool.PREFIX_CONFIG_SYSTEM_TEST)) {
            samplerSettings.setProperty(key, value);
        } else {
            jmsClientSettings.setProperty(key, value);
        }
    }

    public Properties getSysTestSettings() {
        return sysTestSettings;
    }

    public void setSysTestSettings(Properties sysTestSettings) {
        this.sysTestSettings = sysTestSettings;
        ReflectionUtil.configureClass(this, sysTestSettings);
    }

    public Properties getSamplerSettings() {
        return samplerSettings;
    }

    public void setSamplerSettings(Properties samplerSettings) {
        this.samplerSettings = samplerSettings;
    }

    public Properties getJmsClientSettings() {
        return jmsClientSettings;
    }

    public void setJmsClientSettings(Properties jmsClientSettings) {
        this.jmsClientSettings = jmsClientSettings;
    }

    public int getNumClients() {
        return numClients;
    }

    public void setNumClients(int numClients) {
        this.numClients = numClients;
    }
}
