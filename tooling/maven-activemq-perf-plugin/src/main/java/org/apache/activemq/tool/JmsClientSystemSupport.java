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
    public static final String DEST_DISTRO_ALL    = "all";    // Each client will send/receive to all destination;
    public static final String DEST_DISTRO_EQUAL  = "equal";  // Equally divide the number of destinations to the number of clients
    public static final String DEST_DISTRO_DIVIDE = "divide"; // Divide the destination among the clients, even if some have more destination than others

    protected static final String KEY_CLIENT_DEST_COUNT = "client.destCount";
    protected static final String KEY_CLIENT_DEST_INDEX = "client.destIndex";

    protected Properties sysTestSettings   = new Properties();
    protected Properties samplerSettings   = new Properties();
    protected Properties jmsClientSettings = new Properties();
    protected ThreadGroup clientThreadGroup;
    protected PerfMeasurementTool performanceSampler;
    protected String reportDirectory;

    protected int numClients = 1;
    protected int totalDests = 1;
    protected String destDistro = DEST_DISTRO_ALL;

    public void runSystemTest() {
        // Create performance sampler
        performanceSampler = new PerfMeasurementTool();
        performanceSampler.setSamplerSettings(samplerSettings);

        PerfReportGenerator report = new PerfReportGenerator();
        report.setReportName(this.getClass().getCanonicalName());
        report.setTestSettings(getSettings());
        report.startGenerateReport();

        performanceSampler.setWriter(report.getWriter());

        clientThreadGroup = new ThreadGroup(getThreadGroupName());
        for (int i=0; i<getNumClients(); i++) {
            final Properties clientSettings = new Properties();
            clientSettings.putAll(getJmsClientSettings());
            distributeDestinations(getDestDistro(), i, getNumClients(), getTotalDests(), clientSettings);

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
        performanceSampler.waitForSamplerToFinish(0);

        report.stopGenerateReport();
    }

    protected void distributeDestinations(String distroType, int clientIndex, int numClients, int numDests, Properties clientSettings) {
        if (distroType.equalsIgnoreCase(DEST_DISTRO_ALL)) {
            clientSettings.setProperty(KEY_CLIENT_DEST_COUNT, String.valueOf(numDests));
            clientSettings.setProperty(KEY_CLIENT_DEST_INDEX, "0");
        } else if (distroType.equalsIgnoreCase(DEST_DISTRO_EQUAL)) {
            int destPerClient = (numDests / numClients);
            // There are equal or more destinations per client
            if (destPerClient > 0) {
                clientSettings.setProperty(KEY_CLIENT_DEST_COUNT, String.valueOf(destPerClient));
                clientSettings.setProperty(KEY_CLIENT_DEST_INDEX, String.valueOf(destPerClient * clientIndex));

            // If there are more clients than destinations, share destinations per client
            } else {
                clientSettings.setProperty(KEY_CLIENT_DEST_COUNT, "1"); // At most one destination per client
                clientSettings.setProperty(KEY_CLIENT_DEST_INDEX, String.valueOf(clientIndex % numDests));
            }
        } else if (distroType.equalsIgnoreCase(DEST_DISTRO_DIVIDE)) {
            int destPerClient = (numDests / numClients);
            // There are equal or more destinations per client
            if (destPerClient > 0) {
                int remain = numDests % numClients;
                int nextIndex;
                if (clientIndex < remain) {
                    destPerClient++;
                    nextIndex = clientIndex * destPerClient;
                } else {
                    nextIndex = (clientIndex * destPerClient) + remain;
                }

                clientSettings.setProperty(KEY_CLIENT_DEST_COUNT, String.valueOf(destPerClient));
                clientSettings.setProperty(KEY_CLIENT_DEST_INDEX, String.valueOf(nextIndex));

            // If there are more clients than destinations, share destinations per client
            } else {
                clientSettings.setProperty(KEY_CLIENT_DEST_COUNT, "1"); // At most one destination per client
                clientSettings.setProperty(KEY_CLIENT_DEST_INDEX, String.valueOf(clientIndex % numDests));
            }

        // Send to all for unknown behavior
        } else {
            clientSettings.setProperty(KEY_CLIENT_DEST_COUNT, String.valueOf(numDests));
            clientSettings.setProperty(KEY_CLIENT_DEST_INDEX, "0");
        }
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

    public String getReportDirectory(){
        return reportDirectory;
    }

    public void setReportDirectory(String reportDirectory){
        this.reportDirectory = reportDirectory;
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

    public String getDestDistro() {
        return destDistro;
    }

    public void setDestDistro(String destDistro) {
        this.destDistro = destDistro;
    }

    public int getTotalDests() {
        return totalDests;
    }

    public void setTotalDests(int totalDests) {
        this.totalDests = totalDests;
    }
}
