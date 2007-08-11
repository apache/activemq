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
package org.apache.activemq.tool;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.activemq.tool.sampler.ThroughputSamplerTask;
import org.apache.activemq.tool.sampler.CpuSamplerTask;
import org.apache.activemq.tool.reports.PerformanceReportWriter;
import org.apache.activemq.tool.reports.XmlFilePerfReportWriter;
import org.apache.activemq.tool.reports.VerbosePerfReportWriter;
import org.apache.activemq.tool.properties.JmsClientSystemProperties;
import org.apache.activemq.tool.properties.AbstractObjectProperties;
import org.apache.activemq.tool.properties.JmsFactoryProperties;
import org.apache.activemq.tool.properties.ReflectionUtil;
import org.apache.activemq.tool.properties.JmsClientProperties;
import org.apache.activemq.tool.spi.SPIConnectionFactory;

import javax.jms.ConnectionFactory;
import javax.jms.JMSException;
import javax.jms.ConnectionMetaData;
import java.util.Properties;
import java.util.Enumeration;
import java.io.IOException;
import java.io.File;
import java.io.FileInputStream;

public abstract class AbstractJmsClientSystem extends AbstractObjectProperties {
    private static final Log log = LogFactory.getLog(AbstractJmsClientSystem.class);

    private int clientDestIndex, clientDestCount;

    protected ThreadGroup clientThreadGroup;
    protected ConnectionFactory jmsConnFactory;

    // Properties
    protected JmsFactoryProperties factory  = new JmsFactoryProperties();
    protected ThroughputSamplerTask tpSampler = new ThroughputSamplerTask();
    protected CpuSamplerTask cpuSampler = new CpuSamplerTask();

    public void runSystemTest() throws JMSException {
        // Create connection factory
        jmsConnFactory = loadJmsFactory(getSysTest().getSpiClass(), factory.getFactorySettings());

        setProviderMetaData(jmsConnFactory.createConnection().getMetaData(), getJmsClientProperties());

        // Create performance sampler
        PerformanceReportWriter writer = createPerfWriter();
        tpSampler.setPerfReportWriter(writer);
        cpuSampler.setPerfReportWriter(writer);

        writer.openReportWriter();
        writer.writeProperties("jvmSettings", System.getProperties());
        writer.writeProperties("testSystemSettings", ReflectionUtil.retrieveObjectProperties(getSysTest()));
        writer.writeProperties("jmsFactorySettings", ReflectionUtil.retrieveObjectProperties(jmsConnFactory));
        writer.writeProperties("jmsClientSettings", ReflectionUtil.retrieveObjectProperties(getJmsClientProperties()));
        writer.writeProperties("tpSamplerSettings", ReflectionUtil.retrieveObjectProperties(tpSampler));
        writer.writeProperties("cpuSamplerSettings", ReflectionUtil.retrieveObjectProperties(cpuSampler));

        clientThreadGroup = new ThreadGroup(getSysTest().getClientPrefix() + " Thread Group");
        for (int i=0; i<getSysTest().getNumClients(); i++) {
            distributeDestinations(getSysTest().getDestDistro(), i, getSysTest().getNumClients(), getSysTest().getTotalDests());

            final String clientName = getSysTest().getClientPrefix() + i;
            final int clientDestIndex = this.clientDestIndex;
            final int clientDestCount = this.clientDestCount;
            Thread t = new Thread(clientThreadGroup, new Runnable() {
                public void run() {
                    runJmsClient(clientName, clientDestIndex, clientDestCount);
                }
            });
            t.setName(getSysTest().getClientPrefix() + i + " Thread");
            t.start();
        }

        // Run samplers
        if (getSysTest().getSamplers().indexOf(JmsClientSystemProperties.SAMPLER_TP) > -1) {
            tpSampler.startSampler();
        }

        if (getSysTest().getSamplers().indexOf(JmsClientSystemProperties.SAMPLER_CPU) > -1) {
            try {
                cpuSampler.createPlugin();
                cpuSampler.startSampler();
            } catch (IOException e) {
                log.warn("Unable to start CPU sampler plugin. Reason: " + e.getMessage());
            }
        }

        tpSampler.waitUntilDone();
        cpuSampler.waitUntilDone();

        writer.closeReportWriter();
    }

    public ThroughputSamplerTask getTpSampler() {
        return tpSampler;
    }

    public void setTpSampler(ThroughputSamplerTask tpSampler) {
        this.tpSampler = tpSampler;
    }

    public CpuSamplerTask getCpuSampler() {
        return cpuSampler;
    }

    public void setCpuSampler(CpuSamplerTask cpuSampler) {
        this.cpuSampler = cpuSampler;
    }

    public JmsFactoryProperties getFactory() {
        return factory;
    }

    public void setFactory(JmsFactoryProperties factory) {
        this.factory = factory;
    }

    public abstract JmsClientSystemProperties getSysTest();
    public abstract void setSysTest(JmsClientSystemProperties sysTestProps);
    public abstract JmsClientProperties getJmsClientProperties();

    protected PerformanceReportWriter createPerfWriter() {
        if (getSysTest().getReportType().equalsIgnoreCase(JmsClientSystemProperties.REPORT_XML_FILE)) {
            String reportName;

            if ((reportName = getSysTest().getReportName()) == null) {
                reportName = getSysTest().getClientPrefix() + "_" +
                             "numClients" + getSysTest().getNumClients() + "_" +
                             "numDests" + getSysTest().getTotalDests() + "_" +
                             getSysTest().getDestDistro();
            }
            return new XmlFilePerfReportWriter(getSysTest().getReportDir(), reportName);
        } else if (getSysTest().getReportType().equalsIgnoreCase(JmsClientSystemProperties.REPORT_VERBOSE)) {
            return new VerbosePerfReportWriter();
        } else {
            // Use verbose if unknown report type
            return new VerbosePerfReportWriter();
        }
    }

    protected void distributeDestinations(String distroType, int clientIndex, int numClients, int numDests) {
        if (distroType.equalsIgnoreCase(JmsClientSystemProperties.DEST_DISTRO_ALL)) {
            clientDestCount = numDests;
            clientDestIndex = 0;
        } else if (distroType.equalsIgnoreCase(JmsClientSystemProperties.DEST_DISTRO_EQUAL)) {
            int destPerClient = (numDests / numClients);
            // There are equal or more destinations per client
            if (destPerClient > 0) {
                clientDestCount = destPerClient;
                clientDestIndex = destPerClient * clientIndex;
            // If there are more clients than destinations, share destinations per client
            } else {
                clientDestCount = 1; // At most one destination per client
                clientDestIndex = clientIndex % numDests;
            }
        } else if (distroType.equalsIgnoreCase(JmsClientSystemProperties.DEST_DISTRO_DIVIDE)) {
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

                clientDestCount = destPerClient;
                clientDestIndex = nextIndex;

            // If there are more clients than destinations, share destinations per client
            } else {
                clientDestCount = 1; // At most one destination per client
                clientDestIndex = clientIndex % numDests;
            }

        // Send to all for unknown behavior
        } else {
            log.warn("Unknown destination distribution type: " + distroType);
            clientDestCount = numDests;
            clientDestIndex = 0;
        }
    }

    protected ConnectionFactory loadJmsFactory(String spiClass, Properties factorySettings) throws JMSException {
        try {
            Class spi = Class.forName(spiClass);
            SPIConnectionFactory spiFactory = (SPIConnectionFactory)spi.newInstance();
            ConnectionFactory jmsFactory = spiFactory.createConnectionFactory(factorySettings);
            log.info("Created: " + jmsFactory.getClass().getName() + " using SPIConnectionFactory: " + spiFactory.getClass().getName());
            return jmsFactory;
        } catch (Exception e) {
            e.printStackTrace();
            throw new JMSException(e.getMessage());
        }
    }

    protected void setProviderMetaData(ConnectionMetaData metaData, JmsClientProperties props) throws JMSException {
        props.setJmsProvider(metaData.getJMSProviderName() + "-" + metaData.getProviderVersion());
        props.setJmsVersion(metaData.getJMSVersion());

        String jmsProperties = "";
        Enumeration jmsProps = metaData.getJMSXPropertyNames();
        while (jmsProps.hasMoreElements()) {
            jmsProperties += (jmsProps.nextElement().toString() + ",");
        }
        if (jmsProperties.length() > 0) {
            // Remove the last comma
            jmsProperties = jmsProperties.substring(0, jmsProperties.length()-1);
        }
        props.setJmsProperties(jmsProperties);
    }

    protected abstract void runJmsClient(String clientName, int clientDestIndex, int clientDestCount);

    protected static Properties parseStringArgs(String[] args) {
        File configFile = null;
        Properties props = new Properties();

        if (args == null || args.length == 0) {
            return props; // Empty properties
        }

        for (int i=0; i<args.length; i++) {
            String arg = args[i];
            if (arg.startsWith("-D") || arg.startsWith("-d")) {
                arg = arg.substring(2);
            }
            int index  = arg.indexOf("=");
            String key = arg.substring(0, index);
            String val = arg.substring(index + 1);

            if (key.equalsIgnoreCase("sysTest.propsConfigFile")) {
                if (!val.endsWith(".properties")) {
                    val += ".properties";
                }
                configFile = new File(val);
            }
            props.setProperty(key, val);
        }

        Properties fileProps = new Properties();
        try {
            if (configFile != null) {
                log.info("Loading properties file: " + configFile.getAbsolutePath());
                fileProps.load(new FileInputStream(configFile));
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        // Overwrite file settings with command line settings
        fileProps.putAll(props);
        return fileProps;
    }
}
