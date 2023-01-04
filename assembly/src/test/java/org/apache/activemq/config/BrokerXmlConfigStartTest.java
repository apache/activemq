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
package org.apache.activemq.config;

import java.io.File;
import java.io.FileFilter;
import java.io.FileInputStream;
import java.io.IOException;
import java.lang.reflect.Field;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import javax.jms.Connection;

import junit.framework.TestCase;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.TransportConnector;
import org.apache.activemq.transport.stomp.StompConnection;
import org.apache.activemq.util.URISupport;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@RunWith(value = Parameterized.class)
public class BrokerXmlConfigStartTest {

    private static final Logger LOG = LoggerFactory.getLogger(BrokerXmlConfigStartTest.class);
    Properties secProps;

    private String configUrl;
    private String shortName;

    @Parameterized.Parameters(name = "{1}")
    public static Collection<String[]> getTestParameters() throws IOException {
        List<String[]> configUrls = new ArrayList<String[]>();
        configUrls.add(new String[]{"xbean:src/release/conf/activemq.xml", "activemq.xml"});

        String osName=System.getProperty("os.name");
        LOG.info("os.name {} ", osName);
        File sampleConfDir = new File("target/conf");
        String sampleConfDirPath = sampleConfDir.getAbsolutePath();
        if (osName.toLowerCase().contains("windows")) {
            sampleConfDirPath = sampleConfDirPath.substring(2); // Chop off drive letter and :
            sampleConfDirPath = sampleConfDirPath.replace("\\", "/");
        }

        for (File xmlFile : sampleConfDir.listFiles(new FileFilter() {
            public boolean accept(File pathname) {
                return pathname.isFile() &&
                        pathname.getName().startsWith("activemq-") &&
                        pathname.getName().endsWith("xml");
            }})) {
            configUrls.add(new String[]{"xbean:" + sampleConfDirPath + "/" + xmlFile.getName(), xmlFile.getName()});
        }

        return configUrls;
    }


    public BrokerXmlConfigStartTest(String config, String configFileShortName) {
        this.configUrl = config;
        this.shortName = configFileShortName;
    }

    @Test
    public void testStartBrokerUsingXmlConfig1() throws Exception {
        BrokerService broker = null;
        LOG.info("Broker config: " + configUrl);
        System.err.println("Broker config: " + configUrl);
        broker = BrokerFactory.createBroker(configUrl);
        broker.start();
        broker.waitUntilStarted(5000l);
        // alive, now try connect to connect
        try {
            for (TransportConnector transport : broker.getTransportConnectors()) {
                final URI UriToConnectTo = URISupport.removeQuery(transport.getConnectUri());

                if (UriToConnectTo.getScheme().startsWith("stomp")) {
                    LOG.info("validating alive with connection to: " + UriToConnectTo);
                    StompConnection connection = new StompConnection();
                    connection.open(UriToConnectTo.getHost(), UriToConnectTo.getPort());
                    connection.close();
                    break;
                } else if (UriToConnectTo.getScheme().startsWith("tcp")) {
                    LOG.info("validating alive with connection to: " + UriToConnectTo);
                    ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory(UriToConnectTo);
                    Connection connection = connectionFactory.createConnection(secProps.getProperty("activemq.username"),
                            secProps.getProperty("activemq.password"));
                    connection.start();
                    connection.close();
                    break;
                } else {
                    LOG.info("not validating connection to: " + UriToConnectTo);
                }
            }
        } finally {
            if (broker != null) {
                broker.stop();
                broker.waitUntilStopped();
                broker = null;
            }
        }
    }

    @Before
    public void setUp() throws Exception {
        System.setProperty("activemq.base", "target");
        System.setProperty("activemq.home", "target"); // not a valid home but ok for xml validation
        System.setProperty("activemq.data", "target");
        System.setProperty("activemq.conf", "target/conf");
        secProps = new Properties();
        secProps.load(new FileInputStream(new File("target/conf/credentials.properties")));
        setEnv("ACTIVEMQ_ENCRYPTION_PASSWORD", "activemq");
    }

    @After
    public void tearDown() throws Exception {
        TimeUnit.SECONDS.sleep(1);
    }

    private void setEnv(String key, String value) {
        try {
            Map<String, String> env = System.getenv();
            Class<?> cl = env.getClass();
            Field field = cl.getDeclaredField("m");
            field.setAccessible(true);
            Map<String, String> writableEnv = (Map<String, String>) field.get(env);
            writableEnv.put(key, value);
        } catch (Exception e) {
            throw new IllegalStateException("Failed to set environment variable", e);
        }
    }
}
