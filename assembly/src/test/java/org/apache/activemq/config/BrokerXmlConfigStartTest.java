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
import java.net.URI;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import javax.jms.Connection;
import javax.jms.JMSException;

import junit.framework.TestCase;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.TransportConnection;
import org.apache.activemq.broker.TransportConnector;
import org.apache.activemq.transport.stomp.StompConnection;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class BrokerXmlConfigStartTest extends TestCase {

    private static final Log LOG = LogFactory.getLog(BrokerXmlConfigStartTest.class);
    Properties secProps;
    public void testStartBrokerUsingXmlConfig() throws Exception {
        doTestStartBrokerUsingXmlConfig("xbean:src/release/conf/activemq.xml");
    }

    public void testStartBrokerUsingSampleConfig() throws Exception {
        // resource:copy-resource brings all config files into target/conf
        File sampleConfDir = new File("target/conf");
        
        for (File xmlFile : sampleConfDir.listFiles(new FileFilter() {
            public boolean accept(File pathname) {
                return pathname.isFile() &&
                pathname.getName().startsWith("activemq-") &&
                pathname.getName().endsWith("xml");
            }})) {
            
            doTestStartBrokerUsingXmlConfig("xbean:" + sampleConfDir.getAbsolutePath() + "/" + xmlFile.getName());
        }
    }

    public void doTestStartBrokerUsingXmlConfig(String configUrl) throws Exception {

        BrokerService broker = null;
        LOG.info("Broker config: " + configUrl);
        System.err.println("Broker config: " + configUrl);
        broker = BrokerFactory.createBroker(configUrl);
        // alive, now try connect to connect
        try {
            for (TransportConnector transport : broker.getTransportConnectors()) {
                final URI UriToConnectTo = transport.getConnectUri();
                 
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
                broker = null;
            }
        }
    }

    public void setUp() throws Exception {
        System.setProperty("activemq.base", "target");
        secProps = new Properties();
        secProps.load(new FileInputStream(new File("target/conf/credentials.properties")));
    }
    
    public void tearDown() throws Exception {
        TimeUnit.SECONDS.sleep(1);
    }
}
