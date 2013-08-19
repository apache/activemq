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
package org.apache.activemq;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.util.concurrent.TimeUnit;
import org.apache.activemq.broker.BrokerFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.network.NetworkConnector;
import org.apache.activemq.spring.Utils;
import org.junit.After;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.io.Resource;


import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class NetworkConnectorTest {

    public static final Logger LOG = LoggerFactory.getLogger(NetworkConnectorTest.class);
    public static final int SLEEP = 4; // seconds
    String configurationSeed = "networkConnectorTest";
    BrokerService brokerService;

    public void startBroker(String configFileName) throws Exception {
        brokerService = new BrokerService();
        brokerService = BrokerFactory.createBroker("xbean:org/apache/activemq/" + configFileName + ".xml");
        brokerService.start();
        brokerService.waitUntilStarted();
    }

    @After
    public void stopBroker() throws Exception {
        brokerService.stop();
    }

    @Test
    public void testNewConnector() throws Exception {
        final String brokerConfig = configurationSeed + "-no-nc-broker";
        applyNewConfig(brokerConfig, configurationSeed);
        startBroker(brokerConfig);
        assertTrue("broker alive", brokerService.isStarted());
        assertEquals("no network connectors", 0, brokerService.getNetworkConnectors().size());

        applyNewConfig(brokerConfig, configurationSeed + "-one-nc", SLEEP);

        assertEquals("new network connectors", 1, brokerService.getNetworkConnectors().size());

        // apply again - ensure no change
        NetworkConnector networkConnector = brokerService.getNetworkConnectors().get(0);
        applyNewConfig(brokerConfig, configurationSeed + "-one-nc");
        assertEquals("no new network connectors", 1, brokerService.getNetworkConnectors().size());
        assertEquals("same instance", networkConnector, brokerService.getNetworkConnectors().get(0));
    }


    @Test
    public void testModConnector() throws Exception {

        final String brokerConfig = configurationSeed + "-one-nc-broker";
        applyNewConfig(brokerConfig, configurationSeed + "-one-nc");
        startBroker(brokerConfig);
        assertTrue("broker alive", brokerService.isStarted());
        assertEquals("one network connectors", 1, brokerService.getNetworkConnectors().size());

        // track the original
        NetworkConnector networkConnector = brokerService.getNetworkConnectors().get(0);
        assertEquals("network ttl is default", 1, networkConnector.getNetworkTTL());

        applyNewConfig(brokerConfig, configurationSeed + "-mod-one-nc", SLEEP);

        assertEquals("still one network connectors", 1, brokerService.getNetworkConnectors().size());

        NetworkConnector modNetworkConnector = brokerService.getNetworkConnectors().get(0);
        assertEquals("got ttl update", 2, modNetworkConnector.getNetworkTTL());

        // apply again - ensure no change
        applyNewConfig(brokerConfig, configurationSeed + "-mod-one-nc", SLEEP);
        assertEquals("no new network connectors", 1, brokerService.getNetworkConnectors().size());
        assertEquals("same instance", modNetworkConnector, brokerService.getNetworkConnectors().get(0));
    }

    @Test
    public void testRemoveConnector() throws Exception {

        final String brokerConfig = configurationSeed + "-two-nc-broker";
        applyNewConfig(brokerConfig, configurationSeed + "-two-nc");
        startBroker(brokerConfig);
        assertTrue("broker alive", brokerService.isStarted());
        assertEquals("correct network connectors", 2, brokerService.getNetworkConnectors().size());

        applyNewConfig(brokerConfig, configurationSeed + "-one-nc", SLEEP);

        assertEquals("one network connectors", 1, brokerService.getNetworkConnectors().size());

        NetworkConnector remainingNetworkConnector = brokerService.getNetworkConnectors().get(0);
        assertEquals("name match", "one", remainingNetworkConnector.getName());
    }

    private void applyNewConfig(String configName, String newConfigName) throws Exception {
        applyNewConfig(configName, newConfigName, 0l);
    }

    private void applyNewConfig(String configName, String newConfigName, long sleep) throws Exception {
        Resource resource = Utils.resourceFromString("org/apache/activemq");
        FileOutputStream current = new FileOutputStream(new File(resource.getFile(), configName + ".xml"));
        FileInputStream modifications = new FileInputStream(new File(resource.getFile(), newConfigName + ".xml"));
        modifications.getChannel().transferTo(0, Long.MAX_VALUE, current.getChannel());
        current.flush();
        LOG.info("Updated: " + current.getChannel());

        if (sleep > 0) {
            // wait for mods to kick in
            TimeUnit.SECONDS.sleep(sleep);
        }
    }
}
