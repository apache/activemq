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
package org.apache.activemq.broker;

import java.io.File;

import org.apache.activemq.network.NetworkConnector;
import org.apache.activemq.store.PersistenceAdapter;
import org.apache.activemq.util.LargeFile;
import org.apache.activemq.util.StoreUtil;
import org.junit.Test;
import org.mockito.MockedStatic;
import junit.framework.TestCase;
import org.mockito.Mockito;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.times;

/**
 * Tests for the BrokerService class
 * 
 * @author chirino
 */
public class BrokerServiceTest extends TestCase {
    public void testAddRemoveTransportsWithJMX() throws Exception {
        BrokerService service = new BrokerService();
        service.setUseJmx(true);
        service.setPersistent(false);
        TransportConnector connector = service.addConnector("tcp://localhost:0");
        service.start();

        service.removeConnector(connector);
        connector.stop();
        service.stop();
    }

    public void testAddRemoveTransportsWithoutJMX() throws Exception {
        BrokerService service = new BrokerService();
        service.setPersistent(false);
        service.setUseJmx(false);
        TransportConnector connector = service.addConnector("tcp://localhost:0");
        service.start();

        service.removeConnector(connector);
        connector.stop();
        service.stop();
    }

    public void testAddRemoveNetworkWithJMX() throws Exception {
        BrokerService service = new BrokerService();
        service.setPersistent(false);
        service.setUseJmx(true);
        NetworkConnector connector = service.addNetworkConnector("multicast://default?group=group-"+System.currentTimeMillis());
        service.start();

        service.removeNetworkConnector(connector);
        connector.stop();
        service.stop();
    }

    public void testAddRemoveNetworkWithoutJMX() throws Exception {
        BrokerService service = new BrokerService();
        service.setPersistent(false);
        service.setUseJmx(false);
        NetworkConnector connector = service.addNetworkConnector("multicast://default?group=group-"+System.currentTimeMillis());
        service.start();

        service.removeNetworkConnector(connector);
        connector.stop();
        service.stop();
    }
    
    public void testSystemUsage() {
        BrokerService service = new BrokerService();
        assertEquals( 1024 * 1024 * 1024, service.getSystemUsage().getMemoryUsage().getLimit() );
        assertEquals( 1024L * 1024 * 1024 * 50, service.getSystemUsage().getTempUsage().getLimit() );
        assertEquals( 1024L * 1024 * 1024 * 100, service.getSystemUsage().getStoreUsage().getLimit() );
    }

    public void testSetBrokerNameInvalidChars() {
        final BrokerService brokerService = new BrokerService();

        // All valid
        brokerService.setBrokerName("valid");
        assertEquals("valid", brokerService.getBrokerName());
        brokerService.setBrokerName("valid123");
        assertEquals("valid123", brokerService.getBrokerName());
        brokerService.setBrokerName("this_is_valid");
        assertEquals("this_is_valid", brokerService.getBrokerName());
        brokerService.setBrokerName("this_123_valid");
        assertEquals("this_123_valid", brokerService.getBrokerName());
        brokerService.setBrokerName("valid-name123");
        assertEquals("valid-name123", brokerService.getBrokerName());
        brokerService.setBrokerName("1235.6789");
        assertEquals("1235.6789", brokerService.getBrokerName());
        brokerService.setBrokerName("valid:123");
        assertEquals("valid:123", brokerService.getBrokerName());

        // Test invalid names
        brokerService.setBrokerName("abc?bad");
        assertEquals("abc_bad", brokerService.getBrokerName());
        brokerService.setBrokerName("#");
        assertEquals("_", brokerService.getBrokerName());
        brokerService.setBrokerName("?");
        assertEquals("_", brokerService.getBrokerName());
        brokerService.setBrokerName("invalid%");
        assertEquals("invalid_", brokerService.getBrokerName());
        brokerService.setBrokerName("\\");
        assertEquals("_", brokerService.getBrokerName());
        brokerService.setBrokerName("<>");
        assertEquals("__", brokerService.getBrokerName());
        brokerService.setBrokerName("abc=");
        assertEquals("abc_", brokerService.getBrokerName());
        brokerService.setBrokerName("name:abc=?bad");
        assertEquals("name:abc__bad", brokerService.getBrokerName());
    }

    @Test
    public void testLargeFileSystem() throws Exception {
        BrokerService service = new BrokerService();

        File dataDirectory = new File(service.getBrokerDataDirectory(), "KahaDB");
        File tmpDataDirectory = service.getTmpDataDirectory();

        PersistenceAdapter persistenceAdapter = service.createPersistenceAdapter();
        persistenceAdapter.setDirectory(dataDirectory);
        service.setPersistenceAdapter(persistenceAdapter);
        service.setUseJmx(false);

        try(MockedStatic<StoreUtil> mocked = Mockito.mockStatic(StoreUtil.class)) {
            // Return a simulated handle to a very large file system that will return a negative totalSpace.
            mocked.when(() -> StoreUtil.findParentDirectory(dataDirectory)).thenReturn(new LargeFile(dataDirectory.getParentFile(), "KahaDB"));
            mocked.when(() -> StoreUtil.findParentDirectory(tmpDataDirectory)).thenReturn(tmpDataDirectory.getParentFile());

            try {
                service.start();
                fail("Expect error on negative totalspace");
            } catch (Exception expected) {
                // verify message
                assertTrue(expected.getLocalizedMessage().contains("negative"));
            }
            finally {
                service.stop();
            }

            // configure a 2x value for the fs limit so it can start
            service.getSystemUsage().getStoreUsage().setTotal( service.getSystemUsage().getStoreUsage().getLimit() * 2);

            service.start(true);
            service.stop();

            mocked.verify(() -> StoreUtil.findParentDirectory(any()), times(3));
        }

    }
}
