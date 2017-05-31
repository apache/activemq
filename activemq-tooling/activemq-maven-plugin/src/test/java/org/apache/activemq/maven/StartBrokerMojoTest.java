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
package org.apache.activemq.maven;

import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.TransportConnector;
import org.apache.maven.model.Build;
import org.apache.maven.plugin.logging.Log;
import org.apache.maven.project.MavenProject;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.LinkedList;
import java.util.List;
import java.util.Properties;

import static org.junit.Assert.*;

public class StartBrokerMojoTest {
    /**
     * Main object-under-test; configured with mocks and defaults.
     */
    private StartBrokerMojo             startBrokerMojo;

    /**
     * Secondary object-under-test primarily for setter/getter testing; available for all tests without any
     * configuration (i.e. raw).
     */
    private StartBrokerMojo             startBrokerMojoRaw;

    ////
    // MOCKS
    ////
    private MavenProject                mockMavenProject;
    private Build                       mockBuild;
    private Properties                  mockMavenProperties;
    private MavenBrokerManager          mockBrokerManager;
    private XBeanFileResolver           mockXbeanFileResolver;
    private BrokerService               mockBrokerService;
    private Log                         mockMavenLog;

    ////
    // Test Objects
    ////
    private Properties                  systemProperties;
    private List<TransportConnector>    transportConnectorList;
    private Exception                   testException;

    @Before
    public void setupTest () {
        //
        // Create the objects-under-test
        //
        this.startBrokerMojo    = new StartBrokerMojo();
        this.startBrokerMojoRaw = new StartBrokerMojo();

        //
        // Create mocks
        //
        this.mockMavenProject       = Mockito.mock(MavenProject.class);
        this.mockBuild              = Mockito.mock(Build.class);
        this.mockMavenProperties    = Mockito.mock(Properties.class);
        this.mockBrokerManager      = Mockito.mock(MavenBrokerManager.class);
        this.mockXbeanFileResolver  = Mockito.mock(XBeanFileResolver.class);
        this.mockBrokerService      = Mockito.mock(BrokerService.class);
        this.mockMavenLog           = Mockito.mock(Log.class);

        //
        // Prepare other test objects and configure the object-under-test.
        //
        this.transportConnectorList = new LinkedList<TransportConnector>();
        this.systemProperties       = new Properties();
        this.testException          = new Exception("x-test-exc-x");

        this.startBrokerMojo.setBrokerManager(this.mockBrokerManager);
        this.startBrokerMojo.setxBeanFileResolver(this.mockXbeanFileResolver);
        this.startBrokerMojo.setProject(this.mockMavenProject);
        this.startBrokerMojo.setConnectorPropertyNameFormat("x-name-%s");
        this.startBrokerMojo.setConfigUri("x-config-uri-x");
        this.startBrokerMojo.setFork(false);
        this.startBrokerMojo.setSystemProperties(this.systemProperties);
        this.startBrokerMojo.setLog(this.mockMavenLog);

        //
        // Define standard mock interactions.
        //
        Mockito.when(this.mockBrokerManager.getBroker()).thenReturn(this.mockBrokerService);
        Mockito.when(this.mockMavenProject.getProperties()).thenReturn(this.mockMavenProperties);
        Mockito.when(this.mockMavenProject.getBuild()).thenReturn(this.mockBuild);
        Mockito.when(this.mockBuild.getDirectory()).thenReturn("x-proj-dir-x");
        Mockito.when(this.mockBrokerService.getTransportConnectors()).thenReturn(transportConnectorList);
    }

    /**
     * Test the setter and getter for propertyNameFormat.
     */
    @Test
    public void testSetGetConnectorPropertyNameFormat () {
        assertEquals(StartBrokerMojo.DEFAULT_CONNECTOR_PROPERTY_NAME_FORMAT,
                     this.startBrokerMojoRaw.getConnectorPropertyNameFormat());

        this.startBrokerMojoRaw.setConnectorPropertyNameFormat("x-name-format-x");
        assertEquals("x-name-format-x", this.startBrokerMojoRaw.getConnectorPropertyNameFormat());
    }

    /**
     * Test the setter and getter for configUri.
     */
    @Test
    public void testSetGetConfigUri () {
        assertNull(this.startBrokerMojoRaw.getConfigUri());

        this.startBrokerMojoRaw.setConfigUri("x-config-uri-x");
        assertEquals("x-config-uri-x", this.startBrokerMojoRaw.getConfigUri());
    }

    /**
     * Test the setter and getter for mavenProject.
     */
    @Test
    public void testSetGetMavenProject () {
        assertNull(this.startBrokerMojoRaw.getProject());

        this.startBrokerMojoRaw.setProject(this.mockMavenProject);
        assertSame(this.mockMavenProject, this.startBrokerMojoRaw.getProject());
    }

    /**
     * Test the setter and getter for fork.
     */
    @Test
    public void testSetGetFork () {
        assertFalse(this.startBrokerMojoRaw.isFork());

        this.startBrokerMojoRaw.setFork(true);
        assertTrue(this.startBrokerMojoRaw.isFork());
    }

    /**
     * Test the setter and getter for skip.
     */
    @Test
    public void testSetGetSkip () {
        assertFalse(this.startBrokerMojoRaw.isSkip());

        this.startBrokerMojoRaw.setSkip(true);
        assertTrue(this.startBrokerMojoRaw.isSkip());
    }

    /**
     * Test the setter and getter for systemProperties.
     */
    @Test
    public void testSetGetSystemProperties () {
        assertNull(this.startBrokerMojoRaw.getSystemProperties());

        this.startBrokerMojoRaw.setSystemProperties(this.systemProperties);
        assertSame(this.systemProperties, this.startBrokerMojoRaw.getSystemProperties());
    }

    /**
     * Test the setter and getter for brokerManager.
     */
    @Test
    public void testSetGetBrokerManager () {
        assertNull(this.startBrokerMojoRaw.getBrokerManager());

        this.startBrokerMojoRaw.setBrokerManager(this.mockBrokerManager);
        assertSame(this.mockBrokerManager, this.startBrokerMojoRaw.getBrokerManager());
    }

    /**
     * Test the setter and getter for xbeanFileResolver.
     */
    @Test
    public void testSetGetXbeanFileResolver () {
        assertTrue(this.startBrokerMojoRaw.getxBeanFileResolver() instanceof XBeanFileResolver);
        assertNotSame(this.mockXbeanFileResolver, this.startBrokerMojoRaw.getxBeanFileResolver());

        this.startBrokerMojoRaw.setxBeanFileResolver(this.mockXbeanFileResolver);
        assertSame(this.mockXbeanFileResolver, this.startBrokerMojoRaw.getxBeanFileResolver());
    }

    /**
     * Test normal execution of the mojo leads to startup of the broker.
     *
     * @throws Exception
     */
    @Test
    public void testExecute () throws Exception {
        this.startBrokerMojo.execute();

        Mockito.verify(this.mockBrokerManager).start(false, "x-config-uri-x");
    }

    /**
     * Test the registration of a single transport connector URI when a broker with only one connector is started.
     */
    @Test
    public void testExecuteRegistersTransportConnectorOneUri () throws Exception {
        this.startBrokerMojo.setProject(this.mockMavenProject);

        this.createTestTransportConnectors("openwire-client");
        this.startBrokerMojo.execute();

        Mockito.verify(this.mockMavenProperties).setProperty("x-name-openwire-client", "x-pub-addr-for-openwire-client");
    }

    /**
     * Test the registration of multiple transport connector URIs when a broker with multiple connectors is started.
     *
     * @throws Exception
     */
    @Test
    public void testExecuteRegistersTransportConnectorMultiUri () throws Exception {
        this.createTestTransportConnectors("connector1", "connector2", "connector3");

        this.startBrokerMojo.execute();

        Mockito.verify(this.mockMavenProperties).setProperty("x-name-connector1", "x-pub-addr-for-connector1");
        Mockito.verify(this.mockMavenProperties).setProperty("x-name-connector2", "x-pub-addr-for-connector2");
        Mockito.verify(this.mockMavenProperties).setProperty("x-name-connector3", "x-pub-addr-for-connector3");
    }

    /**
     * Test handling when TransportConnector.getPublishableConnectString() throws an exception.
     *
     * @throws Exception
     */
    @Test
    public void testExceptionOnGetPublishableConnectString () throws Exception {
        TransportConnector mockTransportConnector = Mockito.mock(TransportConnector.class);

        Mockito.when(mockTransportConnector.toString()).thenReturn("x-conn-x");
        Mockito.when(mockTransportConnector.getPublishableConnectString()).thenThrow(testException);

        this.transportConnectorList.add(mockTransportConnector);

        this.startBrokerMojo.execute();

        Mockito.verify(this.mockMavenLog).warn("error on obtaining broker connector uri; connector=x-conn-x",
                this.testException);
    }

    /**
     * Test that an xbean configuration file URI is transformed on use.
     *
     * @throws Exception
     */
    @Test
    public void testUseXbeanConfigFile () throws Exception {
        Mockito.when(this.mockXbeanFileResolver.isXBeanFile("x-config-uri-x")).thenReturn(true);
        Mockito.when(this.mockXbeanFileResolver.toUrlCompliantAbsolutePath("x-config-uri-x"))
                .thenReturn("x-transformed-uri-x");

        this.startBrokerMojo.execute();

        Mockito.verify(this.mockMavenLog).debug("configUri before transformation: x-config-uri-x");
        Mockito.verify(this.mockMavenLog).debug("configUri after transformation: x-transformed-uri-x");
    }

    /**
     * Test that a URI that does not represent an xbean configuration file is not translated.
     *
     * @throws Exception
     */
    @Test
    public void testDoNotUseXbeanConfigFile () throws Exception {
        Mockito.when(this.mockXbeanFileResolver.isXBeanFile("x-config-uri-x")).thenReturn(false);

        this.startBrokerMojo.execute();

        Mockito.verify(this.mockMavenLog, Mockito.times(0)).debug("configUri before transformation: x-config-uri-x");
        Mockito.verify(this.mockMavenLog, Mockito.times(0))
                .debug("configUri after transformation: x-transformed-uri-x");
    }

    /**
     * Test that execution of the mojo is skipped if so configured.
     *
     * @throws Exception
     */
    @Test
    public void testSkip () throws Exception {
        this.startBrokerMojo.setSkip(true);
        this.startBrokerMojo.execute();

        Mockito.verify(this.mockMavenLog).info("Skipped execution of ActiveMQ Broker");
        Mockito.verifyNoMoreInteractions(this.mockBrokerManager);
    }

    /**
     * Add mock, test transport connectors with the given names.
     *
     * @param connectorNames names of the mock connectors to add for the test.
     * @throws Exception
     */
    protected void createTestTransportConnectors(String... connectorNames) throws Exception {
        for ( String oneConnectorName : connectorNames ) {
            //
            // Mock the connector
            //
            TransportConnector mockConnector = Mockito.mock(TransportConnector.class);

            //
            // Return the connector name on getName() and a unique string on getPublishableConnectString().
            //
            Mockito.when(mockConnector.getName()).thenReturn(oneConnectorName);
            Mockito.when(mockConnector.getPublishableConnectString()).thenReturn("x-pub-addr-for-" + oneConnectorName);

            //
            // Add to the test connector list.
            //
            this.transportConnectorList.add(mockConnector);
        }
    }
}
