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
package org.apache.activemq.transport.tcp;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.activemq.CombinationTestSupport;
import org.apache.activemq.command.CommandTypes;
import org.apache.activemq.command.WireFormatInfo;
import org.apache.activemq.transport.Transport;
import org.apache.activemq.transport.TransportAcceptListener;
import org.apache.activemq.transport.TransportFactory;
import org.apache.activemq.transport.TransportListener;
import org.apache.activemq.transport.TransportServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WireformatNegociationTest extends CombinationTestSupport {
    private static final Logger LOG = LoggerFactory.getLogger(WireformatNegociationTest.class);

    private TransportServer server;
    private Transport clientTransport;
    private Transport serverTransport;

    private final AtomicReference<WireFormatInfo> clientWF = new AtomicReference<WireFormatInfo>();
    private final AtomicReference<WireFormatInfo> serverWF = new AtomicReference<WireFormatInfo>();
    private final AtomicReference<Exception> asyncError = new AtomicReference<Exception>();
    private final AtomicBoolean ignoreAsycError = new AtomicBoolean();

    private final CountDownLatch negociationCounter = new CountDownLatch(2);

    protected void setUp() throws Exception {
        super.setUp();
    }

    /**
     * @throws Exception
     * @throws URISyntaxException
     */
    private void startClient(String uri) throws Exception, URISyntaxException {
        clientTransport = TransportFactory.connect(new URI(uri));
        clientTransport.setTransportListener(new TransportListener() {
            public void onCommand(Object command) {
                if (command instanceof WireFormatInfo) {
                    clientWF.set((WireFormatInfo)command);
                    negociationCounter.countDown();
                }
            }

            public void onException(IOException error) {
                if (!ignoreAsycError.get()) {
                    LOG.info("Client transport error: ", error);
                    asyncError.set(error);
                    negociationCounter.countDown();
                }
            }

            public void transportInterupted() {
            }

            public void transportResumed() {
            }
        });
        clientTransport.start();
    }

    /**
     * @throws IOException
     * @throws URISyntaxException
     * @throws Exception
     */
    private void startServer(String uri) throws IOException, URISyntaxException, Exception {
        server = TransportFactory.bind(new URI(uri));
        server.setAcceptListener(new TransportAcceptListener() {
            public void onAccept(Transport transport) {
                try {
                    LOG.info("[" + getName() + "] Server Accepted a Connection");
                    serverTransport = transport;
                    serverTransport.setTransportListener(new TransportListener() {
                        public void onCommand(Object command) {
                            if (command instanceof WireFormatInfo) {
                                serverWF.set((WireFormatInfo)command);
                                negociationCounter.countDown();
                            }
                        }

                        public void onException(IOException error) {
                            if (!ignoreAsycError.get()) {
                                LOG.info("Server transport error: ", error);
                                asyncError.set(error);
                                negociationCounter.countDown();
                            }
                        }

                        public void transportInterupted() {
                        }

                        public void transportResumed() {
                        }
                    });
                    serverTransport.start();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }

            public void onAcceptError(Exception error) {
                error.printStackTrace();
            }
        });
        server.start();
    }

    protected void tearDown() throws Exception {
        ignoreAsycError.set(true);
        try {
            if (clientTransport != null) {
                clientTransport.stop();
            }
            if (serverTransport != null) {
                serverTransport.stop();
            }
            if (server != null) {
                server.stop();
            }
        } catch (Throwable e) {
            e.printStackTrace();
        }
        super.tearDown();
    }

    /**
     * @throws Exception
     */
    public void testWireFormatInfoSeverVersion1() throws Exception {

        startServer("tcp://localhost:61616?wireFormat.version=1");
        startClient("tcp://localhost:61616");

        assertTrue("Connect timeout", negociationCounter.await(10, TimeUnit.SECONDS));
        assertNull("Async error: " + asyncError, asyncError.get());

        assertNotNull(clientWF.get());
        assertEquals(1, clientWF.get().getVersion());

        assertNotNull(serverWF.get());
        assertEquals(1, serverWF.get().getVersion());
    }

    /**
     * @throws Exception
     */
    public void testWireFormatInfoClientVersion1() throws Exception {

        startServer("tcp://localhost:61616");
        startClient("tcp://localhost:61616?wireFormat.version=1");

        assertTrue("Connect timeout", negociationCounter.await(10, TimeUnit.SECONDS));
        assertNull("Async error: " + asyncError, asyncError.get());

        assertNotNull(clientWF.get());
        assertEquals(1, clientWF.get().getVersion());

        assertNotNull(serverWF.get());
        assertEquals(1, serverWF.get().getVersion());
    }

    /**
     * @throws Exception
     */
    public void testWireFormatInfoCurrentVersion() throws Exception {

        startServer("tcp://localhost:61616");
        startClient("tcp://localhost:61616");

        assertTrue("Connect timeout", negociationCounter.await(10, TimeUnit.SECONDS));
        assertNull("Async error: " + asyncError, asyncError.get());

        assertNotNull(clientWF.get());
        assertEquals(CommandTypes.PROTOCOL_VERSION, clientWF.get().getVersion());

        assertNotNull(serverWF.get());
        assertEquals(CommandTypes.PROTOCOL_VERSION, serverWF.get().getVersion());
    }
    
    public void testWireFormatInactivityDurationInitialDelay() throws Exception {

        startServer("tcp://localhost:61616");
        startClient("tcp://localhost:61616?wireFormat.maxInactivityDurationInitalDelay=60000");

        assertTrue("Connect timeout", negociationCounter.await(10, TimeUnit.SECONDS));
        assertNull("Async error: " + asyncError, asyncError.get());

        assertNotNull(clientWF.get());
        assertEquals(CommandTypes.PROTOCOL_VERSION, clientWF.get().getVersion());

        assertNotNull(serverWF.get());
        assertEquals(CommandTypes.PROTOCOL_VERSION, serverWF.get().getVersion());
    }

    public void testWireFormatMaxFrameSize() throws Exception {

        startServer("tcp://localhost:61616");
        startClient("tcp://localhost:61616?wireFormat.maxFrameSize=1048576");

        assertTrue("Connect timeout", negociationCounter.await(10, TimeUnit.SECONDS));
        assertNull("Async error: " + asyncError, asyncError.get());

        assertNotNull(clientWF.get());
        assertEquals(1048576, clientWF.get().getMaxFrameSize());

        assertNotNull(serverWF.get());
        assertEquals(1048576, serverWF.get().getMaxFrameSize());
    }


    /**
     * The property maxFrameSizeEnabled should NOT be negotiated as we don't want to allow
     * clients to override a server setting. Verify both sides can change their setting and it won't
     * affect the other side.
     *
     * @throws Exception
     */
    public void testWireFormatMaxFrameSizeBothEnabled() throws Exception {

        startServer("tcp://localhost:61616");
        startClient("tcp://localhost:61616");

        assertTrue("Connect timeout", negociationCounter.await(10, TimeUnit.SECONDS));
        assertNull("Async error: " + asyncError, asyncError.get());

        //both should be enabled
        assertNotNull(clientWF.get());
        assertTrue(Boolean.valueOf(clientWF.get().getProperties().get("MaxFrameSizeEnabled").toString()));

        assertNotNull(serverWF.get());
        assertTrue(Boolean.valueOf(serverWF.get().getProperties().get("MaxFrameSizeEnabled").toString()));
    }

    //Make sure settin the flag to true explicitly and not default works
    public void testWireFormatMaxFrameSizeBothEnabledExplicit() throws Exception {

        startServer("tcp://localhost:61616?wireFormat.maxFrameSizeEnabled=true");
        startClient("tcp://localhost:61616?wireFormat.maxFrameSizeEnabled=true");

        assertTrue("Connect timeout", negociationCounter.await(10, TimeUnit.SECONDS));
        assertNull("Async error: " + asyncError, asyncError.get());

        //both should be enabled
        assertNotNull(clientWF.get());
        assertTrue(Boolean.valueOf(clientWF.get().getProperties().get("MaxFrameSizeEnabled").toString()));

        assertNotNull(serverWF.get());
        assertTrue(Boolean.valueOf(serverWF.get().getProperties().get("MaxFrameSizeEnabled").toString()));
    }

    //Verify disabling client doesn't change server
    public void testWireFormatMaxFrameSizeClientDisabled() throws Exception {

        startServer("tcp://localhost:61616");
        startClient("tcp://localhost:61616?wireFormat.maxFrameSizeEnabled=false");

        assertTrue("Connect timeout", negociationCounter.await(10, TimeUnit.SECONDS));
        assertNull("Async error: " + asyncError, asyncError.get());

        //Verify client disabled
        assertNotNull(clientWF.get());
        assertTrue(Boolean.valueOf(clientWF.get().getProperties().get("MaxFrameSizeEnabled").toString()));

        //Make sure server is still enabled
        assertNotNull(serverWF.get());
        assertFalse(Boolean.valueOf(serverWF.get().getProperties().get("MaxFrameSizeEnabled").toString()));
    }

    //Verify disabling server doesn't change client
    public void testWireFormatMaxFrameSizeServerDisabled() throws Exception {

        startServer("tcp://localhost:61616?wireFormat.maxFrameSizeEnabled=false");
        startClient("tcp://localhost:61616");

        assertTrue("Connect timeout", negociationCounter.await(10, TimeUnit.SECONDS));
        assertNull("Async error: " + asyncError, asyncError.get());

        //Verify client enabled
        assertNotNull(clientWF.get());
        assertFalse(Boolean.valueOf(clientWF.get().getProperties().get("MaxFrameSizeEnabled").toString()));

        //Make server disabled
        assertNotNull(serverWF.get());
        assertTrue(Boolean.valueOf(serverWF.get().getProperties().get("MaxFrameSizeEnabled").toString()));
    }

    //Verify disabling server and client
    public void testWireFormatMaxFrameSizeBothDisabled() throws Exception {

        startServer("tcp://localhost:61616?wireFormat.maxFrameSizeEnabled=false");
        startClient("tcp://localhost:61616?wireFormat.maxFrameSizeEnabled=false");

        assertTrue("Connect timeout", negociationCounter.await(10, TimeUnit.SECONDS));
        assertNull("Async error: " + asyncError, asyncError.get());

        //Make both disabled
        assertNotNull(clientWF.get());
        assertFalse(Boolean.valueOf(clientWF.get().getProperties().get("MaxFrameSizeEnabled").toString()));

        assertNotNull(serverWF.get());
        assertFalse(Boolean.valueOf(serverWF.get().getProperties().get("MaxFrameSizeEnabled").toString()));
    }
}
