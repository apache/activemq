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
package org.apache.activemq.transport.auto;

import java.io.IOException;
import java.io.InputStream;
import java.net.Socket;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import javax.net.ServerSocketFactory;

import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.BrokerServiceAware;
import org.apache.activemq.openwire.OpenWireFormatFactory;
import org.apache.activemq.transport.InactivityIOException;
import org.apache.activemq.transport.Transport;
import org.apache.activemq.transport.TransportFactory;
import org.apache.activemq.transport.TransportServer;
import org.apache.activemq.transport.protocol.AmqpProtocolVerifier;
import org.apache.activemq.transport.protocol.MqttProtocolVerifier;
import org.apache.activemq.transport.protocol.OpenWireProtocolVerifier;
import org.apache.activemq.transport.protocol.ProtocolVerifier;
import org.apache.activemq.transport.protocol.StompProtocolVerifier;
import org.apache.activemq.transport.tcp.TcpTransport;
import org.apache.activemq.transport.tcp.TcpTransport.InitBuffer;
import org.apache.activemq.transport.tcp.TcpTransportFactory;
import org.apache.activemq.transport.tcp.TcpTransportServer;
import org.apache.activemq.util.FactoryFinder;
import org.apache.activemq.util.IOExceptionSupport;
import org.apache.activemq.util.IntrospectionSupport;
import org.apache.activemq.util.ServiceStopper;
import org.apache.activemq.wireformat.WireFormat;
import org.apache.activemq.wireformat.WireFormatFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A TCP based implementation of {@link TransportServer}
 */
public class AutoTcpTransportServer extends TcpTransportServer {

    private static final Logger LOG = LoggerFactory.getLogger(AutoTcpTransportServer.class);

    protected Map<String, Map<String, Object>> wireFormatOptions;
    protected Map<String, Object> autoTransportOptions;
    protected Set<String> enabledProtocols;
    protected final Map<String, ProtocolVerifier> protocolVerifiers = new ConcurrentHashMap<String, ProtocolVerifier>();

    protected BrokerService brokerService;

    protected int maxConnectionThreadPoolSize = Integer.MAX_VALUE;
    protected int protocolDetectionTimeOut = 30000;

    private static final FactoryFinder TRANSPORT_FACTORY_FINDER = new FactoryFinder("META-INF/services/org/apache/activemq/transport/");
    private final ConcurrentMap<String, TransportFactory> transportFactories = new ConcurrentHashMap<String, TransportFactory>();

    private static final FactoryFinder WIREFORMAT_FACTORY_FINDER = new FactoryFinder("META-INF/services/org/apache/activemq/wireformat/");

    public WireFormatFactory findWireFormatFactory(String scheme, Map<String, Map<String, Object>> options) throws IOException {
        WireFormatFactory wff = null;
        try {
            wff = (WireFormatFactory)WIREFORMAT_FACTORY_FINDER.newInstance(scheme);
            if (options != null) {
                IntrospectionSupport.setProperties(wff, options.get(AutoTransportUtils.ALL));
                IntrospectionSupport.setProperties(wff, options.get(scheme));
            }
            if (wff instanceof OpenWireFormatFactory) {
                protocolVerifiers.put(AutoTransportUtils.OPENWIRE, new OpenWireProtocolVerifier((OpenWireFormatFactory) wff));
            }
            return wff;
        } catch (Throwable e) {
           throw IOExceptionSupport.create("Could not create wire format factory for: " + scheme + ", reason: " + e, e);
        }
    }

    public TransportFactory findTransportFactory(String scheme, Map<String, ?> options) throws IOException {
        scheme = append(scheme, "nio");
        scheme = append(scheme, "ssl");

        if (scheme.isEmpty()) {
            scheme = "tcp";
        }

        TransportFactory tf = transportFactories.get(scheme);
        if (tf == null) {
            // Try to load if from a META-INF property.
            try {
                tf = (TransportFactory)TRANSPORT_FACTORY_FINDER.newInstance(scheme);
                if (options != null) {
                    IntrospectionSupport.setProperties(tf, options);
                }
                transportFactories.put(scheme, tf);
            } catch (Throwable e) {
                throw IOExceptionSupport.create("Transport scheme NOT recognized: [" + scheme + "]", e);
            }
        }
        return tf;
    }

    protected String append(String currentScheme, String scheme) {
        if (this.getBindLocation().getScheme().contains(scheme)) {
            if (!currentScheme.isEmpty()) {
                currentScheme += "+";
            }
            currentScheme += scheme;
        }
        return currentScheme;
    }

    /**
     * @param transportFactory
     * @param location
     * @param serverSocketFactory
     * @throws IOException
     * @throws URISyntaxException
     */
    public AutoTcpTransportServer(TcpTransportFactory transportFactory,
            URI location, ServerSocketFactory serverSocketFactory, BrokerService brokerService,
            Set<String> enabledProtocols)
            throws IOException, URISyntaxException {
        super(transportFactory, location, serverSocketFactory);

        //Use an executor service here to handle new connections.  Setting the max number
        //of threads to the maximum number of connections the thread count isn't unbounded
        service = new ThreadPoolExecutor(maxConnectionThreadPoolSize,
                maxConnectionThreadPoolSize,
                30L, TimeUnit.SECONDS,
                new LinkedBlockingQueue<Runnable>());
        //allow the thread pool to shrink if the max number of threads isn't needed
        service.allowCoreThreadTimeOut(true);

        this.brokerService = brokerService;
        this.enabledProtocols = enabledProtocols;
        initProtocolVerifiers();
    }

    public int getMaxConnectionThreadPoolSize() {
        return maxConnectionThreadPoolSize;
    }

    public void setMaxConnectionThreadPoolSize(int maxConnectionThreadPoolSize) {
        this.maxConnectionThreadPoolSize = maxConnectionThreadPoolSize;
        service.setCorePoolSize(maxConnectionThreadPoolSize);
        service.setMaximumPoolSize(maxConnectionThreadPoolSize);
    }

    public void setProtocolDetectionTimeOut(int protocolDetectionTimeOut) {
        this.protocolDetectionTimeOut = protocolDetectionTimeOut;
    }

    @Override
    public void setWireFormatFactory(WireFormatFactory factory) {
        super.setWireFormatFactory(factory);
        initOpenWireProtocolVerifier();
    }

    protected void initProtocolVerifiers() {
        initOpenWireProtocolVerifier();

        if (isAllProtocols() || enabledProtocols.contains(AutoTransportUtils.AMQP)) {
            protocolVerifiers.put(AutoTransportUtils.AMQP, new AmqpProtocolVerifier());
        }
        if (isAllProtocols() || enabledProtocols.contains(AutoTransportUtils.STOMP)) {
            protocolVerifiers.put(AutoTransportUtils.STOMP, new StompProtocolVerifier());
        }
        if (isAllProtocols()|| enabledProtocols.contains(AutoTransportUtils.MQTT)) {
            protocolVerifiers.put(AutoTransportUtils.MQTT, new MqttProtocolVerifier());
        }
    }

    protected void initOpenWireProtocolVerifier() {
        if (isAllProtocols() || enabledProtocols.contains(AutoTransportUtils.OPENWIRE)) {
            OpenWireProtocolVerifier owpv;
            if (wireFormatFactory instanceof OpenWireFormatFactory) {
                owpv = new OpenWireProtocolVerifier((OpenWireFormatFactory) wireFormatFactory);
            } else {
                owpv = new OpenWireProtocolVerifier(new OpenWireFormatFactory());
            }
            protocolVerifiers.put(AutoTransportUtils.OPENWIRE, owpv);
        }
    }

    protected boolean isAllProtocols() {
        return enabledProtocols == null || enabledProtocols.isEmpty();
    }


    protected final ThreadPoolExecutor service;


    /**
     * This holds the initial buffer that has been read to detect the protocol.
     */
    public InitBuffer initBuffer;

    @Override
    protected void handleSocket(final Socket socket) {
        final AutoTcpTransportServer server = this;
        //This needs to be done in a new thread because
        //the socket might be waiting on the client to send bytes
        //doHandleSocket can't complete until the protocol can be detected
        service.submit(new Runnable() {
            @Override
            public void run() {
                server.doHandleSocket(socket);
            }
        });
    }

    @Override
    protected TransportInfo configureTransport(final TcpTransportServer server, final Socket socket) throws Exception {
        final InputStream is = socket.getInputStream();
        ExecutorService executor = Executors.newSingleThreadExecutor();

        final AtomicInteger readBytes = new AtomicInteger(0);
        final ByteBuffer data = ByteBuffer.allocate(8);
        // We need to peak at the first 8 bytes of the buffer to detect the protocol
        Future<?> future = executor.submit(new Runnable() {
            @Override
            public void run() {
                try {
                    do {
                        int read = is.read();
                        if (read == -1) {
                            throw new IOException("Connection failed, stream is closed.");
                        }
                        data.put((byte) read);
                        readBytes.incrementAndGet();
                    } while (readBytes.get() < 8);
                } catch (Exception e) {
                    throw new IllegalStateException(e);
                }
            }
        });

        waitForProtocolDetectionFinish(future, readBytes);
        data.flip();
        ProtocolInfo protocolInfo = detectProtocol(data.array());

        initBuffer = new InitBuffer(readBytes.get(), ByteBuffer.allocate(readBytes.get()));
        initBuffer.buffer.put(data.array());

        if (protocolInfo.detectedTransportFactory instanceof BrokerServiceAware) {
            ((BrokerServiceAware) protocolInfo.detectedTransportFactory).setBrokerService(brokerService);
        }

        WireFormat format = protocolInfo.detectedWireFormatFactory.createWireFormat();
        Transport transport = createTransport(socket, format,protocolInfo.detectedTransportFactory);

        return new TransportInfo(format, transport, protocolInfo.detectedTransportFactory);
    }

    protected void waitForProtocolDetectionFinish(final Future<?> future, final AtomicInteger readBytes) throws Exception {
        try {
            //Wait for protocolDetectionTimeOut if defined
            if (protocolDetectionTimeOut > 0) {
                future.get(protocolDetectionTimeOut, TimeUnit.MILLISECONDS);
            } else {
                future.get();
            }
        } catch (TimeoutException e) {
            throw new InactivityIOException("Client timed out before wire format could be detected. " +
                    " 8 bytes are required to detect the protocol but only: " + readBytes.get() + " byte(s) were sent.");
        }
    }

    @Override
    protected TcpTransport createTransport(Socket socket, WireFormat format) throws IOException {
        return new TcpTransport(format, socket, this.initBuffer);
    }

    /**
     * @param socket
     * @param format
     * @param detectedTransportFactory
     * @return
     */
    protected TcpTransport createTransport(Socket socket, WireFormat format,
            TcpTransportFactory detectedTransportFactory) throws IOException {
        return createTransport(socket, format);
    }

    public void setWireFormatOptions(Map<String, Map<String, Object>> wireFormatOptions) {
        this.wireFormatOptions = wireFormatOptions;
    }

    public void setEnabledProtocols(Set<String> enabledProtocols) {
        this.enabledProtocols = enabledProtocols;
    }

    public void setAutoTransportOptions(Map<String, Object> autoTransportOptions) {
        this.autoTransportOptions = autoTransportOptions;
        if (autoTransportOptions.get("protocols") != null) {
            this.enabledProtocols = AutoTransportUtils.parseProtocols((String) autoTransportOptions.get("protocols"));
        }
    }
    @Override
    protected void doStop(ServiceStopper stopper) throws Exception {
        if (service != null) {
            service.shutdown();
        }
        super.doStop(stopper);
    }

    protected ProtocolInfo detectProtocol(byte[] buffer) throws IOException {
        TcpTransportFactory detectedTransportFactory = transportFactory;
        WireFormatFactory detectedWireFormatFactory = wireFormatFactory;

        boolean found = false;
        for (String scheme : protocolVerifiers.keySet()) {
            if (protocolVerifiers.get(scheme).isProtocol(buffer)) {
                LOG.debug("Detected protocol " + scheme);
                detectedWireFormatFactory = findWireFormatFactory(scheme, wireFormatOptions);

                if (scheme.equals("default")) {
                    scheme = "";
                }

                detectedTransportFactory = (TcpTransportFactory) findTransportFactory(scheme, transportOptions);
                found = true;
                break;
            }
        }

        if (!found) {
            throw new IllegalStateException("Could not detect the wire format");
        }

        return new ProtocolInfo(detectedTransportFactory, detectedWireFormatFactory);

    }

    protected class ProtocolInfo {
        public final TcpTransportFactory detectedTransportFactory;
        public final WireFormatFactory detectedWireFormatFactory;

        public ProtocolInfo(TcpTransportFactory detectedTransportFactory,
                WireFormatFactory detectedWireFormatFactory) {
            super();
            this.detectedTransportFactory = detectedTransportFactory;
            this.detectedWireFormatFactory = detectedWireFormatFactory;
        }
    }

}
