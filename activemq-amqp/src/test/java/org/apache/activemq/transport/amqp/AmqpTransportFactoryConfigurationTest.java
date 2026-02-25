package org.apache.activemq.transport.amqp;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import java.net.Socket;
import java.net.ServerSocket;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.net.SocketFactory;
import javax.net.ssl.SSLSocket;
import javax.net.ssl.SSLSocketFactory;

import org.apache.activemq.transport.MutexTransport;
import org.apache.activemq.transport.Transport;
import org.apache.activemq.transport.TransportFactory;
import org.apache.activemq.transport.TransportFilter;
import org.apache.activemq.transport.tcp.SslTransport;
import org.apache.activemq.transport.tcp.TcpTransport;
import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(ParallelTest.class)
public class AmqpTransportFactoryConfigurationTest {

    private final List<TcpTransport> createdTransports = new ArrayList<>();

    @After
    public void cleanup() throws Exception {
        for (TcpTransport transport : createdTransports) {
            Socket socket = transport.narrow(Socket.class);
            if (socket != null && !socket.isClosed()) {
                socket.close();
            }
        }
        createdTransports.clear();
    }

    @Test
    public void testServerConfigureStripsMutexTransportForAllAmqpFactories() throws Exception {
        assertServerConfigureStripsMutex(new AmqpTransportFactory());
        assertServerConfigureStripsMutex(new AmqpSslTransportFactory());
        assertServerConfigureStripsMutex(new AmqpNioTransportFactory());
        assertServerConfigureStripsMutex(new AmqpNioSslTransportFactory());
    }

    @Test
    public void testCompositeConfigureAppliesAmqpAndWireFormatPropertiesForAllAmqpFactories() throws Exception {
        assertCompositeConfigureAppliesProperties(new AmqpTransportFactory());
        assertCompositeConfigureAppliesProperties(new AmqpSslTransportFactory());
        assertCompositeConfigureAppliesProperties(new AmqpNioTransportFactory());
        assertCompositeConfigureAppliesProperties(new AmqpNioSslTransportFactory());
    }

    @Test
    public void testAllAmqpFactoriesWireAmqpInactivityMonitor() throws Exception {
        assertAmqpFactoriesWireAmqpInactivityMonitor(new AmqpTransportFactory());
        assertAmqpFactoriesWireAmqpInactivityMonitor(new AmqpSslTransportFactory());
        assertAmqpFactoriesWireAmqpInactivityMonitor(new AmqpNioTransportFactory());
        assertAmqpFactoriesWireAmqpInactivityMonitor(new AmqpNioSslTransportFactory());
    }

    private void assertServerConfigureStripsMutex(TransportFactory factory) throws Exception {
        Transport configured = factory.serverConfigure(
            createTransportForFactory(factory, new AmqpWireFormat()),
            new AmqpWireFormat(),
            new HashMap<String, Object>());

        assertFalse("AMQP serverConfigure should strip the broker-side MutexTransport",
            configured instanceof MutexTransport);
    }

    private void assertCompositeConfigureAppliesProperties(TransportFactory factory) throws Exception {
        Map<String, String> options = new HashMap<>();
        options.put("transformer", "raw");
        options.put("producerCredit", "17");
        options.put("wireFormat.maxFrameSize", "4096");
        options.put("wireFormat.connectAttemptTimeout", "1234");

        Transport configured = factory.compositeConfigure(
            createTransportForFactory(factory, new AmqpWireFormat()),
            new AmqpWireFormat(),
            options);

        AmqpTransportFilter filter = findInChain(configured, AmqpTransportFilter.class);
        assertNotNull("Expected AmqpTransportFilter in configured transport chain", filter);
        assertEquals("raw", filter.getTransformer());
        assertEquals(17, filter.getProducerCredit());
        assertEquals(4096L, filter.getMaxFrameSize());
        assertEquals(1234, filter.getConnectAttemptTimeout());
    }

    private void assertAmqpFactoriesWireAmqpInactivityMonitor(TransportFactory factory) throws Exception {
        Transport configured = factory.compositeConfigure(
            createTransportForFactory(factory, new AmqpWireFormat()),
            new AmqpWireFormat(),
            new HashMap<String, String>());

        AmqpInactivityMonitor monitor = findInChain(configured, AmqpInactivityMonitor.class);
        AmqpTransportFilter filter = findInChain(configured, AmqpTransportFilter.class);

        assertNotNull("Expected AmqpInactivityMonitor in configured transport chain", monitor);
        assertNotNull("Expected AmqpTransportFilter in configured transport chain", filter);
        assertTrue("Filter should report inactivity monitor as enabled", filter.isUseInactivityMonitor());
        assertSame("Factory should wire the same monitor instance into the AMQP filter",
            monitor, filter.getInactivityMonitor());
    }

    private TcpTransport createTcpTransport(AmqpWireFormat wireFormat) throws Exception {
        int dynamicPort = findAvailablePort();
        return new TcpTransport(
            wireFormat,
            SocketFactory.getDefault(),
            new URI("tcp://localhost:" + dynamicPort),
            null);
    }

    private int findAvailablePort() throws Exception {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        }
    }

    private TcpTransport createTransportForFactory(TransportFactory factory, AmqpWireFormat wireFormat) throws Exception {
        TcpTransport transport;
        if (factory instanceof AmqpSslTransportFactory || factory instanceof AmqpNioSslTransportFactory) {
            SSLSocket socket = (SSLSocket) SSLSocketFactory.getDefault().createSocket();
            transport = new SslTransport(wireFormat, socket);
        } else {
            transport = createTcpTransport(wireFormat);
        }

        createdTransports.add(transport);
        return transport;
    }

    private <T> T findInChain(Transport transport, Class<T> type) {
        Transport current = transport;
        while (current != null) {
            T found = current.narrow(type);
            if (found != null) {
                return found;
            }
            if (!(current instanceof TransportFilter)) {
                return null;
            }
            current = ((TransportFilter) current).getNext();
        }
        return null;
    }
}
