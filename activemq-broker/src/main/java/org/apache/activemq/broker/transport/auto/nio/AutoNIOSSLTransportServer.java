package org.apache.activemq.broker.transport.auto.nio;

import java.io.IOException;
import java.net.Socket;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.ByteBuffer;
import java.util.Set;

import javax.net.ServerSocketFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;

import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.BrokerServiceAware;
import org.apache.activemq.broker.transport.auto.AutoTcpTransportServer;
import org.apache.activemq.transport.Transport;
import org.apache.activemq.transport.nio.AutoInitNioSSLTransport;
import org.apache.activemq.transport.nio.NIOSSLTransport;
import org.apache.activemq.transport.tcp.TcpTransport;
import org.apache.activemq.transport.tcp.TcpTransport.InitBuffer;
import org.apache.activemq.transport.tcp.TcpTransportFactory;
import org.apache.activemq.transport.tcp.TcpTransportServer;
import org.apache.activemq.wireformat.WireFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AutoNIOSSLTransportServer extends AutoTcpTransportServer {

    private static final Logger LOG = LoggerFactory.getLogger(AutoNIOSSLTransportServer.class);

    private SSLContext context;

    public AutoNIOSSLTransportServer(SSLContext context, TcpTransportFactory transportFactory, URI location, ServerSocketFactory serverSocketFactory,
            BrokerService brokerService, Set<String> enabledProtocols) throws IOException, URISyntaxException {
        super(transportFactory, location, serverSocketFactory, brokerService, enabledProtocols);

        this.context = context;
    }

    private boolean needClientAuth;
    private boolean wantClientAuth;

    protected Transport createTransport(Socket socket, WireFormat format, SSLEngine engine,
            InitBuffer initBuffer, ByteBuffer inputBuffer, TcpTransportFactory detectedFactory) throws IOException {
        NIOSSLTransport transport = new NIOSSLTransport(format, socket, engine, initBuffer, inputBuffer);
        if (context != null) {
            transport.setSslContext(context);
        }

        transport.setNeedClientAuth(needClientAuth);
        transport.setWantClientAuth(wantClientAuth);


        return transport;
    }

    @Override
    protected TcpTransport createTransport(Socket socket, WireFormat format) throws IOException {
        throw new UnsupportedOperationException("method not supported");
    }

    @Override
    public boolean isSslServer() {
        return true;
    }

    public boolean isNeedClientAuth() {
        return this.needClientAuth;
    }

    public void setNeedClientAuth(boolean value) {
        this.needClientAuth = value;
    }

    public boolean isWantClientAuth() {
        return this.wantClientAuth;
    }

    public void setWantClientAuth(boolean value) {
        this.wantClientAuth = value;
    }


    @Override
    protected TransportInfo configureTransport(final TcpTransportServer server, final Socket socket) throws Exception {

        //The SSLEngine needs to be initialized and handshake done to get the first command and detect the format
        AutoInitNioSSLTransport in = new AutoInitNioSSLTransport(wireFormatFactory.createWireFormat(), socket);
        if (context != null) {
            in.setSslContext(context);
        }
        in.start();
        SSLEngine engine = in.getSslSession();

        //Wait for handshake to finish initializing
        byte[] read = null;
        do {
            in.serviceRead();
        } while((read = in.read) == null);

        in.stop();

        initBuffer = new InitBuffer(in.readSize, ByteBuffer.allocate(read.length));
        initBuffer.buffer.put(read);

        ProtocolInfo protocolInfo = detectProtocol(read);

        if (protocolInfo.detectedTransportFactory instanceof BrokerServiceAware) {
            ((BrokerServiceAware) protocolInfo.detectedTransportFactory).setBrokerService(brokerService);
        }

        WireFormat format = protocolInfo.detectedWireFormatFactory.createWireFormat();
        Transport transport = createTransport(socket, format, engine, initBuffer, in.getInputBuffer(), protocolInfo.detectedTransportFactory);

        return new TransportInfo(format, transport, protocolInfo.detectedTransportFactory);
    }


}


