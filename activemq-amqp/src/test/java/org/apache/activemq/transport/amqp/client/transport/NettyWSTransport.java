/*
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
package org.apache.activemq.transport.amqp.client.transport;

import java.io.IOException;
import java.net.URI;
import java.security.Principal;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.activemq.transport.amqp.client.util.IOExceptionSupport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPromise;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.FixedRecvByteBufAllocator;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.http.DefaultHttpHeaders;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpClientCodec;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.websocketx.BinaryWebSocketFrame;
import io.netty.handler.codec.http.websocketx.CloseWebSocketFrame;
import io.netty.handler.codec.http.websocketx.PongWebSocketFrame;
import io.netty.handler.codec.http.websocketx.TextWebSocketFrame;
import io.netty.handler.codec.http.websocketx.WebSocketClientHandshaker;
import io.netty.handler.codec.http.websocketx.WebSocketClientHandshakerFactory;
import io.netty.handler.codec.http.websocketx.WebSocketFrame;
import io.netty.handler.codec.http.websocketx.WebSocketVersion;
import io.netty.handler.ssl.SslHandler;
import io.netty.util.CharsetUtil;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GenericFutureListener;

/**
 * Transport for communicating over WebSockets
 */
public class NettyWSTransport implements NettyTransport {

    private static final Logger LOG = LoggerFactory.getLogger(NettyWSTransport.class);

    private static final int QUIET_PERIOD = 20;
    private static final int SHUTDOWN_TIMEOUT = 100;

    protected Bootstrap bootstrap;
    protected EventLoopGroup group;
    protected Channel channel;
    protected NettyTransportListener listener;
    protected NettyTransportOptions options;
    protected final URI remote;
    protected boolean secure;

    private final AtomicBoolean connected = new AtomicBoolean();
    private final AtomicBoolean closed = new AtomicBoolean();
    private ChannelPromise handshakeFuture;
    private IOException failureCause;
    private Throwable pendingFailure;

    /**
     * Create a new transport instance
     *
     * @param remoteLocation
     *        the URI that defines the remote resource to connect to.
     * @param options
     *        the transport options used to configure the socket connection.
     */
    public NettyWSTransport(URI remoteLocation, NettyTransportOptions options) {
        this(null, remoteLocation, options);
    }

    /**
     * Create a new transport instance
     *
     * @param listener
     *        the TransportListener that will receive events from this Transport.
     * @param remoteLocation
     *        the URI that defines the remote resource to connect to.
     * @param options
     *        the transport options used to configure the socket connection.
     */
    public NettyWSTransport(NettyTransportListener listener, URI remoteLocation, NettyTransportOptions options) {
        this.options = options;
        this.listener = listener;
        this.remote = remoteLocation;
        this.secure = remoteLocation.getScheme().equalsIgnoreCase("wss");
    }

    @Override
    public void connect() throws IOException {

        if (listener == null) {
            throw new IllegalStateException("A transport listener must be set before connection attempts.");
        }

        group = new NioEventLoopGroup(1);

        bootstrap = new Bootstrap();
        bootstrap.group(group);
        bootstrap.channel(NioSocketChannel.class);
        bootstrap.handler(new ChannelInitializer<Channel>() {

            @Override
            public void initChannel(Channel connectedChannel) throws Exception {
                configureChannel(connectedChannel);
            }
        });

        configureNetty(bootstrap, getTransportOptions());

        ChannelFuture future;
        try {
            future = bootstrap.connect(getRemoteHost(), getRemotePort());
            future.addListener(new ChannelFutureListener() {

                @Override
                public void operationComplete(ChannelFuture future) throws Exception {
                    if (future.isSuccess()) {
                        handleConnected(future.channel());
                    } else if (future.isCancelled()) {
                        connectionFailed(future.channel(), new IOException("Connection attempt was cancelled"));
                    } else {
                        connectionFailed(future.channel(), IOExceptionSupport.create(future.cause()));
                    }
                }
            });

            future.sync();

            // Now wait for WS protocol level handshake completion
            handshakeFuture.await();
        } catch (InterruptedException ex) {
            LOG.debug("Transport connection attempt was interrupted.");
            Thread.interrupted();
            failureCause = IOExceptionSupport.create(ex);
        }

        if (failureCause != null) {
            // Close out any Netty resources now as they are no longer needed.
            if (channel != null) {
                channel.close().syncUninterruptibly();
                channel = null;
            }
            if (group != null) {
                group.shutdownGracefully(QUIET_PERIOD, SHUTDOWN_TIMEOUT, TimeUnit.MILLISECONDS);
                group = null;
            }

            throw failureCause;
        } else {
            // Connected, allow any held async error to fire now and close the transport.
            channel.eventLoop().execute(new Runnable() {

                @Override
                public void run() {
                    if (pendingFailure != null) {
                        channel.pipeline().fireExceptionCaught(pendingFailure);
                    }
                }
            });
        }
    }

    @Override
    public boolean isConnected() {
        return connected.get();
    }

    @Override
    public boolean isSSL() {
        return secure;
    }

    @Override
    public void close() throws IOException {
        if (closed.compareAndSet(false, true)) {
            connected.set(false);
            if (channel != null) {
                channel.close().syncUninterruptibly();
            }
            if (group != null) {
                group.shutdownGracefully(QUIET_PERIOD, SHUTDOWN_TIMEOUT, TimeUnit.MILLISECONDS);
            }
        }
    }

    @Override
    public ByteBuf allocateSendBuffer(int size) throws IOException {
        checkConnected();
        return channel.alloc().ioBuffer(size, size);
    }

    @Override
    public void send(ByteBuf output) throws IOException {
        checkConnected();
        int length = output.readableBytes();
        if (length == 0) {
            return;
        }

        LOG.trace("Attempted write of: {} bytes", length);

        channel.writeAndFlush(new BinaryWebSocketFrame(output));
    }

    @Override
    public NettyTransportListener getTransportListener() {
        return listener;
    }

    @Override
    public void setTransportListener(NettyTransportListener listener) {
        this.listener = listener;
    }

    @Override
    public NettyTransportOptions getTransportOptions() {
        if (options == null) {
            if (isSSL()) {
                options = NettyTransportSslOptions.INSTANCE;
            } else {
                options = NettyTransportOptions.INSTANCE;
            }
        }

        return options;
    }

    @Override
    public URI getRemoteLocation() {
        return remote;
    }

    @Override
    public Principal getLocalPrincipal() {
        if (!isSSL()) {
            throw new UnsupportedOperationException("Not connected to a secure channel");
        }

        SslHandler sslHandler = channel.pipeline().get(SslHandler.class);

        return sslHandler.engine().getSession().getLocalPrincipal();
    }

    //----- Internal implementation details, can be overridden as needed --//

    protected String getRemoteHost() {
        return remote.getHost();
    }

    protected int getRemotePort() {
        int port = remote.getPort();

        if (port <= 0) {
            if (isSSL()) {
                port = getSslOptions().getDefaultSslPort();
            } else {
                port = getTransportOptions().getDefaultTcpPort();
            }
        }

        return port;
    }

    protected void configureNetty(Bootstrap bootstrap, NettyTransportOptions options) {
        bootstrap.option(ChannelOption.TCP_NODELAY, options.isTcpNoDelay());
        bootstrap.option(ChannelOption.CONNECT_TIMEOUT_MILLIS, options.getConnectTimeout());
        bootstrap.option(ChannelOption.SO_KEEPALIVE, options.isTcpKeepAlive());
        bootstrap.option(ChannelOption.SO_LINGER, options.getSoLinger());
        bootstrap.option(ChannelOption.ALLOCATOR, PartialPooledByteBufAllocator.INSTANCE);

        if (options.getSendBufferSize() != -1) {
            bootstrap.option(ChannelOption.SO_SNDBUF, options.getSendBufferSize());
        }

        if (options.getReceiveBufferSize() != -1) {
            bootstrap.option(ChannelOption.SO_RCVBUF, options.getReceiveBufferSize());
            bootstrap.option(ChannelOption.RCVBUF_ALLOCATOR, new FixedRecvByteBufAllocator(options.getReceiveBufferSize()));
        }

        if (options.getTrafficClass() != -1) {
            bootstrap.option(ChannelOption.IP_TOS, options.getTrafficClass());
        }
    }

    protected void configureChannel(final Channel channel) throws Exception {
        if (isSSL()) {
            SslHandler sslHandler = NettyTransportSupport.createSslHandler(getRemoteLocation(), getSslOptions());
            sslHandler.handshakeFuture().addListener(new GenericFutureListener<Future<Channel>>() {
                @Override
                public void operationComplete(Future<Channel> future) throws Exception {
                    if (future.isSuccess()) {
                        LOG.trace("SSL Handshake has completed: {}", channel);
                        connectionEstablished(channel);
                    } else {
                        LOG.trace("SSL Handshake has failed: {}", channel);
                        connectionFailed(channel, IOExceptionSupport.create(future.cause()));
                    }
                }
            });

            channel.pipeline().addLast(sslHandler);
        }

        channel.pipeline().addLast(new HttpClientCodec());
        channel.pipeline().addLast(new HttpObjectAggregator(8192));
        channel.pipeline().addLast(new NettyTcpTransportHandler());
    }

    protected void handleConnected(final Channel channel) throws Exception {
        if (!isSSL()) {
            connectionEstablished(channel);
        }
    }

    //----- State change handlers and checks ---------------------------------//

    /**
     * Called when the transport has successfully connected and is ready for use.
     */
    protected void connectionEstablished(Channel connectedChannel) {
        LOG.info("WebSocket connectionEstablished! {}", connectedChannel);
        channel = connectedChannel;
        connected.set(true);
    }

    /**
     * Called when the transport connection failed and an error should be returned.
     *
     * @param failedChannel
     *      The Channel instance that failed.
     * @param cause
     *      An IOException that describes the cause of the failed connection.
     */
    protected void connectionFailed(Channel failedChannel, IOException cause) {
        failureCause = IOExceptionSupport.create(cause);
        channel = failedChannel;
        connected.set(false);
        handshakeFuture.setFailure(cause);
    }

    private NettyTransportSslOptions getSslOptions() {
        return (NettyTransportSslOptions) getTransportOptions();
    }

    private void checkConnected() throws IOException {
        if (!connected.get()) {
            throw new IOException("Cannot send to a non-connected transport.");
        }
    }

    //----- Handle connection events -----------------------------------------//

    private class NettyTcpTransportHandler extends SimpleChannelInboundHandler<Object> {

        private final WebSocketClientHandshaker handshaker;

        public NettyTcpTransportHandler() {
            handshaker = WebSocketClientHandshakerFactory.newHandshaker(
                remote, WebSocketVersion.V13, "amqp", false, new DefaultHttpHeaders());
        }

        @Override
        public void handlerAdded(ChannelHandlerContext context) {
            LOG.trace("Handler has become added! Channel is {}", context.channel());
            handshakeFuture = context.newPromise();
        }

        @Override
        public void channelActive(ChannelHandlerContext context) throws Exception {
            LOG.trace("Channel has become active! Channel is {}", context.channel());
            handshaker.handshake(context.channel());
        }

        @Override
        public void channelInactive(ChannelHandlerContext context) throws Exception {
            LOG.trace("Channel has gone inactive! Channel is {}", context.channel());
            if (connected.compareAndSet(true, false) && !closed.get()) {
                LOG.trace("Firing onTransportClosed listener");
                listener.onTransportClosed();
            }
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext context, Throwable cause) throws Exception {
            LOG.trace("Exception on channel! Channel is {} -> {}", context.channel(), cause.getMessage());
            LOG.trace("Error Stack: ", cause);
            if (connected.compareAndSet(true, false) && !closed.get()) {
                LOG.trace("Firing onTransportError listener");
                if (pendingFailure != null) {
                    listener.onTransportError(pendingFailure);
                } else {
                    listener.onTransportError(cause);
                }
            } else {
                // Hold the first failure for later dispatch if connect succeeds.
                // This will then trigger disconnect using the first error reported.
                if (pendingFailure != null) {
                    LOG.trace("Holding error until connect succeeds: {}", cause.getMessage());
                    pendingFailure = cause;
                }

                if (!handshakeFuture.isDone()) {
                    handshakeFuture.setFailure(cause);
                }
            }
        }

        @Override
        protected void channelRead0(ChannelHandlerContext ctx, Object message) throws Exception {
            LOG.trace("New data read: incoming: {}", message);

            Channel ch = ctx.channel();
            if (!handshaker.isHandshakeComplete()) {
                handshaker.finishHandshake(ch, (FullHttpResponse) message);
                LOG.info("WebSocket Client connected! {}", ctx.channel());
                handshakeFuture.setSuccess();
                return;
            }

            // We shouldn't get this since we handle the handshake previously.
            if (message instanceof FullHttpResponse) {
                FullHttpResponse response = (FullHttpResponse) message;
                throw new IllegalStateException(
                    "Unexpected FullHttpResponse (getStatus=" + response.getStatus() +
                    ", content=" + response.content().toString(CharsetUtil.UTF_8) + ')');
            }

            WebSocketFrame frame = (WebSocketFrame) message;
            if (frame instanceof TextWebSocketFrame) {
                TextWebSocketFrame textFrame = (TextWebSocketFrame) frame;
                LOG.warn("WebSocket Client received message: " + textFrame.text());
                ctx.fireExceptionCaught(new IOException("Received invalid frame over WebSocket."));
            } else if (frame instanceof BinaryWebSocketFrame) {
                BinaryWebSocketFrame binaryFrame = (BinaryWebSocketFrame) frame;
                LOG.info("WebSocket Client received data: {} bytes", binaryFrame.content().readableBytes());
                listener.onData(binaryFrame.content());
            } else if (frame instanceof PongWebSocketFrame) {
                LOG.trace("WebSocket Client received pong");
            } else if (frame instanceof CloseWebSocketFrame) {
                LOG.trace("WebSocket Client received closing");
                ch.close();
            }
        }
    }
}
