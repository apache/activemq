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
package org.apache.activemq.broker.util;

import java.io.DataOutputStream;
import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.UnknownHostException;

import org.apache.activemq.broker.BrokerPluginSupport;
import org.apache.activemq.broker.ConnectionContext;
import org.apache.activemq.broker.ConsumerBrokerExchange;
import org.apache.activemq.broker.ProducerBrokerExchange;
import org.apache.activemq.broker.region.Subscription;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.BrokerId;
import org.apache.activemq.command.ConnectionInfo;
import org.apache.activemq.command.ConsumerInfo;
import org.apache.activemq.command.DataStructure;
import org.apache.activemq.command.DestinationInfo;
import org.apache.activemq.command.JournalTrace;
import org.apache.activemq.command.Message;
import org.apache.activemq.command.MessageAck;
import org.apache.activemq.command.MessageDispatch;
import org.apache.activemq.command.MessageDispatchNotification;
import org.apache.activemq.command.MessagePull;
import org.apache.activemq.command.ProducerInfo;
import org.apache.activemq.command.RemoveSubscriptionInfo;
import org.apache.activemq.command.Response;
import org.apache.activemq.command.SessionInfo;
import org.apache.activemq.command.TransactionId;
import org.apache.activemq.command.TransactionInfo;
import org.apache.activemq.openwire.OpenWireFormatFactory;
import org.apache.activemq.util.ByteArrayOutputStream;
import org.apache.activemq.util.ByteSequence;
import org.apache.activemq.wireformat.WireFormat;
import org.apache.activemq.wireformat.WireFormatFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A Broker interceptor which allows you to trace all operations to a UDP
 * socket.
 * 
 * @org.apache.xbean.XBean element="udpTraceBrokerPlugin"
 * 
 */
public class UDPTraceBrokerPlugin extends BrokerPluginSupport {

    private static final Logger LOG = LoggerFactory.getLogger(UDPTraceBrokerPlugin.class);
    protected WireFormat wireFormat;
    protected WireFormatFactory wireFormatFactory;
    protected int maxTraceDatagramSize = 1024 * 4;
    protected URI destination;
    protected DatagramSocket socket;

    protected BrokerId brokerId;
    protected SocketAddress address;
    protected boolean broadcast;

    public UDPTraceBrokerPlugin() {
        try {
            destination = new URI("udp://127.0.0.1:61616");
        } catch (URISyntaxException wontHappen) {
        }
    }

    public void start() throws Exception {
        super.start();
        if (getWireFormat() == null) {
            throw new IllegalArgumentException("Wireformat must be specifed.");
        }
        if (address == null) {
            address = createSocketAddress(destination);
        }
        socket = createSocket();

        brokerId = super.getBrokerId();
        trace(new JournalTrace("START"));
    }

    protected DatagramSocket createSocket() throws IOException {
        DatagramSocket s = new DatagramSocket();
        s.setSendBufferSize(maxTraceDatagramSize);
        s.setBroadcast(broadcast);
        return s;
    }

    public void stop() throws Exception {
        trace(new JournalTrace("STOP"));
        socket.close();
        super.stop();
    }

    private void trace(DataStructure command) {
        try {

            ByteArrayOutputStream baos = new ByteArrayOutputStream(maxTraceDatagramSize);
            DataOutputStream out = new DataOutputStream(baos);
            wireFormat.marshal(brokerId, out);
            wireFormat.marshal(command, out);
            out.close();
            ByteSequence sequence = baos.toByteSequence();
            DatagramPacket datagram = new DatagramPacket(sequence.getData(), sequence.getOffset(), sequence.getLength(), address);
            socket.send(datagram);

        } catch (Throwable e) {
            LOG.debug("Failed to trace: {}", command, e);
        }
    }

    public void send(ProducerBrokerExchange producerExchange, Message messageSend) throws Exception {
        trace(messageSend);
        super.send(producerExchange, messageSend);
    }

    public void acknowledge(ConsumerBrokerExchange consumerExchange, MessageAck ack) throws Exception {
        trace(ack);
        super.acknowledge(consumerExchange, ack);
    }

    public void addConnection(ConnectionContext context, ConnectionInfo info) throws Exception {
        trace(info);
        super.addConnection(context, info);
    }

    public Subscription addConsumer(ConnectionContext context, ConsumerInfo info) throws Exception {
        trace(info);
        return super.addConsumer(context, info);
    }

    public void addDestinationInfo(ConnectionContext context, DestinationInfo info) throws Exception {
        trace(info);
        super.addDestinationInfo(context, info);
    }

    public void addProducer(ConnectionContext context, ProducerInfo info) throws Exception {
        trace(info);
        super.addProducer(context, info);
    }

    public void addSession(ConnectionContext context, SessionInfo info) throws Exception {
        trace(info);
        super.addSession(context, info);
    }

    public void beginTransaction(ConnectionContext context, TransactionId xid) throws Exception {
        trace(new TransactionInfo(context.getConnectionId(), xid, TransactionInfo.BEGIN));
        super.beginTransaction(context, xid);
    }

    public void commitTransaction(ConnectionContext context, TransactionId xid, boolean onePhase) throws Exception {
        trace(new TransactionInfo(context.getConnectionId(), xid, onePhase ? TransactionInfo.COMMIT_ONE_PHASE : TransactionInfo.COMMIT_TWO_PHASE));
        super.commitTransaction(context, xid, onePhase);
    }

    public void forgetTransaction(ConnectionContext context, TransactionId xid) throws Exception {
        trace(new TransactionInfo(context.getConnectionId(), xid, TransactionInfo.FORGET));
        super.forgetTransaction(context, xid);
    }

    public Response messagePull(ConnectionContext context, MessagePull pull) throws Exception {
        trace(pull);
        return super.messagePull(context, pull);
    }

    public int prepareTransaction(ConnectionContext context, TransactionId xid) throws Exception {
        trace(new TransactionInfo(context.getConnectionId(), xid, TransactionInfo.PREPARE));
        return super.prepareTransaction(context, xid);
    }

    public void postProcessDispatch(MessageDispatch messageDispatch) {
        trace(messageDispatch);
        super.postProcessDispatch(messageDispatch);
    }

    public void processDispatchNotification(MessageDispatchNotification messageDispatchNotification) throws Exception {
        trace(messageDispatchNotification);
        super.processDispatchNotification(messageDispatchNotification);
    }

    public void removeConnection(ConnectionContext context, ConnectionInfo info, Throwable error) throws Exception {
        trace(info.createRemoveCommand());
        super.removeConnection(context, info, error);
    }

    public void removeConsumer(ConnectionContext context, ConsumerInfo info) throws Exception {
        trace(info.createRemoveCommand());
        super.removeConsumer(context, info);
    }

    public void removeDestination(ConnectionContext context, ActiveMQDestination destination, long timeout) throws Exception {
        super.removeDestination(context, destination, timeout);
    }

    public void removeDestinationInfo(ConnectionContext context, DestinationInfo info) throws Exception {
        trace(info);
        super.removeDestinationInfo(context, info);
    }

    public void removeProducer(ConnectionContext context, ProducerInfo info) throws Exception {
        trace(info.createRemoveCommand());
        super.removeProducer(context, info);
    }

    public void removeSession(ConnectionContext context, SessionInfo info) throws Exception {
        trace(info.createRemoveCommand());
        super.removeSession(context, info);
    }

    public void removeSubscription(ConnectionContext context, RemoveSubscriptionInfo info) throws Exception {
        trace(info);
        super.removeSubscription(context, info);
    }

    public void rollbackTransaction(ConnectionContext context, TransactionId xid) throws Exception {
        trace(new TransactionInfo(context.getConnectionId(), xid, TransactionInfo.ROLLBACK));
        super.rollbackTransaction(context, xid);
    }

    public WireFormat getWireFormat() {
        if (wireFormat == null) {
            wireFormat = createWireFormat();
        }
        return wireFormat;
    }

    protected WireFormat createWireFormat() {
        return getWireFormatFactory().createWireFormat();
    }

    public void setWireFormat(WireFormat wireFormat) {
        this.wireFormat = wireFormat;
    }

    public WireFormatFactory getWireFormatFactory() {
        if (wireFormatFactory == null) {
            wireFormatFactory = createWireFormatFactory();
        }
        return wireFormatFactory;
    }

    protected OpenWireFormatFactory createWireFormatFactory() {
        OpenWireFormatFactory wf = new OpenWireFormatFactory();
        wf.setCacheEnabled(false);
        wf.setVersion(1);
        wf.setTightEncodingEnabled(true);
        wf.setSizePrefixDisabled(true);
        return wf;
    }

    public void setWireFormatFactory(WireFormatFactory wireFormatFactory) {
        this.wireFormatFactory = wireFormatFactory;
    }

    protected SocketAddress createSocketAddress(URI location) throws UnknownHostException {
        InetAddress a = InetAddress.getByName(location.getHost());
        int port = location.getPort();
        return new InetSocketAddress(a, port);
    }

    public URI getDestination() {
        return destination;
    }

    public void setDestination(URI destination) {
        this.destination = destination;
    }

    public int getMaxTraceDatagramSize() {
        return maxTraceDatagramSize;
    }

    public void setMaxTraceDatagramSize(int maxTraceDatagramSize) {
        this.maxTraceDatagramSize = maxTraceDatagramSize;
    }

    public boolean isBroadcast() {
        return broadcast;
    }

    public void setBroadcast(boolean broadcast) {
        this.broadcast = broadcast;
    }

    public SocketAddress getAddress() {
        return address;
    }

    public void setAddress(SocketAddress address) {
        this.address = address;
    }

}
