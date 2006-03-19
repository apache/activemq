/**
 *
 * Copyright 2005-2006 The Apache Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.activemq.transport.stomp;

import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.ProtocolException;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.jms.JMSException;

import org.activeio.ByteArrayOutputStream;
import org.activeio.Packet;
import org.activeio.adapter.PacketInputStream;
import org.activeio.command.WireFormat;
import org.activeio.packet.ByteArrayPacket;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.Command;
import org.apache.activemq.command.CommandTypes;
import org.apache.activemq.command.ConnectionId;
import org.apache.activemq.command.ConnectionInfo;
import org.apache.activemq.command.ConsumerId;
import org.apache.activemq.command.FlushCommand;
import org.apache.activemq.command.LocalTransactionId;
import org.apache.activemq.command.Message;
import org.apache.activemq.command.MessageDispatch;
import org.apache.activemq.command.MessageId;
import org.apache.activemq.command.ProducerId;
import org.apache.activemq.command.Response;
import org.apache.activemq.command.SessionId;
import org.apache.activemq.command.TransactionId;
import org.apache.activemq.filter.DestinationMap;
import org.apache.activemq.util.IOExceptionSupport;
import org.apache.activemq.util.IdGenerator;
import org.apache.activemq.util.LongSequenceGenerator;

import edu.emory.mathcs.backport.java.util.concurrent.BlockingQueue;
import edu.emory.mathcs.backport.java.util.concurrent.ConcurrentHashMap;
import edu.emory.mathcs.backport.java.util.concurrent.CopyOnWriteArrayList;
import edu.emory.mathcs.backport.java.util.concurrent.LinkedBlockingQueue;
import edu.emory.mathcs.backport.java.util.concurrent.TimeUnit;

/**
 * Implements the <a href="http://stomp.codehaus.org/">Stomp</a> protocol.
 */
public class StompWireFormat implements WireFormat {

    private static final IdGenerator connectionIdGenerator = new IdGenerator();
    private static int transactionIdCounter;

    private int version = 1;
    private final CommandParser commandParser = new CommandParser(this);
    private final HeaderParser headerParser = new HeaderParser();

    private final BlockingQueue pendingReadCommands = new LinkedBlockingQueue();
    private final BlockingQueue pendingWriteFrames = new LinkedBlockingQueue();
    private final List receiptListeners = new CopyOnWriteArrayList();
    private final Map subscriptionsByConsumerId = new ConcurrentHashMap();
    private final Map subscriptionsByName = new ConcurrentHashMap();
    private final DestinationMap subscriptionsByDestination = new DestinationMap();
    private final Map transactions = new ConcurrentHashMap();
    private final Map dispachedMap = new ConcurrentHashMap();
    private short lastCommandId;

    private final ConnectionId connectionId = new ConnectionId(connectionIdGenerator.generateId());
    private final SessionId sessionId = new SessionId(connectionId, -1);
    private final ProducerId producerId = new ProducerId(sessionId, 1);
    
    private final LongSequenceGenerator consumerIdGenerator = new LongSequenceGenerator();
    private final LongSequenceGenerator messageIdGenerator = new LongSequenceGenerator();
    
    void addResponseListener(ResponseListener listener) {
        receiptListeners.add(listener);
    }

    boolean connected = false;
    
    public Command readCommand(DataInput in) throws IOException, JMSException {
        Command pending = (Command) AsyncHelper.tryUntilNotInterrupted(new AsyncHelper.HelperWithReturn() {
            public Object cycle() throws InterruptedException {
                return pendingReadCommands.poll(0, TimeUnit.MILLISECONDS);
            }
        });
        if (pending != null) {
            return pending;
        }

        try {
            Command command = commandParser.parse(in);
            if( !connected ) {
                if( command.getDataStructureType() != ConnectionInfo.DATA_STRUCTURE_TYPE )
                    throw new IOException("Not yet connected.");
            }
            return command;
        }
        catch (ProtocolException e) {
            sendError(e.getMessage());
            return FlushCommand.COMMAND;
        }
    }

    public Command writeCommand(final Command packet, final DataOutput out) throws IOException, JMSException {
        flushPendingFrames(out);

        // It may have just been a flush request.
        if (packet == null)
            return null;

        if (packet.getDataStructureType() == CommandTypes.RESPONSE) {
            assert (packet instanceof Response);
            Response receipt = (Response) packet;
            for (int i = 0; i < receiptListeners.size(); i++) {
                ResponseListener listener = (ResponseListener) receiptListeners.get(i);
                if (listener.onResponse(receipt, out)) {
                    receiptListeners.remove(listener);
                    return null;
                }
            }
        }
        if( packet.isMessageDispatch() ) {
            MessageDispatch md = (MessageDispatch)packet;
            Message message = md.getMessage();
            Subscription sub = (Subscription) subscriptionsByConsumerId.get(md.getConsumerId());
            if (sub != null)
                sub.receive(md, out);
        }
        return null;
    }

    private void flushPendingFrames(final DataOutput out) throws IOException {
        boolean interrupted = false;
        do {
            try {
                byte[] frame = (byte[]) pendingWriteFrames.poll(0, TimeUnit.MILLISECONDS);
                if (frame == null)
                    return;
                out.write(frame);
            }
            catch (InterruptedException e) {
                interrupted = true;
            }
        }
        while (interrupted);
    }

    private void sendError(final String message) {
        // System.err.println("sending error [" + message + "]");
        AsyncHelper.tryUntilNotInterrupted(new AsyncHelper.Helper() {
            public void cycle() throws InterruptedException {
                pendingWriteFrames.put(new FrameBuilder(Stomp.Responses.ERROR).addHeader(Stomp.Headers.Error.MESSAGE, message).toFrame());
            }
        });
    }
    
    public void onFullyConnected() {
        connected=true;
    }
    
    public void addToPendingReadCommands(final Command info) {
        AsyncHelper.tryUntilNotInterrupted(new AsyncHelper.Helper() {
            public void cycle() throws InterruptedException {
                pendingReadCommands.put(info);
            }
        });
    }


    void clearTransactionId(String user_tx_id) {
        this.transactions.remove(user_tx_id);
    }

    public SessionId getSessionId() {
        return sessionId;
    }

    public ProducerId getProducerId() {
        return producerId;
    }

    
    public Subscription getSubcription(ConsumerId consumerId) {
        return (Subscription) subscriptionsByConsumerId.get(consumerId);
    }
    public Set getSubcriptions(ActiveMQDestination destination) {
        return subscriptionsByDestination.get(destination);
    }
    public Subscription getSubcription(String name) {
        return (Subscription) subscriptionsByName.get(name);
    }
    
    public void addSubscription(Subscription s) {
        if (s.getSubscriptionId()!=null && subscriptionsByName.containsKey(s.getSubscriptionId())) {
            Subscription old = (Subscription) subscriptionsByName.get(s.getSubscriptionId());
            removeSubscription(old);
            enqueueCommand(old.close());
        }
        if( s.getSubscriptionId()!=null )
            subscriptionsByName.put(s.getSubscriptionId(), s);
        subscriptionsByConsumerId.put(s.getConsumerInfo().getConsumerId(), s);
        subscriptionsByDestination.put(s.getConsumerInfo().getDestination(), s);
    }
    
    public void removeSubscription(Subscription s) {
        if( s.getSubscriptionId()!=null )
            subscriptionsByName.remove(s.getSubscriptionId());
        subscriptionsByConsumerId.remove(s.getConsumerInfo().getConsumerId());
        subscriptionsByDestination.remove(s.getConsumerInfo().getDestination(), s);
    }

    public void enqueueCommand(final Command ack) {
        AsyncHelper.tryUntilNotInterrupted(new AsyncHelper.Helper() {
            public void cycle() throws InterruptedException {
                pendingReadCommands.put(ack);
            }
        });
    }

    public TransactionId getTransactionId(String key) {
        return (TransactionId) transactions.get(key);
    }

    public TransactionId registerTransactionId(String user_tx_id, int tx_id) {
        LocalTransactionId transactionId = new LocalTransactionId(getConnectionId(), tx_id);
        transactions.put(user_tx_id, transactionId);
        return transactionId;
    }

    public int getVersion() {
        return version;
    }

    public void setVersion(int version) {
        this.version = version;
    }

    public ConnectionId getConnectionId() {
        return connectionId;
    }

    public static synchronized int generateTransactionId() {
        return ++transactionIdCounter;
    }

    public ConsumerId createConsumerId() {
        return new ConsumerId(sessionId, consumerIdGenerator.getNextSequenceId());
    }
    
    public MessageId createMessageId() {
        return new MessageId(producerId, messageIdGenerator.getNextSequenceId());
    }
    
    synchronized public short generateCommandId() {
        return lastCommandId++;
    }

    public SessionId generateSessionId() {
        throw new RuntimeException("TODO!!");
    }

    public Packet marshal(Object command) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(baos);
        marshal(command, dos);
        dos.close();
        return new ByteArrayPacket(baos.toByteSequence());
    }

    public Object unmarshal(Packet packet) throws IOException {
        PacketInputStream stream = new PacketInputStream(packet);
        DataInputStream dis = new DataInputStream(stream);
        return unmarshal(dis);
    }

    public void marshal(Object command, DataOutputStream os) throws IOException {
        try {
            writeCommand((Command) command, os);
        } catch (IOException e) {
            throw e;
        } catch (JMSException e) {
            throw IOExceptionSupport.create(e);
        }
    }

    public Object unmarshal(DataInputStream is) throws IOException {
        try {
            return readCommand(is);
        } catch (IOException e) {
            throw e;
        } catch (JMSException e) {
            throw IOExceptionSupport.create(e);
        }
    }

    public Map getDispachedMap() {
        return dispachedMap;
    }

}
