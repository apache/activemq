/*
 * Copyright (c) 2005 Your Corporation. All Rights Reserved.
 */
package org.activemq.transport.stomp;

import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.ProtocolException;
import java.util.List;
import java.util.Map;

import javax.jms.JMSException;

import org.activeio.ByteArrayOutputStream;
import org.activeio.Packet;
import org.activeio.adapter.PacketInputStream;
import org.activeio.command.WireFormat;
import org.activeio.packet.ByteArrayPacket;
import org.activemq.command.ActiveMQBytesMessage;
import org.activemq.command.ActiveMQDestination;
import org.activemq.command.ActiveMQTextMessage;
import org.activemq.command.Command;
import org.activemq.command.CommandTypes;
import org.activemq.command.ConnectionId;
import org.activemq.command.ConnectionInfo;
import org.activemq.command.ConsumerId;
import org.activemq.command.FlushCommand;
import org.activemq.command.LocalTransactionId;
import org.activemq.command.MessageId;
import org.activemq.command.ProducerId;
import org.activemq.command.Response;
import org.activemq.command.SessionId;
import org.activemq.command.TransactionId;
import org.activemq.util.IOExceptionSupport;
import org.activemq.util.IdGenerator;
import org.activemq.util.LongSequenceGenerator;

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
    private CommandParser commandParser = new CommandParser(this);
    private HeaderParser headerParser = new HeaderParser();

    private BlockingQueue pendingReadCommands = new LinkedBlockingQueue();
    private BlockingQueue pendingWriteFrames = new LinkedBlockingQueue();
    private List receiptListeners = new CopyOnWriteArrayList();
    private Map subscriptions = new ConcurrentHashMap();
    private List ackListeners = new CopyOnWriteArrayList();
    private final Map transactions = new ConcurrentHashMap();
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

        if (packet.getDataStructureType() == CommandTypes.ACTIVEMQ_TEXT_MESSAGE) {
            assert (packet instanceof ActiveMQTextMessage);
            ActiveMQTextMessage msg = (ActiveMQTextMessage) packet;
            Subscription sub = (Subscription) subscriptions.get(msg.getJMSDestination());
            sub.receive(msg, out);
        }
        else if (packet.getDataStructureType() == CommandTypes.ACTIVEMQ_BYTES_MESSAGE) {
            assert (packet instanceof ActiveMQBytesMessage);
            ActiveMQBytesMessage msg = (ActiveMQBytesMessage) packet;
            Subscription sub = (Subscription) subscriptions.get(msg.getJMSDestination());
            sub.receive(msg, out);
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
    
    public void addSubscription(Subscription s) {
        if (subscriptions.containsKey(s.getDestination())) {
            Subscription old = (Subscription) subscriptions.get(s.getDestination());
            Command p = old.close();
            enqueueCommand(p);
            subscriptions.put(s.getDestination(), s);
        }
        else {
            subscriptions.put(s.getDestination(), s);
        }
    }

    public void enqueueCommand(final Command ack) {
        AsyncHelper.tryUntilNotInterrupted(new AsyncHelper.Helper() {
            public void cycle() throws InterruptedException {
                pendingReadCommands.put(ack);
            }
        });
    }

    public Subscription getSubscriptionFor(ActiveMQDestination destination) {
        return (Subscription) subscriptions.get(destination);
    }

    public void addAckListener(AckListener listener) {
        this.ackListeners.add(listener);
    }

    public List getAckListeners() {
        return ackListeners;
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

}
