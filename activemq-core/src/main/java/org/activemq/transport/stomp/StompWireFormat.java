/*
 * Copyright (c) 2005 Your Corporation. All Rights Reserved.
 */
package org.activemq.transport.stomp;

import edu.emory.mathcs.backport.java.util.concurrent.BlockingQueue;
import edu.emory.mathcs.backport.java.util.concurrent.ConcurrentHashMap;
import edu.emory.mathcs.backport.java.util.concurrent.CopyOnWriteArrayList;
import edu.emory.mathcs.backport.java.util.concurrent.LinkedBlockingQueue;
import edu.emory.mathcs.backport.java.util.concurrent.TimeUnit;

import org.activeio.Packet;
import org.activeio.command.WireFormat;
import org.activemq.command.ActiveMQDestination;
import org.activemq.command.ActiveMQTextMessage;
import org.activemq.command.CommandTypes;
import org.activemq.command.ConnectionId;
import org.activemq.command.ConnectionInfo;
import org.activemq.command.ConsumerId;
import org.activemq.command.Command;
import org.activemq.command.FlushCommand;
import org.activemq.command.LocalTransactionId;
import org.activemq.command.MessageId;
import org.activemq.command.Response;
import org.activemq.command.SessionId;
import org.activemq.command.SessionInfo;
import org.activemq.command.ActiveMQBytesMessage;
import org.activemq.command.TransactionId;
import org.activemq.util.IdGenerator;

import javax.jms.JMSException;
import java.io.BufferedReader;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.DatagramPacket;
import java.net.ProtocolException;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * Implements the <a href="http://stomp.codehaus.org/">Stomp</a> protocol.
 */
public class StompWireFormat implements WireFormat {

    static final IdGenerator PACKET_IDS = new IdGenerator();
    static final IdGenerator clientIds = new IdGenerator();
    private static int transactionIdCounter;

    private int version = 1;
    private CommandParser commandParser = new CommandParser(this);
    private HeaderParser headerParser = new HeaderParser();

    private DataInputStream in;

    private String clientId;

    private BlockingQueue pendingReadCommands = new LinkedBlockingQueue();
    private BlockingQueue pendingWriteFrames = new LinkedBlockingQueue();
    private List receiptListeners = new CopyOnWriteArrayList();
    private SessionId sessionId;
    private Map subscriptions = new ConcurrentHashMap();
    private List ackListeners = new CopyOnWriteArrayList();
    private final Map transactions = new ConcurrentHashMap();
    private ConnectionId connectionId;

    void addResponseListener(ResponseListener listener) {
        receiptListeners.add(listener);
    }

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
            return commandParser.parse(in);
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

    /**
     * some transports may register their streams (e.g. Tcp)
     * 
     * @param dataOut
     * @param dataIn
     */
    public void registerTransportStreams(DataOutputStream dataOut, DataInputStream dataIn) {
        this.in = dataIn;
    }

    /**
     * Some wire formats require a handshake at start-up
     * 
     * @throws java.io.IOException
     */
    public void initiateServerSideProtocol() throws IOException {
        BufferedReader in = new BufferedReader(new InputStreamReader(this.in));
        String first_line = in.readLine();
        if (!first_line.startsWith(Stomp.Commands.CONNECT)) {
            throw new IOException("First line does not begin with with " + Stomp.Commands.CONNECT);
        }

        Properties headers = headerParser.parse(in);
        // if (!headers.containsKey(TTMP.Headers.Connect.LOGIN))
        // System.err.println("Required header [" + TTMP.Headers.Connect.LOGIN +
        // "] missing");
        // if (!headers.containsKey(TTMP.Headers.Connect.PASSCODE))
        // System.err.println("Required header [" +
        // TTMP.Headers.Connect.PASSCODE + "] missing");

        // allow anyone to login for now

        String login = headers.getProperty(Stomp.Headers.Connect.LOGIN);
        String passcode = headers.getProperty(Stomp.Headers.Connect.PASSCODE);

        // skip to end of the packet
        while (in.read() != 0) {
        }
        final ConnectionInfo info = new ConnectionInfo();
        clientId = clientIds.generateId();
        commandParser.setClientId(clientId);

        info.setClientId(clientId);
        info.setResponseRequired(true);
        // info.setClientVersion(Integer.toString(getCurrentWireFormatVersion()));
        final short commandId = generateCommandId();
        info.setCommandId(commandId);
        info.setUserName(login);
        info.setPassword(passcode);
        // info.setStartTime(System.currentTimeMillis());
        AsyncHelper.tryUntilNotInterrupted(new AsyncHelper.Helper() {
            public void cycle() throws InterruptedException {
                pendingReadCommands.put(info);
            }
        });

        addResponseListener(new ResponseListener() {
            public boolean onResponse(Response receipt, DataOutput out) {
                if (receipt.getCorrelationId() != commandId)
                    return false;
                sessionId = generateSessionId();

                final SessionInfo info = new SessionInfo();
                info.setCommandId(generateCommandId());
                info.setSessionId(sessionId);
                info.setResponseRequired(true);

                AsyncHelper.tryUntilNotInterrupted(new AsyncHelper.Helper() {
                    public void cycle() throws InterruptedException {
                        pendingReadCommands.put(info);
                    }
                });

                addResponseListener(new ResponseListener() {
                    public boolean onResponse(Response receipt, DataOutput out) throws IOException {
                        if (receipt.getCorrelationId() != commandId)
                            return false;
                        StringBuffer buffer = new StringBuffer();
                        buffer.append(Stomp.Responses.CONNECTED).append(Stomp.NEWLINE);
                        buffer.append(Stomp.Headers.Connected.SESSION).append(Stomp.Headers.SEPERATOR).append(clientId).append(Stomp.NEWLINE).append(
                                Stomp.NEWLINE);
                        buffer.append(Stomp.NULL);
                        out.writeBytes(buffer.toString());
                        return true;
                    }
                });

                return true;
            }
        });
    }

    /**
     * Creates a new copy of this wire format so it can be used in another
     * thread/context
     */
    public WireFormat copy() {
        return new StompWireFormat();
    }

    /* Stuff below here is leaky stuff we don't actually need */

    /**
     * Some wire formats require a handshake at start-up
     * 
     * @throws java.io.IOException
     */
    public void initiateClientSideProtocol() throws IOException {
        throw new UnsupportedOperationException("Not yet implemented!");
    }

    /**
     * Can this wireformat process packets of this version
     * 
     * @param version
     *            the version number to test
     * @return true if can accept the version
     */
    public boolean canProcessWireFormatVersion(int version) {
        return version == getCurrentWireFormatVersion();
    }

    /**
     * @return the current version of this wire format
     */
    public int getCurrentWireFormatVersion() {
        return 1;
    }

    /**
     * @return Returns the enableCaching.
     */
    public boolean isCachingEnabled() {
        return false;
    }

    /**
     * @param enableCaching
     *            The enableCaching to set.
     */
    public void setCachingEnabled(boolean enableCaching) {
        // never
    }

    /**
     * some wire formats will implement their own fragementation
     * 
     * @return true unless a wire format supports it's own fragmentation
     */
    public boolean doesSupportMessageFragmentation() {
        return false;
    }

    /**
     * Some wire formats will not be able to understand compressed messages
     * 
     * @return true unless a wire format cannot understand compression
     */
    public boolean doesSupportMessageCompression() {
        return false;
    }

    /**
     * Writes the given package to a new datagram
     * 
     * @param channelID
     *            is the unique channel ID
     * @param packet
     *            is the packet to write
     * @return
     * @throws java.io.IOException
     * @throws javax.jms.JMSException
     */
    public DatagramPacket writeCommand(String channelID, Command packet) throws IOException, JMSException {
        throw new UnsupportedOperationException("Will not be implemented");
    }

    /**
     * Reads the packet from the given byte[]
     * 
     * @param bytes
     * @param offset
     * @param length
     * @return
     * @throws java.io.IOException
     */
    public Command fromBytes(byte[] bytes, int offset, int length) throws IOException {
        throw new UnsupportedOperationException("Will not be implemented");
    }

    /**
     * Reads the packet from the given byte[]
     * 
     * @param bytes
     * @return
     * @throws java.io.IOException
     */
    public Command fromBytes(byte[] bytes) throws IOException {
        throw new UnsupportedOperationException("Will not be implemented");
    }

    /**
     * A helper method which converts a packet into a byte array
     * 
     * @param packet
     * @return a byte array representing the packet using some wire protocol
     * @throws java.io.IOException
     * @throws javax.jms.JMSException
     */
    public byte[] toBytes(Command packet) throws IOException, JMSException {
        throw new UnsupportedOperationException("Will not be implemented");
    }

    /**
     * A helper method for working with sockets where the first byte is read
     * first, then the rest of the message is read. <p/> Its common when dealing
     * with sockets to have different timeout semantics until the first non-zero
     * byte is read of a message, after which time a zero timeout is used.
     * 
     * @param firstByte
     *            the first byte of the packet
     * @param in
     *            the rest of the packet
     * @return
     * @throws java.io.IOException
     */
    public Command readCommand(int firstByte, DataInput in) throws IOException {
        throw new UnsupportedOperationException("Will not be implemented");
    }

    /**
     * Read a packet from a Datagram packet from the given channelID. If the
     * packet is from the same channel ID as it was sent then we have a
     * loop-back so discard the packet
     * 
     * @param channelID
     *            is the unique channel ID
     * @param dpacket
     * @return the packet read from the datagram or null if it should be
     *         discarded
     * @throws java.io.IOException
     */
    public Command readCommand(String channelID, DatagramPacket dpacket) throws IOException {
        throw new UnsupportedOperationException("Will not be implemented");
    }

    void clearTransactionId(String user_tx_id) {
        this.transactions.remove(user_tx_id);
    }

    String getClientId() {
        return this.clientId;
    }

    public SessionId getSessionId() {
        return sessionId;
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

    public void setConnectionId(ConnectionId connectionId) {
        this.connectionId = connectionId;
    }

    public static synchronized int generateTransactionId() {
        return ++transactionIdCounter;
    }

    public ConsumerId createConsumerId() {
        throw new RuntimeException("TODO!!");
    }

    public MessageId generateMessageId() {
        throw new RuntimeException("TODO!!");
    }

    // TODO static???
    public static short generateCommandId() {
        throw new RuntimeException("TODO!!");
    }

    public SessionId generateSessionId() {
        throw new RuntimeException("TODO!!");
    }

    public Packet marshal(Object arg0) throws IOException {
        throw new RuntimeException("TODO!!");
    }

    public Object unmarshal(Packet arg0) throws IOException {
        throw new RuntimeException("TODO!!");
    }

    public void marshal(Object arg0, DataOutputStream arg1) throws IOException {
        throw new RuntimeException("TODO!!");
    }

    public Object unmarshal(DataInputStream arg0) throws IOException {
        throw new RuntimeException("TODO!!");
    }
}
