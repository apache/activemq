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
package org.apache.activemq.transport.stomp;

import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.jms.JMSException;

import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.ActiveMQMessage;
import org.apache.activemq.command.ActiveMQTempQueue;
import org.apache.activemq.command.ActiveMQTempTopic;
import org.apache.activemq.command.Command;
import org.apache.activemq.command.ConnectionError;
import org.apache.activemq.command.ConnectionId;
import org.apache.activemq.command.ConnectionInfo;
import org.apache.activemq.command.ConsumerId;
import org.apache.activemq.command.ConsumerInfo;
import org.apache.activemq.command.DestinationInfo;
import org.apache.activemq.command.ExceptionResponse;
import org.apache.activemq.command.LocalTransactionId;
import org.apache.activemq.command.MessageAck;
import org.apache.activemq.command.MessageDispatch;
import org.apache.activemq.command.MessageId;
import org.apache.activemq.command.ProducerId;
import org.apache.activemq.command.ProducerInfo;
import org.apache.activemq.command.RemoveSubscriptionInfo;
import org.apache.activemq.command.Response;
import org.apache.activemq.command.SessionId;
import org.apache.activemq.command.SessionInfo;
import org.apache.activemq.command.ShutdownInfo;
import org.apache.activemq.command.TransactionId;
import org.apache.activemq.command.TransactionInfo;
import org.apache.activemq.util.ByteArrayOutputStream;
import org.apache.activemq.util.FactoryFinder;
import org.apache.activemq.util.IOExceptionSupport;
import org.apache.activemq.util.IdGenerator;
import org.apache.activemq.util.IntrospectionSupport;
import org.apache.activemq.util.LongSequenceGenerator;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;

/**
 * @author <a href="http://hiramchirino.com">chirino</a>
 */
public class ProtocolConverter {

    private static final IdGenerator CONNECTION_ID_GENERATOR = new IdGenerator();

    private final ConnectionId connectionId = new ConnectionId(CONNECTION_ID_GENERATOR.generateId());
    private final SessionId sessionId = new SessionId(connectionId, -1);
    private final ProducerId producerId = new ProducerId(sessionId, 1);

    private final LongSequenceGenerator consumerIdGenerator = new LongSequenceGenerator();
    private final LongSequenceGenerator messageIdGenerator = new LongSequenceGenerator();
    private final LongSequenceGenerator transactionIdGenerator = new LongSequenceGenerator();
    private final LongSequenceGenerator tempDestinationGenerator = new LongSequenceGenerator();

    private final ConcurrentHashMap<Integer, ResponseHandler> resposeHandlers = new ConcurrentHashMap<Integer, ResponseHandler>();
    private final ConcurrentHashMap<ConsumerId, StompSubscription> subscriptionsByConsumerId = new ConcurrentHashMap<ConsumerId, StompSubscription>();
    private final ConcurrentHashMap<String, ActiveMQDestination> tempDestinations = new ConcurrentHashMap<String, ActiveMQDestination>();
    private final ConcurrentHashMap<String, String> tempDestinationAmqToStompMap = new ConcurrentHashMap<String, String>();
    private final Map<String, LocalTransactionId> transactions = new ConcurrentHashMap<String, LocalTransactionId>();
    private final StompTransport stompTransport;

    private final Object commnadIdMutex = new Object();
    private int lastCommandId;
    private final AtomicBoolean connected = new AtomicBoolean(false);
    private final FrameTranslator frameTranslator;
    private final FactoryFinder FRAME_TRANSLATOR_FINDER = new FactoryFinder("META-INF/services/org/apache/activemq/transport/frametranslator/");
    private final ApplicationContext applicationContext;

    public ProtocolConverter(StompTransport stompTransport, FrameTranslator translator, ApplicationContext applicationContext) {
        this.stompTransport = stompTransport;
        this.frameTranslator = translator;
        this.applicationContext = applicationContext;
    }

    protected int generateCommandId() {
        synchronized (commnadIdMutex) {
            return lastCommandId++;
        }
    }

    protected ResponseHandler createResponseHandler(final StompFrame command) {
        final String receiptId = command.getHeaders().get(Stomp.Headers.RECEIPT_REQUESTED);
        if (receiptId != null) {
            return new ResponseHandler() {
                public void onResponse(ProtocolConverter converter, Response response) throws IOException {
                    if (response.isException()) {
                        // Generally a command can fail.. but that does not invalidate the connection.
                        // We report back the failure but we don't close the connection.
                        Throwable exception = ((ExceptionResponse)response).getException();
                        handleException(exception, command);
                    } else {
                        StompFrame sc = new StompFrame();
                        sc.setAction(Stomp.Responses.RECEIPT);
                        sc.setHeaders(new HashMap<String, String>(1));
                        sc.getHeaders().put(Stomp.Headers.Response.RECEIPT_ID, receiptId);
                        stompTransport.sendToStomp(sc);
                    }
                }
            };
        }
        return null;
    }

    protected void sendToActiveMQ(Command command, ResponseHandler handler) {
        command.setCommandId(generateCommandId());
        if (handler != null) {
            command.setResponseRequired(true);
            resposeHandlers.put(Integer.valueOf(command.getCommandId()), handler);
        }
        stompTransport.sendToActiveMQ(command);
    }

    protected void sendToStomp(StompFrame command) throws IOException {
        stompTransport.sendToStomp(command);
    }

    protected FrameTranslator findTranslator(String header) {
        FrameTranslator translator = frameTranslator;
        try {
            if (header != null) {
                translator = (FrameTranslator) FRAME_TRANSLATOR_FINDER
                        .newInstance(header);
                if (translator instanceof ApplicationContextAware) {
                    ((ApplicationContextAware)translator).setApplicationContext(applicationContext);
                }
            }
        } catch (Exception ignore) {
            // if anything goes wrong use the default translator
        }

        return translator;
    }

    /**
     * Convert a stomp command
     *
     * @param command
     */
    public void onStompCommand(StompFrame command) throws IOException, JMSException {
        try {

            if (command.getClass() == StompFrameError.class) {
                throw ((StompFrameError)command).getException();
            }

            String action = command.getAction();
            if (action.startsWith(Stomp.Commands.SEND)) {
                onStompSend(command);
            } else if (action.startsWith(Stomp.Commands.ACK)) {
                onStompAck(command);
            } else if (action.startsWith(Stomp.Commands.BEGIN)) {
                onStompBegin(command);
            } else if (action.startsWith(Stomp.Commands.COMMIT)) {
                onStompCommit(command);
            } else if (action.startsWith(Stomp.Commands.ABORT)) {
                onStompAbort(command);
            } else if (action.startsWith(Stomp.Commands.SUBSCRIBE)) {
                onStompSubscribe(command);
            } else if (action.startsWith(Stomp.Commands.UNSUBSCRIBE)) {
                onStompUnsubscribe(command);
            } else if (action.startsWith(Stomp.Commands.CONNECT)) {
                onStompConnect(command);
            } else if (action.startsWith(Stomp.Commands.DISCONNECT)) {
                onStompDisconnect(command);
            } else {
                throw new ProtocolException("Unknown STOMP action: " + action);
            }

        } catch (ProtocolException e) {
            handleException(e, command);
            // Some protocol errors can cause the connection to get closed.
            if( e.isFatal() ) {
               getStompTransport().onException(e);
            }
        }
    }

    protected void handleException(Throwable exception, StompFrame command) throws IOException {
        // Let the stomp client know about any protocol errors.
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        PrintWriter stream = new PrintWriter(new OutputStreamWriter(baos, "UTF-8"));
        exception.printStackTrace(stream);
        stream.close();

        HashMap<String, String> headers = new HashMap<String, String>();
        headers.put(Stomp.Headers.Error.MESSAGE, exception.getMessage());

        if (command != null) {
            final String receiptId = command.getHeaders().get(Stomp.Headers.RECEIPT_REQUESTED);
            if (receiptId != null) {
                headers.put(Stomp.Headers.Response.RECEIPT_ID, receiptId);
            }
        }

        StompFrame errorMessage = new StompFrame(Stomp.Responses.ERROR, headers, baos.toByteArray());
        sendToStomp(errorMessage);
    }

    protected void onStompSend(StompFrame command) throws IOException, JMSException {
        checkConnected();

        Map<String, String> headers = command.getHeaders();
        String stompTx = headers.get(Stomp.Headers.TRANSACTION);
        headers.remove("transaction");

        ActiveMQMessage message = convertMessage(command);

        message.setProducerId(producerId);
        MessageId id = new MessageId(producerId, messageIdGenerator.getNextSequenceId());
        message.setMessageId(id);
        message.setJMSTimestamp(System.currentTimeMillis());

        if (stompTx != null) {
            TransactionId activemqTx = transactions.get(stompTx);
            if (activemqTx == null) {
                throw new ProtocolException("Invalid transaction id: " + stompTx);
            }
            message.setTransactionId(activemqTx);
        }

        message.onSend();
        sendToActiveMQ(message, createResponseHandler(command));

    }

    protected void onStompAck(StompFrame command) throws ProtocolException {
        checkConnected();

        // TODO: acking with just a message id is very bogus
        // since the same message id could have been sent to 2 different
        // subscriptions
        // on the same stomp connection. For example, when 2 subs are created on
        // the same topic.

        Map<String, String> headers = command.getHeaders();
        String messageId = headers.get(Stomp.Headers.Ack.MESSAGE_ID);
        if (messageId == null) {
            throw new ProtocolException("ACK received without a message-id to acknowledge!");
        }

        TransactionId activemqTx = null;
        String stompTx = headers.get(Stomp.Headers.TRANSACTION);
        if (stompTx != null) {
            activemqTx = transactions.get(stompTx);
            if (activemqTx == null) {
                throw new ProtocolException("Invalid transaction id: " + stompTx);
            }
        }

        boolean acked = false;
        for (Iterator<StompSubscription> iter = subscriptionsByConsumerId.values().iterator(); iter.hasNext();) {
            StompSubscription sub = iter.next();
            MessageAck ack = sub.onStompMessageAck(messageId, activemqTx);
            if (ack != null) {
                ack.setTransactionId(activemqTx);
                sendToActiveMQ(ack, createResponseHandler(command));
                acked = true;
                break;
            }
        }

        if (!acked) {
            throw new ProtocolException("Unexpected ACK received for message-id [" + messageId + "]");
        }

    }

    protected void onStompBegin(StompFrame command) throws ProtocolException {
        checkConnected();

        Map<String, String> headers = command.getHeaders();

        String stompTx = headers.get(Stomp.Headers.TRANSACTION);

        if (!headers.containsKey(Stomp.Headers.TRANSACTION)) {
            throw new ProtocolException("Must specify the transaction you are beginning");
        }

        if (transactions.get(stompTx) != null) {
            throw new ProtocolException("The transaction was allready started: " + stompTx);
        }

        LocalTransactionId activemqTx = new LocalTransactionId(connectionId, transactionIdGenerator.getNextSequenceId());
        transactions.put(stompTx, activemqTx);

        TransactionInfo tx = new TransactionInfo();
        tx.setConnectionId(connectionId);
        tx.setTransactionId(activemqTx);
        tx.setType(TransactionInfo.BEGIN);

        sendToActiveMQ(tx, createResponseHandler(command));

    }

    protected void onStompCommit(StompFrame command) throws ProtocolException {
        checkConnected();

        Map<String, String> headers = command.getHeaders();

        String stompTx = headers.get(Stomp.Headers.TRANSACTION);
        if (stompTx == null) {
            throw new ProtocolException("Must specify the transaction you are committing");
        }

        TransactionId activemqTx = transactions.remove(stompTx);
        if (activemqTx == null) {
            throw new ProtocolException("Invalid transaction id: " + stompTx);
        }

        for (Iterator<StompSubscription> iter = subscriptionsByConsumerId.values().iterator(); iter.hasNext();) {
            StompSubscription sub = iter.next();
            sub.onStompCommit(activemqTx);
        }

        TransactionInfo tx = new TransactionInfo();
        tx.setConnectionId(connectionId);
        tx.setTransactionId(activemqTx);
        tx.setType(TransactionInfo.COMMIT_ONE_PHASE);

        sendToActiveMQ(tx, createResponseHandler(command));

    }

    protected void onStompAbort(StompFrame command) throws ProtocolException {
        checkConnected();
        Map<String, String> headers = command.getHeaders();

        String stompTx = headers.get(Stomp.Headers.TRANSACTION);
        if (stompTx == null) {
            throw new ProtocolException("Must specify the transaction you are committing");
        }

        TransactionId activemqTx = transactions.remove(stompTx);
        if (activemqTx == null) {
            throw new ProtocolException("Invalid transaction id: " + stompTx);
        }
        for (Iterator<StompSubscription> iter = subscriptionsByConsumerId.values().iterator(); iter.hasNext();) {
            StompSubscription sub = iter.next();
            try {
                sub.onStompAbort(activemqTx);
            } catch (Exception e) {
                throw new ProtocolException("Transaction abort failed", false, e);
            }
        }

        TransactionInfo tx = new TransactionInfo();
        tx.setConnectionId(connectionId);
        tx.setTransactionId(activemqTx);
        tx.setType(TransactionInfo.ROLLBACK);

        sendToActiveMQ(tx, createResponseHandler(command));

    }

    protected void onStompSubscribe(StompFrame command) throws ProtocolException {
        checkConnected();
        FrameTranslator translator = findTranslator(command.getHeaders().get(Stomp.Headers.TRANSFORMATION));
        Map<String, String> headers = command.getHeaders();

        String subscriptionId = headers.get(Stomp.Headers.Subscribe.ID);
        String destination = headers.get(Stomp.Headers.Subscribe.DESTINATION);

        ActiveMQDestination actualDest = translator.convertDestination(this, destination);

        if (actualDest == null) {
            throw new ProtocolException("Invalid Destination.");
        }

        ConsumerId id = new ConsumerId(sessionId, consumerIdGenerator.getNextSequenceId());
        ConsumerInfo consumerInfo = new ConsumerInfo(id);
        consumerInfo.setPrefetchSize(1000);
        consumerInfo.setDispatchAsync(true);

        String selector = headers.remove(Stomp.Headers.Subscribe.SELECTOR);
        consumerInfo.setSelector(selector);

        IntrospectionSupport.setProperties(consumerInfo, headers, "activemq.");

        consumerInfo.setDestination(translator.convertDestination(this, destination));

        StompSubscription stompSubscription = new StompSubscription(this, subscriptionId, consumerInfo, headers.get(Stomp.Headers.TRANSFORMATION));
        stompSubscription.setDestination(actualDest);

        String ackMode = headers.get(Stomp.Headers.Subscribe.ACK_MODE);
        if (Stomp.Headers.Subscribe.AckModeValues.CLIENT.equals(ackMode)) {
            stompSubscription.setAckMode(StompSubscription.CLIENT_ACK);
        } else if (Stomp.Headers.Subscribe.AckModeValues.INDIVIDUAL.equals(ackMode)) {
            stompSubscription.setAckMode(StompSubscription.INDIVIDUAL_ACK);
        } else {
            stompSubscription.setAckMode(StompSubscription.AUTO_ACK);
        }

        subscriptionsByConsumerId.put(id, stompSubscription);
        sendToActiveMQ(consumerInfo, createResponseHandler(command));

    }

    protected void onStompUnsubscribe(StompFrame command) throws ProtocolException {
        checkConnected();
        Map<String, String> headers = command.getHeaders();

        ActiveMQDestination destination = null;
        Object o = headers.get(Stomp.Headers.Unsubscribe.DESTINATION);
        if (o != null) {
            destination = findTranslator(command.getHeaders().get(Stomp.Headers.TRANSFORMATION)).convertDestination(this, (String)o);
        }

        String subscriptionId = headers.get(Stomp.Headers.Unsubscribe.ID);

        if (subscriptionId == null && destination == null) {
            throw new ProtocolException("Must specify the subscriptionId or the destination you are unsubscribing from");
        }

        // check if it is a durable subscription
        String durable = command.getHeaders().get("activemq.subscriptionName");
        if (durable != null) {
            RemoveSubscriptionInfo info = new RemoveSubscriptionInfo();
            info.setClientId(durable);
            info.setSubscriptionName(durable);
            info.setConnectionId(connectionId);
            sendToActiveMQ(info, createResponseHandler(command));
            return;
        }

        // TODO: Unsubscribing using a destination is a bit wierd if multiple
        // subscriptions
        // are created with the same destination. Perhaps this should be
        // removed.
        //
        for (Iterator<StompSubscription> iter = subscriptionsByConsumerId.values().iterator(); iter.hasNext();) {
            StompSubscription sub = iter.next();
            if ((subscriptionId != null && subscriptionId.equals(sub.getSubscriptionId())) || (destination != null && destination.equals(sub.getDestination()))) {
                sendToActiveMQ(sub.getConsumerInfo().createRemoveCommand(), createResponseHandler(command));
                iter.remove();
                return;
            }
        }

        throw new ProtocolException("No subscription matched.");
    }

    ConnectionInfo connectionInfo = new ConnectionInfo();

    protected void onStompConnect(final StompFrame command) throws ProtocolException {

        if (connected.get()) {
            throw new ProtocolException("Allready connected.");
        }

        final Map<String, String> headers = command.getHeaders();

        // allow anyone to login for now
        String login = headers.get(Stomp.Headers.Connect.LOGIN);
        String passcode = headers.get(Stomp.Headers.Connect.PASSCODE);
        String clientId = headers.get(Stomp.Headers.Connect.CLIENT_ID);


        IntrospectionSupport.setProperties(connectionInfo, headers, "activemq.");

        connectionInfo.setConnectionId(connectionId);
        if (clientId != null) {
            connectionInfo.setClientId(clientId);
        } else {
            connectionInfo.setClientId("" + connectionInfo.getConnectionId().toString());
        }

        connectionInfo.setResponseRequired(true);
        connectionInfo.setUserName(login);
        connectionInfo.setPassword(passcode);
        connectionInfo.setTransportContext(stompTransport.getPeerCertificates());

        sendToActiveMQ(connectionInfo, new ResponseHandler() {
            public void onResponse(ProtocolConverter converter, Response response) throws IOException {

                if (response.isException()) {
                    // If the connection attempt fails we close the socket.
                    Throwable exception = ((ExceptionResponse)response).getException();
                    handleException(exception, command);
                    getStompTransport().onException(IOExceptionSupport.create(exception));
                    return;
                }

                final SessionInfo sessionInfo = new SessionInfo(sessionId);
                sendToActiveMQ(sessionInfo, null);

                final ProducerInfo producerInfo = new ProducerInfo(producerId);
                sendToActiveMQ(producerInfo, new ResponseHandler() {
                    public void onResponse(ProtocolConverter converter, Response response) throws IOException {

                        if (response.isException()) {
                            // If the connection attempt fails we close the socket.
                            Throwable exception = ((ExceptionResponse)response).getException();
                            handleException(exception, command);
                            getStompTransport().onException(IOExceptionSupport.create(exception));
                        }

                        connected.set(true);
                        HashMap<String, String> responseHeaders = new HashMap<String, String>();

                        responseHeaders.put(Stomp.Headers.Connected.SESSION, connectionInfo.getClientId());
                        String requestId = headers.get(Stomp.Headers.Connect.REQUEST_ID);
                        if (requestId == null) {
                            // TODO legacy
                            requestId = headers.get(Stomp.Headers.RECEIPT_REQUESTED);
                        }
                        if (requestId != null) {
                            // TODO legacy
                            responseHeaders.put(Stomp.Headers.Connected.RESPONSE_ID, requestId);
                            responseHeaders.put(Stomp.Headers.Response.RECEIPT_ID, requestId);
                        }

                        StompFrame sc = new StompFrame();
                        sc.setAction(Stomp.Responses.CONNECTED);
                        sc.setHeaders(responseHeaders);
                        sendToStomp(sc);
                    }
                });

            }
        });
    }

    protected void onStompDisconnect(StompFrame command) throws ProtocolException {
        checkConnected();
        sendToActiveMQ(connectionInfo.createRemoveCommand(), createResponseHandler(command));
        sendToActiveMQ(new ShutdownInfo(), createResponseHandler(command));
        connected.set(false);
    }

    protected void checkConnected() throws ProtocolException {
        if (!connected.get()) {
            throw new ProtocolException("Not connected.");
        }
    }

    /**
     * Dispatch a ActiveMQ command
     *
     * @param command
     * @throws IOException
     */
    public void onActiveMQCommand(Command command) throws IOException, JMSException {
        if (command.isResponse()) {

            Response response = (Response)command;
            ResponseHandler rh = resposeHandlers.remove(Integer.valueOf(response.getCorrelationId()));
            if (rh != null) {
                rh.onResponse(this, response);
            } else {
                // Pass down any unexpected errors. Should this close the connection?
                if (response.isException()) {
                    Throwable exception = ((ExceptionResponse)response).getException();
                    handleException(exception, null);
                }
            }
        } else if (command.isMessageDispatch()) {

            MessageDispatch md = (MessageDispatch)command;
            StompSubscription sub = subscriptionsByConsumerId.get(md.getConsumerId());
            if (sub != null) {
                sub.onMessageDispatch(md);
            }
        } else if (command.getDataStructureType() == ConnectionError.DATA_STRUCTURE_TYPE) {
            // Pass down any unexpected async errors. Should this close the connection?
            Throwable exception = ((ConnectionError)command).getException();
            handleException(exception, null);
        }
    }

    public ActiveMQMessage convertMessage(StompFrame command) throws IOException, JMSException {
        ActiveMQMessage msg = findTranslator(command.getHeaders().get(Stomp.Headers.TRANSFORMATION)).convertFrame(this, command);
        return msg;
    }

    public StompFrame convertMessage(ActiveMQMessage message, boolean ignoreTransformation) throws IOException, JMSException {
        if (ignoreTransformation == true) {
            return frameTranslator.convertMessage(this, message);
        } else {
            return findTranslator(message.getStringProperty(Stomp.Headers.TRANSFORMATION)).convertMessage(this, message);
        }
    }

    public StompTransport getStompTransport() {
        return stompTransport;
    }

    public ActiveMQDestination createTempQueue(String name) {
        ActiveMQDestination rc = tempDestinations.get(name);
        if( rc == null ) {
            rc = new ActiveMQTempQueue(connectionId, tempDestinationGenerator.getNextSequenceId());
            sendToActiveMQ(new DestinationInfo(connectionId, DestinationInfo.ADD_OPERATION_TYPE, rc), null);
            tempDestinations.put(name, rc);
        }
        return rc;
    }

    public ActiveMQDestination createTempTopic(String name) {
        ActiveMQDestination rc = tempDestinations.get(name);
        if( rc == null ) {
            rc = new ActiveMQTempTopic(connectionId, tempDestinationGenerator.getNextSequenceId());
            sendToActiveMQ(new DestinationInfo(connectionId, DestinationInfo.ADD_OPERATION_TYPE, rc), null);
            tempDestinations.put(name, rc);
            tempDestinationAmqToStompMap.put(rc.getQualifiedName(), name);
        }
        return rc;
    }

    public String getCreatedTempDestinationName(ActiveMQDestination destination) {
        return tempDestinationAmqToStompMap.get(destination.getQualifiedName());
    }
}
