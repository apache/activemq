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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.jms.JMSException;

import org.apache.activemq.ActiveMQPrefetchPolicy;
import org.apache.activemq.advisory.AdvisorySupport;
import org.apache.activemq.broker.BrokerContext;
import org.apache.activemq.broker.BrokerContextAware;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.ActiveMQMessage;
import org.apache.activemq.command.ActiveMQTempQueue;
import org.apache.activemq.command.ActiveMQTempTopic;
import org.apache.activemq.command.Command;
import org.apache.activemq.command.CommandTypes;
import org.apache.activemq.command.ConnectionError;
import org.apache.activemq.command.ConnectionId;
import org.apache.activemq.command.ConnectionInfo;
import org.apache.activemq.command.ConsumerControl;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author <a href="http://hiramchirino.com">chirino</a>
 */
public class ProtocolConverter {

    private static final Logger LOG = LoggerFactory.getLogger(ProtocolConverter.class);

    private static final IdGenerator CONNECTION_ID_GENERATOR = new IdGenerator();

    private static final String BROKER_VERSION;
    private static final StompFrame ping = new StompFrame(Stomp.Commands.KEEPALIVE);

    static {
        String version = "5.6.0";
        try(InputStream in = ProtocolConverter.class.getResourceAsStream("/org/apache/activemq/version.txt")) {
            if (in != null) {
                try(InputStreamReader isr = new InputStreamReader(in);
                    BufferedReader reader = new BufferedReader(isr)) {
                    version = reader.readLine();
                }
            }
        }catch(Exception e) {
        }

        BROKER_VERSION = version;
    }

    private final ConnectionId connectionId = new ConnectionId(CONNECTION_ID_GENERATOR.generateId());
    private final SessionId sessionId = new SessionId(connectionId, -1);
    private final ProducerId producerId = new ProducerId(sessionId, 1);

    private final LongSequenceGenerator consumerIdGenerator = new LongSequenceGenerator();
    private final LongSequenceGenerator messageIdGenerator = new LongSequenceGenerator();
    private final LongSequenceGenerator transactionIdGenerator = new LongSequenceGenerator();
    private final LongSequenceGenerator tempDestinationGenerator = new LongSequenceGenerator();

    private final ConcurrentMap<Integer, ResponseHandler> resposeHandlers = new ConcurrentHashMap<>();
    private final ConcurrentMap<ConsumerId, StompSubscription> subscriptionsByConsumerId = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, StompSubscription> subscriptions = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, ActiveMQDestination> tempDestinations = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, String> tempDestinationAmqToStompMap = new ConcurrentHashMap<>();
    private final Map<String, LocalTransactionId> transactions = new ConcurrentHashMap<>();
    private final StompTransport stompTransport;

    // Global Map shared with all subscriptions to allow finding the sub associated with an ACK Id
    private final ConcurrentMap<String, StompAckEntry> pendingAcksTracker = new ConcurrentHashMap<>();
    // Read-Only view used in this class to enforce the separation of read vs update of the global index.
    private final Map<String, StompAckEntry> pendingAcks = Collections.unmodifiableMap(pendingAcksTracker);

    private final Object commnadIdMutex = new Object();
    private int lastCommandId;
    private final AtomicBoolean connected = new AtomicBoolean(false);
    private final FrameTranslator frameTranslator = new LegacyFrameTranslator();
    private ConcurrentMap<String, FrameTranslator> jmsFrameTranslators=new ConcurrentHashMap<String,FrameTranslator>();
  
    private final FactoryFinder FRAME_TRANSLATOR_FINDER = new FactoryFinder("META-INF/services/org/apache/activemq/transport/frametranslator/");
    private final BrokerContext brokerContext;
    private String version = "1.0";
    private long hbReadInterval;
    private long hbWriteInterval;
    private float hbGracePeriodMultiplier = 1.0f;
    private String defaultHeartBeat = Stomp.DEFAULT_HEART_BEAT;

    public ProtocolConverter(StompTransport stompTransport, BrokerContext brokerContext) {
        this.stompTransport = stompTransport;
        this.brokerContext = brokerContext;
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
                @Override
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
        return findTranslator(header, null, false);
    }

    protected FrameTranslator findTranslator(String header, ActiveMQDestination destination, boolean advisory) {
        FrameTranslator translator = frameTranslator;
        try {
            if (header != null) {
               translator=jmsFrameTranslators.get(header);
            	if(translator==null) {
            		LOG.info("Creating a new FrameTranslator to convert "+header);
            		translator = (FrameTranslator) FRAME_TRANSLATOR_FINDER.newInstance(header);
            		if(translator!=null) {
            			LOG.info("Created a new FrameTranslator to convert "+header);
            			jmsFrameTranslators.put(header,translator);
            		}else {
            			LOG.error("Failed in creating FrameTranslator to convert "+header);
            		}            			
            	}
            } else {
                if (destination != null && (advisory || AdvisorySupport.isAdvisoryTopic(destination))) {
                    translator = new JmsFrameTranslator();
                }
            }
        } catch (Exception ignore) {
            // if anything goes wrong use the default translator
			LOG.debug("Failed in getting a FrameTranslator to convert ", ignore);
           	translator = frameTranslator;
        }

        if (translator instanceof BrokerContextAware) {
            ((BrokerContextAware)translator).setBrokerContext(brokerContext);
        }

        return translator;
    }

    /**
     * Convert a STOMP command
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
            } else if (action.startsWith(Stomp.Commands.NACK)) {
                onStompNack(command);
            } else if (action.startsWith(Stomp.Commands.BEGIN)) {
                onStompBegin(command);
            } else if (action.startsWith(Stomp.Commands.COMMIT)) {
                onStompCommit(command);
            } else if (action.startsWith(Stomp.Commands.ABORT)) {
                onStompAbort(command);
            } else if (action.startsWith(Stomp.Commands.SUBSCRIBE_PREFIX)) {
                onStompSubscribe(command);
            } else if (action.startsWith(Stomp.Commands.UNSUBSCRIBE_PREFIX)) {
                onStompUnsubscribe(command);
            } else if (action.startsWith(Stomp.Commands.CONNECT) ||
                       action.startsWith(Stomp.Commands.STOMP)) {
                onStompConnect(command);
            } else if (action.startsWith(Stomp.Commands.DISCONNECT)) {
                onStompDisconnect(command);
            } else {
                throw new ProtocolException("Unknown STOMP action: " + action, true);
            }

        } catch (ProtocolException e) {
            handleException(e, command);
            // Some protocol errors can cause the connection to get closed.
            if (e.isFatal()) {
               getStompTransport().onException(e);
            }
        }
    }

    protected void handleException(Throwable exception, StompFrame command) throws IOException {
        if (command == null) {
            LOG.warn("Exception occurred while processing a command: {}", exception.toString());
        } else {
            if (exception instanceof JMSException) {
                JMSException jmsException = (JMSException) exception;
                if (jmsException.getLinkedException() != null) {
                    LOG.warn("Exception occurred for client {} ({}) processing: {} -> {} ({})", connectionInfo.getClientId(), connectionInfo.getClientIp(), safeGetAction(command), exception.toString(), jmsException.getLinkedException().toString());
                } else {
                    LOG.warn("Exception occurred for client {} ({}) processing: {} -> {}", connectionInfo.getClientId(), connectionInfo.getClientIp(), safeGetAction(command), exception.toString());
                }
            } else {
                LOG.warn("Exception occurred for client {} ({}) processing: {} -> {}", connectionInfo.getClientId(), connectionInfo.getClientIp(), safeGetAction(command), exception.toString());
            }
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("Exception detail", exception);
        }

        if (command != null && LOG.isTraceEnabled()) {
            LOG.trace("Command that caused the error: {}", command);
        }

        // Let the stomp client know about any protocol errors.
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        PrintWriter stream = new PrintWriter(new OutputStreamWriter(baos, "UTF-8"));
        if (exception instanceof SecurityException || exception.getCause() instanceof SecurityException) {
            stream.write(exception.getLocalizedMessage());
        } else {
            exception.printStackTrace(stream);
        }
        stream.close();

        HashMap<String, String> headers = new HashMap<>();
        headers.put(Stomp.Headers.Error.MESSAGE, exception.getMessage());
        headers.put(Stomp.Headers.CONTENT_TYPE, "text/plain");

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
        String destination = headers.get(Stomp.Headers.Send.DESTINATION);
        if (destination == null) {
            throw new ProtocolException("SEND received without a Destination specified!");
        }

        String stompTx = headers.get(Stomp.Headers.TRANSACTION);
        headers.remove("transaction");

        ActiveMQMessage message = convertMessage(command);

        message.setProducerId(producerId);
        MessageId id = new MessageId(producerId, messageIdGenerator.getNextSequenceId());
        message.setMessageId(id);

        if (stompTx != null) {
            TransactionId activemqTx = transactions.get(stompTx);
            if (activemqTx == null) {
                throw new ProtocolException("Invalid transaction id: " + stompTx);
            }
            message.setTransactionId(activemqTx);
        }

        message.onSend();
        message.beforeMarshall(null);
        sendToActiveMQ(message, createResponseHandler(command));
    }

    protected void onStompNack(StompFrame command) throws ProtocolException {

        checkConnected();

        if (this.version.equals(Stomp.V1_0)) {
            throw new ProtocolException("NACK received but connection is in v1.0 mode.");
        }

        Map<String, String> headers = command.getHeaders();

        String subscriptionId = headers.get(Stomp.Headers.Ack.SUBSCRIPTION);
        if (subscriptionId == null && !this.version.equals(Stomp.V1_2)) {
            throw new ProtocolException("NACK received without a subscription id for acknowledge!");
        }

        String messageId = headers.get(Stomp.Headers.Ack.MESSAGE_ID);
        if (messageId == null && !this.version.equals(Stomp.V1_2)) {
            throw new ProtocolException("NACK received without a message-id to acknowledge!");
        }

        String ackId = headers.get(Stomp.Headers.Ack.ACK_ID);
        if (ackId == null && this.version.equals(Stomp.V1_2)) {
            throw new ProtocolException("NACK received without an ack header to acknowledge!");
        }

        TransactionId activemqTx = null;
        String stompTx = headers.get(Stomp.Headers.TRANSACTION);
        if (stompTx != null) {
            activemqTx = transactions.get(stompTx);
            if (activemqTx == null) {
                throw new ProtocolException("Invalid transaction id: " + stompTx);
            }
        }

        boolean nacked = false;

        if (ackId != null) {
            StompAckEntry pendingAck = this.pendingAcks.get(ackId);
            if (pendingAck != null) {
                messageId = pendingAck.getMessageId().toString();
                MessageAck ack = pendingAck.onMessageNack(activemqTx);
                if (ack != null) {
                    sendToActiveMQ(ack, createResponseHandler(command));
                    nacked = true;
                }
            }
        } else if (subscriptionId != null) {
            StompSubscription sub = this.subscriptions.get(subscriptionId);
            if (sub != null) {
                MessageAck ack = sub.onStompMessageNack(messageId, activemqTx);
                if (ack != null) {
                    sendToActiveMQ(ack, createResponseHandler(command));
                    nacked = true;
                }
            }
        }

        if (!nacked) {
            throw new ProtocolException("Unexpected NACK received for message-id [" + messageId + "]");
        }
    }

    protected void onStompAck(StompFrame command) throws ProtocolException {
        checkConnected();

        Map<String, String> headers = command.getHeaders();
        String messageId = headers.get(Stomp.Headers.Ack.MESSAGE_ID);
        if (messageId == null && !(this.version.equals(Stomp.V1_2))) {
            throw new ProtocolException("ACK received without a message-id to acknowledge!");
        }

        String subscriptionId = headers.get(Stomp.Headers.Ack.SUBSCRIPTION);
        if (subscriptionId == null && this.version.equals(Stomp.V1_1)) {
            throw new ProtocolException("ACK received without a subscription id for acknowledge!");
        }

        String ackId = headers.get(Stomp.Headers.Ack.ACK_ID);
        if (ackId == null && this.version.equals(Stomp.V1_2)) {
            throw new ProtocolException("ACK received without a ack id for acknowledge!");
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

        if (ackId != null) {
            StompAckEntry pendingAck = this.pendingAcks.get(ackId);
            if (pendingAck != null) {
                messageId = pendingAck.getMessageId().toString();
                MessageAck ack = pendingAck.onMessageAck(activemqTx);
                if (ack != null) {
                    sendToActiveMQ(ack, createResponseHandler(command));
                    acked = true;
                }
            }

        } else if (subscriptionId != null) {
            StompSubscription sub = this.subscriptions.get(subscriptionId);
            if (sub != null) {
                MessageAck ack = sub.onStompMessageAck(messageId, activemqTx);
                if (ack != null) {
                    sendToActiveMQ(ack, createResponseHandler(command));
                    acked = true;
                }
            }
        } else {
            // STOMP v1.0: acking with just a message id is very bogus since the same message id
            // could have been sent to 2 different subscriptions on the same Stomp connection.
            // For example, when 2 subs are created on the same topic.
            for (StompSubscription sub : subscriptionsByConsumerId.values()) {
                MessageAck ack = sub.onStompMessageAck(messageId, activemqTx);
                if (ack != null) {
                    sendToActiveMQ(ack, createResponseHandler(command));
                    acked = true;
                    break;
                }
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
            throw new ProtocolException("The transaction was already started: " + stompTx);
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

        for (StompSubscription sub : subscriptionsByConsumerId.values()) {
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
        for (StompSubscription sub : subscriptionsByConsumerId.values()) {
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

        if (!this.version.equals(Stomp.V1_0) && subscriptionId == null) {
            throw new ProtocolException("SUBSCRIBE received without a subscription id!");
        }

        if (destination == null || "".equals(destination)) {
            throw new ProtocolException("Invalid empty or 'null' Destination header");
        }

        final ActiveMQDestination actualDest = translator.convertDestination(this, destination, true);

        if (actualDest == null) {
            throw new ProtocolException("Invalid 'null' Destination.");
        }

        final ConsumerId id = new ConsumerId(sessionId, consumerIdGenerator.getNextSequenceId());
        ConsumerInfo consumerInfo = new ConsumerInfo(id);
        consumerInfo.setPrefetchSize(actualDest.isQueue() ?
                ActiveMQPrefetchPolicy.DEFAULT_QUEUE_PREFETCH :
                headers.containsKey("activemq.subscriptionName") ?
                        ActiveMQPrefetchPolicy.DEFAULT_DURABLE_TOPIC_PREFETCH : ActiveMQPrefetchPolicy.DEFAULT_TOPIC_PREFETCH);
        consumerInfo.setDispatchAsync(true);

        String browser = headers.get(Stomp.Headers.Subscribe.BROWSER);
        if (browser != null && browser.equals(Stomp.TRUE)) {

            if (this.version.equals(Stomp.V1_0)) {
                throw new ProtocolException("Queue Browser feature only valid for Stomp v1.1+ clients!");
            }

            consumerInfo.setBrowser(true);
            consumerInfo.setPrefetchSize(ActiveMQPrefetchPolicy.DEFAULT_QUEUE_BROWSER_PREFETCH);
        }

        String selector = headers.remove(Stomp.Headers.Subscribe.SELECTOR);
        if (selector != null) {
            consumerInfo.setSelector("convert_string_expressions:" + selector);
        }

        IntrospectionSupport.setProperties(consumerInfo, headers, "activemq.");

        if (actualDest.isQueue() && consumerInfo.getSubscriptionName() != null) {
            throw new ProtocolException("Invalid Subscription: cannot durably subscribe to a Queue destination!");
        }

        consumerInfo.setDestination(actualDest);
        consumerInfo.setDispatchAsync(true);

        StompSubscription stompSubscription;
        if (!consumerInfo.isBrowser()) {
            stompSubscription = new StompSubscription(this, subscriptionId, consumerInfo, headers.get(Stomp.Headers.TRANSFORMATION), pendingAcksTracker);
        } else {
            stompSubscription = new StompQueueBrowserSubscription(this, subscriptionId, consumerInfo, headers.get(Stomp.Headers.TRANSFORMATION), pendingAcksTracker);
        }
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
        // Stomp v1.0 doesn't need to set this header so we avoid an NPE if not set.
        if (subscriptionId != null) {
            subscriptions.put(subscriptionId, stompSubscription);
        }

        final String receiptId = command.getHeaders().get(Stomp.Headers.RECEIPT_REQUESTED);
        if (receiptId != null && consumerInfo.getPrefetchSize() > 0) {

            final StompFrame cmd = command;
            final int prefetch = consumerInfo.getPrefetchSize();

            // Since dispatch could beat the receipt we set prefetch to zero to start and then
            // once we've sent our Receipt we are safe to turn on dispatch if the response isn't
            // an error message.
            consumerInfo.setPrefetchSize(0);

            final ResponseHandler handler = new ResponseHandler() {
                @Override
                public void onResponse(ProtocolConverter converter, Response response) throws IOException {
                    if (response.isException()) {
                        // Generally a command can fail.. but that does not invalidate the connection.
                        // We report back the failure but we don't close the connection.
                        Throwable exception = ((ExceptionResponse)response).getException();
                        handleException(exception, cmd);
                    } else {
                        StompFrame sc = new StompFrame();
                        sc.setAction(Stomp.Responses.RECEIPT);
                        sc.setHeaders(new HashMap<String, String>(1));
                        sc.getHeaders().put(Stomp.Headers.Response.RECEIPT_ID, receiptId);
                        stompTransport.sendToStomp(sc);

                        ConsumerControl control = new ConsumerControl();
                        control.setPrefetch(prefetch);
                        control.setDestination(actualDest);
                        control.setConsumerId(id);

                        sendToActiveMQ(control, null);
                    }
                }
            };

            sendToActiveMQ(consumerInfo, handler);
        } else {
            sendToActiveMQ(consumerInfo, createResponseHandler(command));
        }
    }

    protected void onStompUnsubscribe(StompFrame command) throws ProtocolException {
        checkConnected();
        Map<String, String> headers = command.getHeaders();

        ActiveMQDestination destination = null;
        Object o = headers.get(Stomp.Headers.Unsubscribe.DESTINATION);
        if (o != null) {
            destination = findTranslator(command.getHeaders().get(Stomp.Headers.TRANSFORMATION)).convertDestination(this, (String)o, true);
        }

        String subscriptionId = headers.get(Stomp.Headers.Unsubscribe.ID);
        if (!this.version.equals(Stomp.V1_0) && subscriptionId == null) {
            throw new ProtocolException("UNSUBSCRIBE received without a subscription id!");
        }

        if (subscriptionId == null && destination == null) {
            throw new ProtocolException("Must specify the subscriptionId or the destination you are unsubscribing from");
        }

        // check if it is a durable subscription
        String durable = command.getHeaders().get("activemq.subscriptionName");
        String clientId = durable;
        if (!this.version.equals(Stomp.V1_0)) {
            clientId = connectionInfo.getClientId();
        }

        if (durable != null) {
            RemoveSubscriptionInfo info = new RemoveSubscriptionInfo();
            info.setClientId(clientId);
            info.setSubscriptionName(durable);
            info.setConnectionId(connectionId);
            sendToActiveMQ(info, createResponseHandler(command));
            return;
        }

        if (subscriptionId != null) {
            StompSubscription sub = this.subscriptions.remove(subscriptionId);
            if (sub != null) {
                sendToActiveMQ(sub.getConsumerInfo().createRemoveCommand(), createResponseHandler(command));
                return;
            }
        } else {
            // Unsubscribing using a destination is a bit weird if multiple subscriptions
            // are created with the same destination.
            for (Iterator<StompSubscription> iter = subscriptionsByConsumerId.values().iterator(); iter.hasNext();) {
                StompSubscription sub = iter.next();
                if (destination != null && destination.equals(sub.getDestination())) {
                    sendToActiveMQ(sub.getConsumerInfo().createRemoveCommand(), createResponseHandler(command));
                    iter.remove();
                    return;
                }
            }
        }

        throw new ProtocolException("No subscription matched.");
    }

    ConnectionInfo connectionInfo = new ConnectionInfo();

    protected void onStompConnect(final StompFrame command) throws ProtocolException {

        if (connected.get()) {
            throw new ProtocolException("Already connected.");
        }

        final Map<String, String> headers = command.getHeaders();

        // allow anyone to login for now
        String login = headers.get(Stomp.Headers.Connect.LOGIN);
        String passcode = headers.get(Stomp.Headers.Connect.PASSCODE);
        String clientId = headers.get(Stomp.Headers.Connect.CLIENT_ID);
        String heartBeat = headers.get(Stomp.Headers.Connect.HEART_BEAT);
        String host = headers.get(Stomp.Headers.Connect.HOST);

        if (heartBeat == null) {
            heartBeat = defaultHeartBeat;
        }

        this.version = StompCodec.detectVersion(headers);

        configureInactivityMonitor(heartBeat.trim());

        IntrospectionSupport.setProperties(connectionInfo, headers, "activemq.");
        connectionInfo.setConnectionId(connectionId);
        if (clientId != null) {
            connectionInfo.setClientId(clientId);
        } else {
            connectionInfo.setClientId("" + connectionInfo.getConnectionId().toString());
        }
        connectionInfo.setClientIp(host);

        connectionInfo.setResponseRequired(true);
        connectionInfo.setUserName(login);
        connectionInfo.setPassword(passcode);
        connectionInfo.setTransportContext(command.getTransportContext());

        sendToActiveMQ(connectionInfo, new ResponseHandler() {
            @Override
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
                    @Override
                    public void onResponse(ProtocolConverter converter, Response response) throws IOException {

                        if (response.isException()) {
                            // If the connection attempt fails we close the socket.
                            Throwable exception = ((ExceptionResponse)response).getException();
                            handleException(exception, command);
                            getStompTransport().onException(IOExceptionSupport.create(exception));
                        }

                        connected.set(true);
                        HashMap<String, String> responseHeaders = new HashMap<>();

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

                        responseHeaders.put(Stomp.Headers.Connected.VERSION, version);
                        responseHeaders.put(Stomp.Headers.Connected.HEART_BEAT,
                                            String.format("%d,%d", hbWriteInterval, hbReadInterval));
                        responseHeaders.put(Stomp.Headers.Connected.SERVER, "ActiveMQ/"+BROKER_VERSION);

                        StompFrame sc = new StompFrame();
                        sc.setAction(Stomp.Responses.CONNECTED);
                        sc.setHeaders(responseHeaders);
                        sendToStomp(sc);

                        StompWireFormat format = stompTransport.getWireFormat();
                        if (format != null) {
                            format.setStompVersion(version);
                        }
                    }
                });
            }
        });
    }

    protected void onStompDisconnect(StompFrame command) throws ProtocolException {
        if (connected.get()) {
            LOG.trace("Connection closed with {} pending ACKs still being tracked.", pendingAcks.size());
            sendToActiveMQ(connectionInfo.createRemoveCommand(), createResponseHandler(command));
            sendToActiveMQ(new ShutdownInfo(), createResponseHandler(command));
            connected.set(false);
        }
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
        } else if (command.getDataStructureType() == CommandTypes.KEEP_ALIVE_INFO) {
            stompTransport.sendToStomp(ping);
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
            FrameTranslator translator = findTranslator(
                message.getStringProperty(Stomp.Headers.TRANSFORMATION), message.getDestination(), message.isAdvisory());
            return translator.convertMessage(this, message);
        }
    }

    public StompTransport getStompTransport() {
        return stompTransport;
    }

    public ActiveMQDestination createTempDestination(String name, boolean topic) {
        ActiveMQDestination rc = tempDestinations.get(name);
        if( rc == null ) {
            if (topic) {
                rc = new ActiveMQTempTopic(connectionId, tempDestinationGenerator.getNextSequenceId());
            } else {
                rc = new ActiveMQTempQueue(connectionId, tempDestinationGenerator.getNextSequenceId());
            }
            sendToActiveMQ(new DestinationInfo(connectionId, DestinationInfo.ADD_OPERATION_TYPE, rc), null);
            tempDestinations.put(name, rc);
            tempDestinationAmqToStompMap.put(rc.getQualifiedName(), name);
        }
        return rc;
    }

    public String getCreatedTempDestinationName(ActiveMQDestination destination) {
        return tempDestinationAmqToStompMap.get(destination.getQualifiedName());
    }

    public String getDefaultHeartBeat() {
        return defaultHeartBeat;
    }

    public void setDefaultHeartBeat(String defaultHeartBeat) {
        this.defaultHeartBeat = defaultHeartBeat;
    }

    /**
     * @return the hbGracePeriodMultiplier
     */
    public float getHbGracePeriodMultiplier() {
        return hbGracePeriodMultiplier;
    }

    /**
     * @param hbGracePeriodMultiplier the hbGracePeriodMultiplier to set
     */
    public void setHbGracePeriodMultiplier(float hbGracePeriodMultiplier) {
        this.hbGracePeriodMultiplier = hbGracePeriodMultiplier;
    }

    protected void configureInactivityMonitor(String heartBeatConfig) throws ProtocolException {

        String[] keepAliveOpts = heartBeatConfig.split(Stomp.COMMA);

        if (keepAliveOpts == null || keepAliveOpts.length != 2) {
            throw new ProtocolException("Invalid heart-beat header:" + heartBeatConfig, true);
        } else {

            try {
                hbReadInterval = (Long.parseLong(keepAliveOpts[0]));
                hbWriteInterval = Long.parseLong(keepAliveOpts[1]);
            } catch(NumberFormatException e) {
                throw new ProtocolException("Invalid heart-beat header:" + heartBeatConfig, true);
            }

            try {
                StompInactivityMonitor monitor = this.stompTransport.getInactivityMonitor();
                monitor.setReadCheckTime((long) (hbReadInterval * hbGracePeriodMultiplier));
                monitor.setInitialDelayTime(Math.min(hbReadInterval, hbWriteInterval));
                monitor.setWriteCheckTime(hbWriteInterval);
                monitor.startMonitoring();
            } catch(Exception ex) {
                hbReadInterval = 0;
                hbWriteInterval = 0;
            }

            if (LOG.isDebugEnabled()) {
                LOG.debug("Stomp Connect heartbeat conf RW[{},{}]", hbReadInterval, hbWriteInterval);
            }
        }
    }

    protected void sendReceipt(StompFrame command) {
        final String receiptId = command.getHeaders().get(Stomp.Headers.RECEIPT_REQUESTED);
        if (receiptId != null) {
            StompFrame sc = new StompFrame();
            sc.setAction(Stomp.Responses.RECEIPT);
            sc.setHeaders(new HashMap<String, String>(1));
            sc.getHeaders().put(Stomp.Headers.Response.RECEIPT_ID, receiptId);
            try {
                sendToStomp(sc);
            } catch (IOException e) {
                LOG.warn("Could not send a receipt for {}", command, e);
            }
        }
    }

    /**
     * Retrieve the STOMP action value from a frame if the value is valid, otherwise
     * return an unknown string to allow for safe log output.
     *
     * @param command
     *      The STOMP command to fetch an action from.
     *
     * @return the command action or a safe string to use in logging.
     */
    protected Object safeGetAction(StompFrame command) {
        String result = "<Unknown>";
        if (command != null && command.getAction() != null) {
            String action = command.getAction().trim();

            if (action != null) {
                switch (action) {
                    case Stomp.Commands.SEND:
                    case Stomp.Commands.ACK:
                    case Stomp.Commands.NACK:
                    case Stomp.Commands.BEGIN:
                    case Stomp.Commands.COMMIT:
                    case Stomp.Commands.ABORT:
                    case Stomp.Commands.SUBSCRIBE:
                    case Stomp.Commands.UNSUBSCRIBE:
                    case Stomp.Commands.CONNECT:
                    case Stomp.Commands.STOMP:
                    case Stomp.Commands.DISCONNECT:
                        result = action;
                        break;
                    case Stomp.Commands.SUBSCRIBE_PREFIX:
                        result = Stomp.Commands.SUBSCRIBE;
                    case Stomp.Commands.UNSUBSCRIBE_PREFIX:
                        result = Stomp.Commands.UNSUBSCRIBE;
                    default:
                        break;
                }
            }
        }

        return result;
    }

    boolean isStomp10() {
        return version.equals(Stomp.V1_0);
    }

    boolean isStomp11() {
        return version.equals(Stomp.V1_1);
    }

    boolean isStomp12() {
        return version.equals(Stomp.V1_2);
    }
}
