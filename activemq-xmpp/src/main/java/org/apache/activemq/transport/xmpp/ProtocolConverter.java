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
package org.apache.activemq.transport.xmpp;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.FutureTask;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.jms.JMSException;
import org.w3c.dom.Element;

import ietf.params.xml.ns.xmpp_sasl.Auth;
import ietf.params.xml.ns.xmpp_sasl.Challenge;
import ietf.params.xml.ns.xmpp_sasl.Success;
import ietf.params.xml.ns.xmpp_tls.Proceed;
import ietf.params.xml.ns.xmpp_tls.Starttls;
import jabber.client.Body;
import jabber.client.Error;
import jabber.client.Iq;
import jabber.client.Message;
import jabber.client.Presence;
import jabber.iq.auth.Query;

import org.apache.activemq.advisory.AdvisorySupport;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.ActiveMQMessage;
import org.apache.activemq.command.ActiveMQTempQueue;
import org.apache.activemq.command.ActiveMQTextMessage;
import org.apache.activemq.command.ActiveMQTopic;
import org.apache.activemq.command.Command;
import org.apache.activemq.command.ConnectionId;
import org.apache.activemq.command.ConnectionInfo;
import org.apache.activemq.command.ConsumerId;
import org.apache.activemq.command.ConsumerInfo;
import org.apache.activemq.command.DestinationInfo;
import org.apache.activemq.command.ExceptionResponse;
import org.apache.activemq.command.MessageAck;
import org.apache.activemq.command.MessageDispatch;
import org.apache.activemq.command.MessageId;
import org.apache.activemq.command.ProducerId;
import org.apache.activemq.command.ProducerInfo;
import org.apache.activemq.command.Response;
import org.apache.activemq.command.SessionId;
import org.apache.activemq.command.SessionInfo;
import org.apache.activemq.transport.xmpp.command.Handler;
import org.apache.activemq.transport.xmpp.command.HandlerRegistry;
import org.apache.activemq.util.IdGenerator;
import org.apache.activemq.util.IntSequenceGenerator;
import org.apache.activemq.util.LongSequenceGenerator;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jabber.protocol.disco_info.Feature;
import org.jabber.protocol.disco_info.Identity;
import org.jabber.protocol.disco_items.Item;
import org.jabber.protocol.muc_user.X;

/**
 * TODO lots of this code could be shared with Stomp
 */
public class ProtocolConverter {
    private static final transient Log LOG = LogFactory.getLog(ProtocolConverter.class);
    private static final IdGenerator CONNECTION_ID_GENERATOR = new IdGenerator();
    private static final IdGenerator CLIENT_ID_GENERATOR = new IdGenerator("xmpp");

    private HandlerRegistry registry = new HandlerRegistry();
    private XmppTransport transport;

    private final ConnectionId connectionId = new ConnectionId(CONNECTION_ID_GENERATOR.generateId());
    private final SessionId sessionId = new SessionId(connectionId, -1);
    private final ProducerId producerId = new ProducerId(sessionId, 1);

    private final ConnectionInfo connectionInfo = new ConnectionInfo(connectionId);
    private final SessionInfo sessionInfo = new SessionInfo(sessionId);
    private final ProducerInfo producerInfo = new ProducerInfo(producerId);

    private final LongSequenceGenerator consumerIdGenerator = new LongSequenceGenerator();
    private final LongSequenceGenerator messageIdGenerator = new LongSequenceGenerator();
    private final IntSequenceGenerator tempDestinationIdGenerator = new IntSequenceGenerator();

    private final Map<Integer, Handler<Response>> responseHandlers = new ConcurrentHashMap<Integer, Handler<Response>>();
    private final Map<ConsumerId, Handler<MessageDispatch>> subscriptionsByConsumerId = new ConcurrentHashMap<ConsumerId, Handler<MessageDispatch>>();
    private final Map<String, ConsumerInfo> jidToConsumerMap = new HashMap<String, ConsumerInfo>();
    private final Map<String, ConsumerInfo> jidToInboxConsumerMap = new HashMap<String, ConsumerInfo>();

    private final Object commnadIdMutex = new Object();
    private int lastCommandId;
    private final AtomicBoolean connected = new AtomicBoolean(false);
    private ActiveMQTempQueue inboxDestination;

    //to avoid calling into sendToActiveMq from a handler
    private ScheduledThreadPoolExecutor scheduledThreadPoolExecutor = new ScheduledThreadPoolExecutor(1);

    public ProtocolConverter(XmppTransport transport) {
        this.transport = transport;
        initialiseRegistry();
    }

    protected int generateCommandId() {
        synchronized (commnadIdMutex) {
            return lastCommandId++;
        }
    }

    protected void initialiseRegistry() {
        // this kinda wiring muck is soooo much cleaner in C# :(
        registry.registerHandler(Message.class, new Handler<Message>() {
            public void handle(Message event) throws Exception {
                onMessage(event);
            }
        });
        registry.registerHandler(Auth.class, new Handler<Auth>() {
            public void handle(Auth event) throws Exception {
                onAuth(event);
            }
        });
        registry.registerHandler(Starttls.class, new Handler<Starttls>() {
            public void handle(Starttls event) throws Exception {
                onStarttls(event);
            }
        });
        registry.registerHandler(Iq.class, new Handler<Iq>() {
            public void handle(Iq event) throws Exception {
                onIq(event);
            }
        });
        registry.registerHandler(Presence.class, new Handler<Presence>() {
            public void handle(Presence event) throws Exception {
                onPresence(event);
            }
        });
    }

    public void onXmppCommand(Object command) throws Exception {
        // TODO we could do some nice code generation to boost performance
        // by autogenerating the bytecode to statically lookup a handler from a
        // registry maybe?

        Handler handler = registry.getHandler(command.getClass());
        if (handler == null) {
            unknownCommand(command);
        } else {
            handler.handle(command);
        }
    }

    public void onActiveMQCommand(Command command) throws Exception {
        if (command.isResponse()) {
            Response response = (Response)command;
            Handler<Response> handler = responseHandlers.remove(new Integer(response.getCorrelationId()));
            if (handler != null) {
                handler.handle(response);
            } else {
                LOG.warn("No handler for response: " + response);
            }
        } else if (command.isMessageDispatch()) {
            MessageDispatch md = (MessageDispatch)command;
            Handler<MessageDispatch> handler = subscriptionsByConsumerId.get(md.getConsumerId());
            if (handler != null) {
                handler.handle(md);
            } else {
                LOG.warn("No handler for message: " + md);
            }
        }
    }

    protected void unknownCommand(Object command) throws Exception {
        LOG.warn("Unkown command: " + command + " of type: " + command.getClass().getName());
    }

    protected void onIq(final Iq iq) throws Exception {
        Object any = iq.getAny();

        if (any instanceof Query) {
            onAuthQuery(any, iq);

        } else if (any instanceof jabber.iq._private.Query) {
            jabber.iq._private.Query query = (jabber.iq._private.Query)any;

            if (LOG.isDebugEnabled()) {
                LOG.debug("Iq Private " + debugString(iq) + " any: " + query.getAny());
            }

            Iq result = createResult(iq);
            jabber.iq._private.Query answer = new jabber.iq._private.Query();
            result.setAny(answer);
            transport.marshall(result);
        } else if (any instanceof jabber.iq.roster.Query) {
            jabber.iq.roster.Query query = (jabber.iq.roster.Query)any;

            if (LOG.isDebugEnabled()) {
                LOG.debug("Iq Roster " + debugString(iq) + " item: " + query.getItem());
            }

            Iq result = createResult(iq);
            jabber.iq.roster.Query roster = new jabber.iq.roster.Query();
            result.setAny(roster);
            transport.marshall(result);
        } else if (any instanceof org.jabber.protocol.disco_items.Query) {
            onDiscoItems(iq, (org.jabber.protocol.disco_items.Query)any);
        } else if (any instanceof org.jabber.protocol.disco_info.Query) {
            onDiscoInfo(iq, (org.jabber.protocol.disco_info.Query)any);
        } else {
            if (any instanceof Element) {
                Element element = (Element)any;
                LOG.warn("Iq Unknown " + debugString(iq) + " element namespace: " + element.getNamespaceURI() + " localName: " + element.getLocalName());
            } else {
                LOG.warn("Iq Unknown " + debugString(iq) + " any: " + any + " of type: " + any.getClass().getName());
            }
            Iq result = createResult(iq);
            jabber.client.Error error = new Error();
            error.setUnexpectedRequest("Don't understand: " + any.toString());
            result.setAny(error);
            transport.marshall(result);
        }
    }

    protected void onAuthQuery(Object any, final Iq iq) throws IOException, JMSException {
        Query query = (Query)any;
        if (LOG.isDebugEnabled()) {
            LOG.debug("Iq Auth Query " + debugString(iq) + " resource: " + query.getResource() + " username: " + query.getUsername());
        }
        if (query.getPassword() == null) {
            Iq result = createResult(iq);
            Query required = new Query();
            required.setPassword("");
            required.setUsername("");
            result.setAny(required);
            transport.marshall(result);
            return;
        }

        // connectionInfo.setClientId(query.getResource());
        connectionInfo.setUserName(query.getUsername());
        connectionInfo.setPassword(query.getPassword());

        // TODO support digest?

        if (connectionInfo.getClientId() == null) {
            connectionInfo.setClientId(CLIENT_ID_GENERATOR.generateId());
        }

        sendToActiveMQ(connectionInfo, new Handler<Response>() {
            public void handle(Response response) throws Exception {

                Iq result = createResult(iq);

                if (response instanceof ExceptionResponse) {
                    ExceptionResponse exceptionResponse = (ExceptionResponse)response;
                    Throwable exception = exceptionResponse.getException();

                    LOG.warn("Failed to create connection: " + exception, exception);

                    Error error = new Error();
                    result.setError(error);

                    StringWriter buffer = new StringWriter();
                    exception.printStackTrace(new PrintWriter(buffer));
                    error.setInternalServerError(buffer.toString());
                } else {
                    connected.set(true);
                }
                transport.marshall(result);

                sendToActiveMQ(sessionInfo, createErrorHandler("create sesssion"));
                sendToActiveMQ(producerInfo, createErrorHandler("create producer"));
            }
        });

        // create a destination for this client
        final String to = query.getUsername();
        createDestination(to);
    }

    public void createDestination(String to) throws IOException, JMSException {
        ActiveMQDestination destination = createActiveMQDestination(to);
        if (destination == null) {
            LOG.debug("Unable to create destination for " + to);
            return;
        }
        subscribe(to, destination, jidToConsumerMap);

        // lets subscribe to a personal inbox for replies

        // Check if Destination info is of temporary type.
        if (inboxDestination == null) {
            inboxDestination = new ActiveMQTempQueue(connectionInfo.getConnectionId(), tempDestinationIdGenerator.getNextSequenceId());

            DestinationInfo info = new DestinationInfo();
            info.setConnectionId(connectionInfo.getConnectionId());
            info.setOperationType(DestinationInfo.ADD_OPERATION_TYPE);
            info.setDestination(inboxDestination);
            sendToActiveMQ(info, null);

            subscribe(to, inboxDestination, jidToInboxConsumerMap);
        }
    }

    protected String debugString(Iq iq) {
        return "to: " + iq.getTo() + " type: " + iq.getType() + " from: " + iq.getFrom() + " id: " + iq.getId();
    }

    protected void onDiscoItems(Iq iq, org.jabber.protocol.disco_items.Query query) throws IOException {
        String to = iq.getTo();

        if (LOG.isDebugEnabled()) {
            LOG.debug("Iq Disco Items query " + debugString(iq) + " node: " + query.getNode() + " item: " + query.getItem());
        }

        Iq result = createResult(iq);
        org.jabber.protocol.disco_items.Query answer = new org.jabber.protocol.disco_items.Query();
        if (to == null || to.length() == 0) {
            answer.getItem().add(createItem("queues", "Queues", "queues"));
            answer.getItem().add(createItem("topics", "Topics", "topics"));
        } else {
            // lets not add anything?
        }

        result.setAny(answer);
        transport.marshall(result);
    }

    protected void onDiscoInfo(Iq iq, org.jabber.protocol.disco_info.Query query) throws IOException {
        String to = iq.getTo();

        // TODO lets create the topic 'to'

        if (LOG.isDebugEnabled()) {
            LOG.debug("Iq Disco Info query " + debugString(iq) + " node: " + query.getNode() + " features: " + query.getFeature() + " identity: " + query.getIdentity());
        }

        Iq result = createResult(iq);
        org.jabber.protocol.disco_info.Query answer = new org.jabber.protocol.disco_info.Query();
        answer.setNode(to);
        answer.getFeature().add(createFeature("http://jabber.org/protocol/disco#info"));
        answer.getFeature().add(createFeature("http://jabber.org/protocol/disco#items"));
        if (to == null || to.length() == 0) {
            answer.getIdentity().add(createIdentity("directory", "chatroom", "queues"));
            answer.getIdentity().add(createIdentity("directory", "chatroom", "topics"));
            /*
             * answer.getIdentity().add(createIdentity("hierarchy", "queues",
             * "branch")); answer.getIdentity().add(createIdentity("hierarchy",
             * "topics", "branch"));
             */
        } else {
            // for queues/topics
            if (to.equals("queues")) {
                answer.getIdentity().add(createIdentity("conference", "queue.a", "text"));
                answer.getIdentity().add(createIdentity("conference", "queue.b", "text"));
            } else if (to.equals("topics")) {
                answer.getIdentity().add(createIdentity("conference", "topic.x", "text"));
                answer.getIdentity().add(createIdentity("conference", "topic.y", "text"));
                answer.getIdentity().add(createIdentity("conference", "topic.z", "text"));
            } else {
                // lets reply to an actual room
                answer.getIdentity().add(createIdentity("conference", to, "text"));
                answer.getFeature().add(createFeature("http://jabber.org/protocol/muc"));
                answer.getFeature().add(createFeature("muc-open"));
            }
        }

        result.setAny(answer);
        transport.marshall(result);
    }

    protected void onPresence(Presence presence) throws IOException, JMSException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Presence: " + presence.getFrom() + " id: " + presence.getId() + " to: " + presence.getTo() + " type: " + presence.getType() + " showOrStatusOrPriority: "
                      + presence.getShowOrStatusOrPriority() + " any: " + presence.getAny());
        }
        org.jabber.protocol.muc_user.Item item = new org.jabber.protocol.muc_user.Item();
        item.setAffiliation("owner");
        item.setRole("moderator");
        item.setNick("broker");
        sendPresence(presence, item);

        /*
         * item = new org.jabber.protocol.muc_user.Item();
         * item.setAffiliation("admin"); item.setRole("moderator");
         * sendPresence(presence, item);
         */

        // lets create a subscription for the room, Jabber clients would use
        // "room/nickname", so we need to strip off the nickname
        String to = presence.getTo();
        if ( to != null ) {
            to = to.substring(0, to.indexOf("/"));
        }
        createDestination(to);
    }

    protected void subscribe(final String to, ActiveMQDestination destination, Map<String, ConsumerInfo> consumerMap) {
        boolean createConsumer = false;
        ConsumerInfo consumerInfo = null;
        synchronized (consumerMap) {
            consumerInfo = consumerMap.get(to);
            if (consumerInfo == null) {
                consumerInfo = new ConsumerInfo();
                consumerMap.put(to, consumerInfo);

                ConsumerId consumerId = new ConsumerId(sessionId, consumerIdGenerator.getNextSequenceId());
                consumerInfo.setConsumerId(consumerId);
                consumerInfo.setPrefetchSize(10);
                consumerInfo.setNoLocal(true);
                createConsumer = true;
            }
        }
        if (!createConsumer) {
            return;
        }

        consumerInfo.setDestination(destination);

        subscriptionsByConsumerId.put(consumerInfo.getConsumerId(), new Handler<MessageDispatch>() {
            public void handle(final MessageDispatch messageDispatch) throws Exception {
                // processing the inbound message
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Receiving inbound: " + messageDispatch.getMessage());
                }

                // lets send back an ACK
                final MessageAck ack = new MessageAck(messageDispatch, MessageAck.STANDARD_ACK_TYPE, 1);

                FutureTask<Void> task = new FutureTask<Void>(new Callable<Void>() {
                    public Void call() {
                        sendToActiveMQ(ack, createErrorHandler("Ack of message: " + messageDispatch.getMessage().getMessageId()));
                        return null;
                    }
                });

                scheduledThreadPoolExecutor.submit(task);

                Message message = createXmppMessage(to, messageDispatch);
                if (message != null) {
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("Sending message to XMPP client from: " + message.getFrom() + " to: " + message.getTo() + " type: " + message.getType() + " with body: " + message.getAny());
                    }
                    transport.marshall(message);
                }
            }
        });
        sendToActiveMQ(consumerInfo, createErrorHandler("subscribe to destination: " + destination));
    }

    protected Message createXmppMessage(String to, MessageDispatch messageDispatch) throws IOException, JMSException {
        
        org.apache.activemq.command.Message message = messageDispatch.getMessage();

        Message answer = new Message();
        String from = (String)message.getProperty("XMPPFrom");
        if ( from == null ) {
            from = to;
            int idx = from.indexOf('/');
            if (idx > 0) {
                from = from.substring(0, idx) + "/broker";
            }
            answer.setType("groupchat");
        } else {
            answer.setType("chat");
        }
        LOG.debug("Sending message from " + from + " and to " + to);
        answer.setFrom(from);
        answer.setTo(to);

        // answer.setType(message.getType());
        if (message instanceof ActiveMQTextMessage) {
            ActiveMQTextMessage activeMQTextMessage = (ActiveMQTextMessage)message;
            Body body = new Body();
            String text = activeMQTextMessage.getText();
            LOG.info("Setting the body text to be: " + text);
            body.setValue(text);
            answer.getAny().add(body);
        } else {
            // TODO support other message types
            LOG.warn("Could not convert the message to a complete Jabber message: " + message);
        }
        return answer;
    }

    protected void sendPresence(Presence presence, org.jabber.protocol.muc_user.Item item) throws IOException {
        Presence answer = new Presence();
        answer.setFrom(presence.getTo());
        answer.setType(presence.getType());
        answer.setTo(presence.getFrom());
        X x = new X();
        x.getDeclineOrDestroyOrInvite().add(item);
        answer.getShowOrStatusOrPriority().add(x);
        transport.marshall(answer);
    }

    protected Item createItem(String jid, String name, String node) {
        Item answer = new Item();
        answer.setJid(jid);
        answer.setName(name);
        answer.setNode(node);
        return answer;
    }

    protected Identity createIdentity(String category, String type, String name) {
        Identity answer = new Identity();
        answer.setCategory(category);
        answer.setName(name);
        answer.setType(type);
        return answer;
    }

    protected Feature createFeature(String var) {
        Feature feature = new Feature();
        feature.setVar(var);
        return feature;
    }

    /**
     * Creates a result command from the input
     */
    protected Iq createResult(Iq iq) {
        Iq result = new Iq();
        result.setId(iq.getId());
        result.setFrom(transport.getFrom());
        result.setTo(iq.getFrom());
        result.setLang(iq.getLang());
        result.setType("result");
        return result;
    }

    protected void sendToActiveMQ(Command command, Handler<Response> handler) {
        command.setCommandId(generateCommandId());
        if (handler != null) {
            command.setResponseRequired(true);
            responseHandlers.put(command.getCommandId(), handler);
        }
        transport.getTransportListener().onCommand(command);
    }

    protected void onStarttls(Starttls starttls) throws Exception {
        LOG.debug("Starttls");
        transport.marshall(new Proceed());
    }

    protected void onMessage(Message message) throws Exception {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Message from: " + message.getFrom() + " to: " + message.getTo() + " subjectOrBodyOrThread: " + message.getSubjectOrBodyOrThread());
        }

        final ActiveMQMessage activeMQMessage = createActiveMQMessage(message);

        ActiveMQDestination destination = createActiveMQDestination(message.getTo());

        activeMQMessage.setMessageId(new MessageId(producerInfo, messageIdGenerator.getNextSequenceId()));
        activeMQMessage.setDestination(destination);
        activeMQMessage.setProducerId(producerId);
        activeMQMessage.setTimestamp(System.currentTimeMillis());
        addActiveMQMessageHeaders(activeMQMessage, message);

        /*
         * MessageDispatch dispatch = new MessageDispatch();
         * dispatch.setDestination(destination);
         * dispatch.setMessage(activeMQMessage);
         */

        if (LOG.isDebugEnabled()) {
            LOG.debug("Sending ActiveMQ message: " + activeMQMessage);
        }
        sendToActiveMQ(activeMQMessage, createErrorHandler("send message"));
    }

    protected Handler<Response> createErrorHandler(final String text) {
        return new Handler<Response>() {
            public void handle(Response event) throws Exception {
                if (event instanceof ExceptionResponse) {
                    ExceptionResponse exceptionResponse = (ExceptionResponse)event;
                    Throwable exception = exceptionResponse.getException();
                    LOG.error("Failed to " + text + ". Reason: " + exception, exception);
                } else if (LOG.isDebugEnabled()) {
                    LOG.debug("Completed " + text);
                }
            }
        };
    }

    /**
     * Converts the Jabber destination name into a destination in ActiveMQ
     */
    protected ActiveMQDestination createActiveMQDestination(String jabberDestination) throws JMSException {
        if (jabberDestination == null) {
            return null;
        }
        String name = jabberDestination;
        int idx = jabberDestination.indexOf('@');
        if (idx > 0) {
            name = name.substring(0, idx);
        }
        // lets support lower-case versions of the agent topic
        if (name.equalsIgnoreCase(AdvisorySupport.AGENT_TOPIC)) {
            name = AdvisorySupport.AGENT_TOPIC;
        }
        return new ActiveMQTopic(name);
    }

    protected ActiveMQMessage createActiveMQMessage(Message message) throws JMSException {
        ActiveMQTextMessage answer = new ActiveMQTextMessage();
        String text = "";
        List<Object> list = message.getSubjectOrBodyOrThread();
        for (Object object : list) {
            if (object instanceof Body) {
                Body body = (Body)object;
                text = body.getValue();
                break;
            }
        }
        answer.setText(text);
        return answer;
    }

    protected void addActiveMQMessageHeaders(ActiveMQMessage answer, Message message) throws JMSException {
        answer.setStringProperty("XMPPFrom", message.getFrom());
        answer.setStringProperty("XMPPID", message.getId());
        answer.setStringProperty("XMPPLang", message.getLang());
        answer.setStringProperty("XMPPTo", message.getTo());
        answer.setJMSType(message.getType());
        ActiveMQDestination replyTo = createActiveMQDestination(message.getFrom());
        if (replyTo == null) {
            replyTo = inboxDestination;
        }
        LOG.info("Setting reply to destination to: " + replyTo);
        answer.setJMSReplyTo(replyTo);
    }

    protected void onAuth(Auth auth) throws Exception {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Auth mechanism: " + auth.getMechanism() + " value: " + auth.getValue());
        }
        String value = createChallengeValue(auth);
        if (value != null) {
            Challenge challenge = new Challenge();
            challenge.setValue(value);
            transport.marshall(challenge);
        } else {
            transport.marshall(new Success());
        }
    }

    protected String createChallengeValue(Auth auth) {
        // TODO implement the challenge
        return null;
    }

}
