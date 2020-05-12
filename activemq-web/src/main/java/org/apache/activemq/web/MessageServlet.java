/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.activemq.web;

import org.apache.activemq.MessageAvailableConsumer;
import org.apache.activemq.MessageAvailableListener;
import org.eclipse.jetty.continuation.Continuation;
import org.eclipse.jetty.continuation.ContinuationSupport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jms.*;
import javax.servlet.ServletConfig;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Enumeration;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * A servlet for sending and receiving messages to/from JMS destinations using
 * HTTP POST for sending and HTTP GET for receiving.
 * <p/>
 * You can specify the destination and whether it is a topic or queue via
 * configuration details on the servlet or as request parameters.
 * <p/>
 * For reading messages you can specify a readTimeout parameter to determine how
 * long the servlet should block for.
 *
 * One thing to keep in mind with this solution - due to the nature of REST,
 * there will always be a chance of losing messages. Consider what happens when
 * a message is retrieved from the broker but the web call is interrupted before
 * the client receives the message in the response - the message is lost.
 */
public class MessageServlet extends MessageServletSupport {

    // its a bit pita that this servlet got intermixed with jetty continuation/rest
    // instead of creating a special for that. We should have kept a simple servlet
    // for good old fashioned request/response blocked communication.

    private static final long serialVersionUID = 8737914695188481219L;

    private static final Logger LOG = LoggerFactory.getLogger(MessageServlet.class);

    private final String readTimeoutParameter = "readTimeout";
    private final String readTimeoutRequestAtt = "xamqReadDeadline";
    private final String oneShotParameter = "oneShot";
    private long defaultReadTimeout = -1;
    private long maximumReadTimeout = 20000;
    private long requestTimeout = 1000;
    private String defaultContentType;

    private final Map<String, WebClient> clients = new ConcurrentHashMap<>();
    private final Set<MessageAvailableConsumer> activeConsumers = ConcurrentHashMap.newKeySet();

    @Override
    public void init() throws ServletException {
        ServletConfig servletConfig = getServletConfig();
        String name = servletConfig.getInitParameter("defaultReadTimeout");
        if (name != null) {
            defaultReadTimeout = asLong(name);
        }
        name = servletConfig.getInitParameter("maximumReadTimeout");
        if (name != null) {
            maximumReadTimeout = asLong(name);
        }
        name = servletConfig.getInitParameter("replyTimeout");
        if (name != null) {
            requestTimeout = asLong(name);
        }
        name = servletConfig.getInitParameter("defaultContentType");
        if (name != null) {
            defaultContentType = name;
        }
    }

    /**
     * Sends a message to a destination
     *
     * @param request
     * @param response
     * @throws ServletException
     * @throws IOException
     */
    @Override
    protected void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
        // lets turn the HTTP post into a JMS Message
        try {

            String action = request.getParameter("action");
            String clientId = request.getParameter("clientId");
            if (clientId != null && "unsubscribe".equals(action)) {
                LOG.info("Unsubscribing client " + clientId);
                WebClient client = getWebClient(request);
                client.close();
                clients.remove(clientId);
                return;
            }

            WebClient client = getWebClient(request);

            String text = getPostedMessageBody(request);

            // lets create the destination from the URI?
            Destination destination = getDestination(client, request);
            if (destination == null) {
                throw new NoDestinationSuppliedException();
            }

            if (LOG.isDebugEnabled()) {
                LOG.debug("Sending message to: " + destination + " with text: " + text);
            }

            boolean sync = isSync(request);
            TextMessage message = client.getSession().createTextMessage(text);

            appendParametersToMessage(request, message);
            boolean persistent = isSendPersistent(request);
            int priority = getSendPriority(request);
            long timeToLive = getSendTimeToLive(request);
            client.send(destination, message, persistent, priority, timeToLive);

            // lets return a unique URI for reliable messaging
            response.setHeader("messageID", message.getJMSMessageID());
            response.setStatus(HttpServletResponse.SC_OK);
            response.getWriter().write("Message sent");

        } catch (JMSException e) {
            throw new ServletException("Could not post JMS message: " + e, e);
        }
    }

    /**
     * Supports a HTTP DELETE to be equivalent of consuming a single message
     * from a queue
     */
    @Override
    protected void doDelete(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
        doMessages(request, response);
    }

    /**
     * Supports a HTTP DELETE to be equivalent of consuming a single message
     * from a queue
     */
    @Override
    protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
        doMessages(request, response);
    }

    /**
     * Reads a message from a destination up to some specific timeout period
     *
     * @param request
     * @param response
     * @throws ServletException
     * @throws IOException
     */
    protected void doMessages(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
        MessageAvailableConsumer consumer = null;

        try {
            WebClient client = getWebClient(request);
            Destination destination = getDestination(client, request);
            if (destination == null) {
                throw new NoDestinationSuppliedException();
            }
            consumer = (MessageAvailableConsumer) client.getConsumer(destination, request.getHeader(WebClient.selectorName));
            Continuation continuation = ContinuationSupport.getContinuation(request);

            // Don't allow concurrent use of the consumer. Do make sure to allow
            // subsequent calls on continuation to use the consumer.
            if (continuation.isInitial() && !activeConsumers.add(consumer)) {
                throw new ServletException("Concurrent access to consumer is not supported");
            }

            Message message = null;

            long deadline = getReadDeadline(request);
            long timeout = deadline - System.currentTimeMillis();

            // Set the message available listener *before* calling receive to eliminate any
            // chance of a missed notification between the time receive() completes without
            // a message and the time the listener is set.
            synchronized (consumer) {
                Listener listener = (Listener) consumer.getAvailableListener();
                if (listener == null) {
                    listener = new Listener(consumer);
                    consumer.setAvailableListener(listener);
                }
            }

            if (LOG.isDebugEnabled()) {
                LOG.debug("Receiving message(s) from: " + destination + " with timeout: " + timeout);
            }

            // Look for any available messages (need a little timeout). Always
            // try at least one lookup; don't block past the deadline.
            if (timeout <= 0) {
                message = consumer.receiveNoWait();
            } else if (timeout < 10) {
                message = consumer.receive(timeout);
            } else {
                message = consumer.receive(10);
            }

            if (message == null) {
                handleContinuation(request, response, client, destination, consumer, deadline);
            } else {
                writeResponse(request, response, message);
                closeConsumerOnOneShot(request, client, destination);

                activeConsumers.remove(consumer);
            }
        } catch (JMSException e) {
            throw new ServletException("Could not post JMS message: " + e, e);
        }
    }

    protected void handleContinuation(HttpServletRequest request, HttpServletResponse response, WebClient client, Destination destination,
                                      MessageAvailableConsumer consumer, long deadline) {
        // Get an existing Continuation or create a new one if there are no events.
        Continuation continuation = ContinuationSupport.getContinuation(request);

        long timeout = deadline - System.currentTimeMillis();
        if ((continuation.isExpired()) || (timeout <= 0)) {
            // Reset the continuation on the available listener for the consumer to prevent the
            // next message receipt from being consumed without a valid, active continuation.
            synchronized (consumer) {
                Object obj = consumer.getAvailableListener();
                if (obj instanceof Listener) {
                    ((Listener) obj).setContinuation(null);
                }
            }
            response.setStatus(HttpServletResponse.SC_NO_CONTENT);
            closeConsumerOnOneShot(request, client, destination);
            activeConsumers.remove(consumer);
            return;
        }

        continuation.setTimeout(timeout);
        continuation.suspend();

        synchronized (consumer) {
            Listener listener = (Listener) consumer.getAvailableListener();

            // register this continuation with our listener.
            listener.setContinuation(continuation);
        }
    }

    protected void writeResponse(HttpServletRequest request, HttpServletResponse response, Message message) throws IOException, JMSException {
        int messages = 0;
        try {
            response.setHeader("Cache-Control", "no-cache, no-store, must-revalidate"); // HTTP
            // 1.1
            response.setHeader("Pragma", "no-cache"); // HTTP 1.0
            response.setDateHeader("Expires", 0);

            // Set content type as in request. This should be done before calling getWriter by specification
            String type = getContentType(request);

            if (type != null) {
                response.setContentType(type);
            } else {
                if (defaultContentType != null) {
                    response.setContentType(defaultContentType);
                } else if (isXmlContent(message)) {
                    response.setContentType("application/xml");
                } else {
                    response.setContentType("text/plain");
                }
            }

            // write a responds
            PrintWriter writer = response.getWriter();

            // handle any message(s)
            if (message == null) {
                // No messages so OK response of for ajax else no content.
                response.setStatus(HttpServletResponse.SC_NO_CONTENT);
            } else {
                // We have at least one message so set up the response
                messages = 1;

                response.setStatus(HttpServletResponse.SC_OK);

                setResponseHeaders(response, message);
                writeMessageResponse(writer, message);
                writer.flush();
            }
        } finally {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Received " + messages + " message(s)");
            }
        }
    }

    protected void writeMessageResponse(PrintWriter writer, Message message) throws JMSException, IOException {
        if (message instanceof TextMessage) {
            TextMessage textMsg = (TextMessage) message;
            String txt = textMsg.getText();
            if (txt != null) {
                if (txt.startsWith("<?")) {
                    txt = txt.substring(txt.indexOf("?>") + 2);
                }
                writer.print(txt);
            }
        } else if (message instanceof ObjectMessage) {
            ObjectMessage objectMsg = (ObjectMessage) message;
            Object object = objectMsg.getObject();
            if (object != null) {
                writer.print(object.toString());
            }
        }
    }

    protected boolean isXmlContent(Message message) throws JMSException {
        if (message instanceof TextMessage) {
            TextMessage textMsg = (TextMessage) message;
            String txt = textMsg.getText();
            if (txt != null) {
                // assume its xml when it starts with <
                if (txt.startsWith("<")) {
                    return true;
                }
            }
        }
        // for any other kind of messages we dont assume xml
        return false;
    }

    public WebClient getWebClient(HttpServletRequest request) {
        String clientId = request.getParameter("clientId");
        if (clientId != null) {
            LOG.debug("Getting local client [" + clientId + "]");
            return clients.computeIfAbsent(clientId, k -> new WebClient());
        } else {
            return WebClient.getWebClient(request);
        }
    }

    protected String getContentType(HttpServletRequest request) {
        String value = request.getParameter("xml");
        if (value != null && "true".equalsIgnoreCase(value)) {
            return "application/xml";
        }
        value = request.getParameter("json");
        if (value != null && "true".equalsIgnoreCase(value)) {
            return "application/json";
        }
        return null;
    }

    @SuppressWarnings("rawtypes")
    protected void setResponseHeaders(HttpServletResponse response, Message message) throws JMSException {
        response.setHeader("destination", message.getJMSDestination().toString());
        response.setHeader("id", message.getJMSMessageID());

        // Return JMS properties as header values.
        for (Enumeration names = message.getPropertyNames(); names.hasMoreElements(); ) {
            String name = (String) names.nextElement();
            response.setHeader(name, message.getObjectProperty(name).toString());
        }
    }

    /**
     * @return the timeout value for read requests which is always >= 0 and <=
     *         maximumReadTimeout to avoid DoS attacks
     */
    protected long getReadDeadline(HttpServletRequest request) {
        Long answer;

        answer = (Long) request.getAttribute(readTimeoutRequestAtt);

        if (answer == null) {
            long timeout = defaultReadTimeout;
            String name = request.getParameter(readTimeoutParameter);
            if (name != null) {
                timeout = asLong(name);
            }
            if (timeout < 0 || timeout > maximumReadTimeout) {
                timeout = maximumReadTimeout;
            }

            answer = Long.valueOf(System.currentTimeMillis() + timeout);
        }
        return answer.longValue();
    }

    /**
     * Close the consumer if one-shot mode is used on the given request.
     */
    protected void closeConsumerOnOneShot(HttpServletRequest request, WebClient client, Destination dest) {
        if (asBoolean(request.getParameter(oneShotParameter), false)) {
            try {
                client.closeConsumer(dest);
            } catch (JMSException jms_exc) {
                LOG.warn("JMS exception on closing consumer after request with one-shot mode", jms_exc);
            }
        }
    }

    /*
     * Listen for available messages and wakeup any continuations.
     */
    private static class Listener implements MessageAvailableListener {
        MessageConsumer consumer;
        Continuation continuation;

        Listener(MessageConsumer consumer) {
            this.consumer = consumer;
        }

        public void setContinuation(Continuation continuation) {
            synchronized (consumer) {
                this.continuation = continuation;
            }
        }

        @Override
        public void onMessageAvailable(MessageConsumer consumer) {
            assert this.consumer == consumer;

            ((MessageAvailableConsumer) consumer).setAvailableListener(null);

            synchronized (this.consumer) {
                if (continuation != null) {
                    continuation.resume();
                }
            }
        }
    }
}
