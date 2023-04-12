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
package org.apache.activemq.web;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.ObjectMessage;
import javax.jms.TextMessage;
import javax.servlet.ServletConfig;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.HttpSession;
import org.apache.activemq.MessageAvailableConsumer;
import org.apache.activemq.web.async.AsyncServletRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A servlet for sending and receiving messages to/from JMS destinations using
 * HTTP POST for sending and HTTP GET for receiving. <p/> You can specify the
 * destination and whether it is a topic or queue via configuration details on
 * the servlet or as request parameters. <p/> For reading messages you can
 * specify a readTimeout parameter to determine how long the servlet should
 * block for. The servlet can be configured with the following init parameters:
 * <dl>
 * <dt>defaultReadTimeout</dt>
 * <dd>The default time in ms to wait for messages. May be overridden by a
 * request using the 'timeout' parameter</dd>
 * <dt>maximumReadTimeout</dt>
 * <dd>The maximum value a request may specify for the 'timeout' parameter</dd>
 * <dt>maximumMessages</dt>
 * <dd>maximum messages to send per response</dd>
 * <dt></dt>
 * <dd></dd>
 * </dl>
 *
 *
 */
@SuppressWarnings("serial")
public class MessageListenerServlet extends MessageServletSupport {
    private static final Logger LOG = LoggerFactory.getLogger(MessageListenerServlet.class);

    private final String readTimeoutParameter = "timeout";
    private long defaultReadTimeout = -1;
    private long maximumReadTimeout = 25000;
    private int maximumMessages = 100;
    private final Timer clientCleanupTimer = new Timer("ActiveMQ Ajax Client Cleanup Timer", true);
    private final HashMap<String,AjaxWebClient> ajaxWebClients = new HashMap<String,AjaxWebClient>();

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
        name = servletConfig.getInitParameter("maximumMessages");
        if (name != null) {
            maximumMessages = (int)asLong(name);
        }
        clientCleanupTimer.schedule( new ClientCleaner(), 5000, 60000 );
    }

    /**
     * Sends a message to a destination or manage subscriptions. If the the
     * content type of the POST is
     * <code>application/x-www-form-urlencoded</code>, then the form
     * parameters "destination", "message" and "type" are used to pass a message
     * or a subscription. If multiple messages or subscriptions are passed in a
     * single post, then additional parameters are shortened to "dN", "mN" and
     * "tN" where N is an index starting from 1. The type is either "send",
     * "listen" or "unlisten". For send types, the message is the text of the
     * TextMessage, otherwise it is the ID to be used for the subscription. If
     * the content type is not <code>application/x-www-form-urlencoded</code>,
     * then the body of the post is sent as the message to a destination that is
     * derived from a query parameter, the URL or the default destination.
     *
     * @param request
     * @param response
     * @throws ServletException
     * @throws IOException
     */
    @Override
    protected void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {

        // lets turn the HTTP post into a JMS Message
        AjaxWebClient client = getAjaxWebClient( request );
        String messageIds = "";

        synchronized (client) {

            if (LOG.isDebugEnabled()) {
                LOG.debug("POST client=" + client + " session=" + request.getSession().getId() + " clientId="+ request.getParameter("clientId") + " info=" + request.getPathInfo() + " contentType=" + request.getContentType());
                // dump(request.getParameterMap());
            }

            int messages = 0;

            // loop until no more messages
            while (true) {
                // Get the message parameters. Multiple messages are encoded
                // with more compact parameter names.
                String destinationName = request.getParameter(messages == 0 ? "destination" : ("d" + messages));

                if (destinationName == null) {
                    destinationName = request.getHeader("destination");
                }

                String message = request.getParameter(messages == 0 ? "message" : ("m" + messages));
                String type = request.getParameter(messages == 0 ? "type" : ("t" + messages));

                if (destinationName == null || message == null || type == null) {
                    break;
                }

                try {
                    Destination destination = getDestination(client, request, destinationName);

                    if (LOG.isDebugEnabled()) {
                        LOG.debug(messages + " destination=" + destinationName + " message=" + message + " type=" + type);
                        LOG.debug(destination + " is a " + destination.getClass().getName());
                    }

                    messages++;

                    if ("listen".equals(type)) {
                        AjaxListener listener = client.getListener();
                        Map<MessageAvailableConsumer, String> consumerIdMap = client.getIdMap();
                        Map<MessageAvailableConsumer, String> consumerDestinationNameMap = client.getDestinationNameMap();
                        client.closeConsumer(destination); // drop any existing
                        // consumer.
                        MessageAvailableConsumer consumer = (MessageAvailableConsumer)client.getConsumer(destination, request.getHeader(WebClient.selectorName));

                        consumer.setAvailableListener(listener);
                        consumerIdMap.put(consumer, message);
                        consumerDestinationNameMap.put(consumer, destinationName);
                        if (LOG.isDebugEnabled()) {
                            LOG.debug("Subscribed: " + consumer + " to " + destination + " id=" + message);
                        }
                    } else if ("unlisten".equals(type)) {
                        Map<MessageAvailableConsumer, String> consumerIdMap = client.getIdMap();
                        Map<MessageAvailableConsumer, String> consumerDestinationNameMap = client.getDestinationNameMap();
                        MessageAvailableConsumer consumer = (MessageAvailableConsumer)client.getConsumer(destination, request.getHeader(WebClient.selectorName));

                        consumer.setAvailableListener(null);
                        consumerIdMap.remove(consumer);
                        consumerDestinationNameMap.remove(consumer);
                        client.closeConsumer(destination);
                        if (LOG.isDebugEnabled()) {
                            LOG.debug("Unsubscribed: " + consumer);
                        }
                    } else if ("send".equals(type)) {
                        TextMessage text = client.getSession().createTextMessage(message);
                        appendParametersToMessage(request, text);

                        client.send(destination, text);
                        messageIds += text.getJMSMessageID() + "\n";
                        if (LOG.isDebugEnabled()) {
                            LOG.debug("Sent " + message + " to " + destination);
                        }
                    } else {
                        LOG.warn("unknown type " + type);
                    }

                } catch (JMSException e) {
                    LOG.warn("jms", e);
                }
            }
        }

        if ("true".equals(request.getParameter("poll"))) {
            try {
                // TODO return message IDs
                doMessages(client, request, response);
            } catch (JMSException e) {
                throw new ServletException("JMS problem: " + e, e);
            }
        } else {
            // handle simple POST of a message
            if (request.getContentLength() != 0 && (request.getContentType() == null || !request.getContentType().toLowerCase().startsWith("application/x-www-form-urlencoded"))) {
                try {
                    Destination destination = getDestination(client, request);
                    String body = getPostedMessageBody(request);
                    TextMessage message = client.getSession().createTextMessage(body);
                    appendParametersToMessage(request, message);

                    client.send(destination, message);
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("Sent to destination: " + destination + " body: " + body);
                    }
                    messageIds += message.getJMSMessageID() + "\n";
                } catch (JMSException e) {
                    throw new ServletException(e);
                }
            }

            response.setContentType("text/plain");
            response.setHeader("Cache-Control", "no-cache");
            response.getWriter().print(messageIds);
        }
    }

    /**
     * Supports a HTTP DELETE to be equivlanent of consuming a singe message
     * from a queue
     */
    @Override
    protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
        try {
            AjaxWebClient client = getAjaxWebClient(request);
            if (LOG.isDebugEnabled()) {
                LOG.debug("GET client=" + client + " session=" + request.getSession().getId() + " clientId="+ request.getParameter("clientId") + " uri=" + request.getRequestURI() + " query=" + request.getQueryString());
            }

            doMessages(client, request, response);
        } catch (JMSException e) {
            throw new ServletException("JMS problem: " + e, e);
        }
    }

    /**
     * Reads a message from a destination up to some specific timeout period
     *
     * @param client The webclient
     * @param request
     * @param response
     * @throws ServletException
     * @throws IOException
     */
    protected void doMessages(AjaxWebClient client, HttpServletRequest request, HttpServletResponse response) throws JMSException, IOException {

        int messages = 0;
        // This is a poll for any messages

        long timeout = getReadTimeout(request);
        if (LOG.isDebugEnabled()) {
            LOG.debug("doMessage timeout=" + timeout);
        }

        // this is non-null if we're resuming the asyncRequest.
        // attributes set in AjaxListener
        UndeliveredAjaxMessage undelivered_message = null;
        Message message = null;
        undelivered_message = (UndeliveredAjaxMessage)request.getAttribute("undelivered_message");
        if( undelivered_message != null ) {
            message = undelivered_message.getMessage();
        }

        synchronized (client) {

            List<MessageConsumer> consumers = client.getConsumers();
            MessageAvailableConsumer consumer = null;
            if( undelivered_message != null ) {
                consumer = (MessageAvailableConsumer)undelivered_message.getConsumer();
            }

            if (message == null) {
                // Look for a message that is ready to go
                for (int i = 0; message == null && i < consumers.size(); i++) {
                    consumer = (MessageAvailableConsumer)consumers.get(i);
                    if (consumer.getAvailableListener() == null) {
                        continue;
                    }

                    // Look for any available messages
                    message = consumer.receive(10);
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("received " + message + " from " + consumer);
                    }
                }
            }

            // prepare the response
            response.setContentType("text/xml");
            response.setHeader("Cache-Control", "no-cache");

            if (message == null && client.getListener().getUndeliveredMessages().size() == 0) {
                final AsyncServletRequest asyncRequest = AsyncServletRequest.getAsyncRequest(request);

                if (asyncRequest.isExpired()) {
                    response.setStatus(HttpServletResponse.SC_OK);
                    StringWriter swriter = new StringWriter();
                    PrintWriter writer = new PrintWriter(swriter);
                    writer.println("<ajax-response>");
                    writer.print("</ajax-response>");

                    writer.flush();
                    String m = swriter.toString();
                    response.getWriter().println(m);

                    return;
                }

                asyncRequest.setTimeoutMs(timeout);
                asyncRequest.startAsync();
                LOG.debug("Suspending asyncRequest " + asyncRequest);

                // Fetch the listeners
                AjaxListener listener = client.getListener();
                listener.access();

                // register this asyncRequest with our listener.
                listener.setAsyncRequest(asyncRequest);

                return;
            }

            StringWriter swriter = new StringWriter();
            PrintWriter writer = new PrintWriter(swriter);

            Map<MessageAvailableConsumer, String> consumerIdMap = client.getIdMap();
            Map<MessageAvailableConsumer, String> consumerDestinationNameMap = client.getDestinationNameMap();
            response.setStatus(HttpServletResponse.SC_OK);
            writer.println("<ajax-response>");

            // Send any message we already have
            if (message != null) {
                String id = consumerIdMap.get(consumer);
                String destinationName = consumerDestinationNameMap.get(consumer);
                LOG.debug( "sending pre-existing message" );
                writeMessageResponse(writer, message, id, destinationName);

                messages++;
            }

            // send messages buffered while asyncRequest was unavailable.
            LinkedList<UndeliveredAjaxMessage> undeliveredMessages = ((AjaxListener)consumer.getAvailableListener()).getUndeliveredMessages();
            LOG.debug("Send " + undeliveredMessages.size() + " unconsumed messages");
            synchronized( undeliveredMessages ) {
                for (Iterator<UndeliveredAjaxMessage> it = undeliveredMessages.iterator(); it.hasNext();) {
                    messages++;
                    UndeliveredAjaxMessage undelivered = it.next();
                    Message msg = undelivered.getMessage();
                    consumer = (MessageAvailableConsumer)undelivered.getConsumer();
                    String id = consumerIdMap.get(consumer);
                    String destinationName = consumerDestinationNameMap.get(consumer);
                    LOG.debug( "sending undelivered/buffered messages" );
                    LOG.debug( "msg:" +msg+ ", id:" +id+ ", destinationName:" +destinationName);
                    writeMessageResponse(writer, msg, id, destinationName);
                    it.remove();
                    if (messages >= maximumMessages) {
                        break;
                    }
                }
            }

            // Send the rest of the messages
            for (int i = 0; i < consumers.size() && messages < maximumMessages; i++) {
                consumer = (MessageAvailableConsumer)consumers.get(i);
                if (consumer.getAvailableListener() == null) {
                    continue;
                }

                // Look for any available messages
                while (messages < maximumMessages) {
                    message = consumer.receiveNoWait();
                    if (message == null) {
                        break;
                    }
                    messages++;
                    String id = consumerIdMap.get(consumer);
                    String destinationName = consumerDestinationNameMap.get(consumer);
                    LOG.debug( "sending final available messages" );
                    writeMessageResponse(writer, message, id, destinationName);
                }
            }

            writer.print("</ajax-response>");

            writer.flush();
            String m = swriter.toString();
            response.getWriter().println(m);
        }
    }

    protected void writeMessageResponse(PrintWriter writer, Message message, String id, String destinationName) throws JMSException, IOException {
        writer.print("<response id='");
        writer.print(id);
        writer.print("'");
        if (destinationName != null) {
            writer.print(" destination='" + destinationName + "' ");
        }
        writer.print(">");
        if (message instanceof TextMessage) {
            TextMessage textMsg = (TextMessage)message;
            String txt = textMsg.getText();
            if (txt != null) {
                if (txt.startsWith("<?")) {
                    txt = txt.substring(txt.indexOf("?>") + 2);
                }
                writer.print(txt);
            }
        } else if (message instanceof ObjectMessage) {
            ObjectMessage objectMsg = (ObjectMessage)message;
            Object object = objectMsg.getObject();
            if (object != null) {
                writer.print(object.toString());
            }
        }
        writer.println("</response>");
    }

    /*
     * Return the AjaxWebClient for this session+clientId.
     * Create one if it does not already exist.
     */
    protected AjaxWebClient getAjaxWebClient( HttpServletRequest request ) {
        HttpSession session = request.getSession(true);

        String clientId = request.getParameter( "clientId" );
        // if user doesn't supply a 'clientId', we'll just use a default.
        if( clientId == null ) {
            clientId = "defaultAjaxWebClient";
        }
        String sessionKey = session.getId() + '-' + clientId;

        AjaxWebClient client = null;
        synchronized (ajaxWebClients) {
            client = ajaxWebClients.get( sessionKey );
            // create a new AjaxWebClient if one does not already exist for this sessionKey.
            if( client == null ) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug( "creating new AjaxWebClient in "+sessionKey );
                }
                client = new AjaxWebClient( request, maximumReadTimeout );
                ajaxWebClients.put( sessionKey, client );
            }
            client.updateLastAccessed();
        }
        return client;
    }

    /**
     * @return the timeout value for read requests which is always >= 0 and <=
     *         maximumReadTimeout to avoid DoS attacks
     */
    protected long getReadTimeout(HttpServletRequest request) {
        long answer = defaultReadTimeout;

        String name = request.getParameter(readTimeoutParameter);
        if (name != null) {
            answer = asLong(name);
        }
        if (answer < 0 || answer > maximumReadTimeout) {
            answer = maximumReadTimeout;
        }
        return answer;
    }

    /*
     * an instance of this class runs every minute (started in init), to clean up old web clients & free resources.
     */
    private class ClientCleaner extends TimerTask {
        @Override
        public void run() {
            if( LOG.isDebugEnabled() ) {
                LOG.debug( "Cleaning up expired web clients." );
            }

            synchronized( ajaxWebClients ) {
                Iterator<Map.Entry<String, AjaxWebClient>> it = ajaxWebClients.entrySet().iterator();
                while ( it.hasNext() ) {
                    Map.Entry<String,AjaxWebClient> e = it.next();
                    String key = e.getKey();
                    AjaxWebClient val = e.getValue();
                    if ( LOG.isDebugEnabled() ) {
                        LOG.debug( "AjaxWebClient " + key + " last accessed " + val.getMillisSinceLastAccessed()/1000 + " seconds ago." );
                    }
                    // close an expired client and remove it from the ajaxWebClients hash.
                    if( val.closeIfExpired() ) {
                        if ( LOG.isDebugEnabled() ) {
                            LOG.debug( "Removing expired AjaxWebClient " + key );
                        }
                        it.remove();
                    }
                }
            }
        }
    }

    @Override
    public void destroy() {
        // make sure we cancel the timer
        clientCleanupTimer.cancel();
        super.destroy();
    }
}
