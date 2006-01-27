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

package org.apache.activemq.web;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

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
import org.apache.activemq.MessageAvailableListener;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.mortbay.util.ajax.Continuation;
import org.mortbay.util.ajax.ContinuationSupport;

/**
 * A servlet for sending and receiving messages to/from JMS destinations using
 * HTTP POST for sending and HTTP GET for receiving. <p/> You can specify the
 * destination and whether it is a topic or queue via configuration details on
 * the servlet or as request parameters. <p/> For reading messages you can
 * specify a readTimeout parameter to determine how long the servlet should
 * block for.
 * 
 * @version $Revision: 1.1.1.1 $
 */
public class MessageListenerServlet extends MessageServletSupport {
    private static final Log log = LogFactory.getLog(MessageListenerServlet.class);

    private String readTimeoutParameter = "timeout";

    private long defaultReadTimeout = -1;

    private long maximumReadTimeout = 20000;

    private int maximumMessages = 100;

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
            maximumMessages = (int) asLong(name);
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
    protected void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
        // lets turn the HTTP post into a JMS Message
        try {
            WebClient client = getWebClient(request);
            // System.err.println("POST client="+client+"
            // session="+request.getSession().getId()+"
            // info="+request.getPathInfo()+" query="+request.getQueryString());

            String text = getPostedMessageBody(request);

            // lets create the destination from the URI?
            Destination destination = getDestination(client, request);
            if (destination == null)
                throw new NoDestinationSuppliedException();

            if (log.isDebugEnabled()) {
                log.debug("Sending message to: " + destination + " with text: " + text);
            }

            TextMessage message = client.getSession().createTextMessage(text);
            appendParametersToMessage(request, message);
            client.send(destination, message);

            // lets return a unique URI for reliable messaging
            response.setStatus(HttpServletResponse.SC_NO_CONTENT);
            // System.err.println("Sent "+message+" to "+destination);
            /*
             * StringWriter swriter = new StringWriter(); PrintWriter writer =
             * new PrintWriter(swriter);
             * 
             * writer.println("<ajax-response><response type='object'
             * id='amqSend'><amq jmsid='"+message.getJMSMessageID()+"'/></response></ajax-response>");
             * writer.flush(); String m=swriter.toString();
             * System.err.println(m); response.getWriter().write(m);
             * response.getWriter().flush();
             */
        } catch (JMSException e) {
            throw new ServletException("Could not post JMS message: " + e, e);
        }
    }

    /**
     * Supports a HTTP DELETE to be equivlanent of consuming a singe message
     * from a queue
     */
    protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {

        try {
            WebClient client = getWebClient(request);
            // System.err.println("GET client="+client+"
            // session="+request.getSession().getId()+"
            // info="+request.getPathInfo()+" query="+request.getQueryString());

            Destination destination = getDestination(client, request);
            if (destination != null)
                doSubscription(client, destination, request, response);
            else
                doMessages(client, request, response);
        }

        catch (JMSException e) {
            throw new ServletException("JMS problem: " + e, e);
        }

    }

    /**
     * Reads a message from a destination up to some specific timeout period
     * 
     * @param client
     *            The webclient
     * @param request
     * @param response
     * @throws ServletException
     * @throws IOException
     */
    protected void doMessages(WebClient client, HttpServletRequest request, HttpServletResponse response) throws JMSException, IOException {

        int messages = 0;
        // This is a poll for any messages

        long timeout = getReadTimeout(request);

        Continuation continuation = null;
        Message message = null;

        Listener listener = getListener(request);

        synchronized (client) {

            List consumers = client.getConsumers();
            MessageAvailableConsumer consumer = null;

            // Look for a message that is ready to go
            for (int i = 0; message == null && i < consumers.size(); i++) {
                consumer = (MessageAvailableConsumer) consumers.get(i);
                if (consumer.getAvailableListener() == null)
                    continue;

                // Look for any available messages
                message = consumer.receiveNoWait();
                // System.err.println("received "+message+" from "+consumer);
            }

            // Get an existing Continuation or create a new one if there are no
            // messages

            if (message == null) {
                continuation = ContinuationSupport.getContinuation(request, client);

                // register this continuation with our listener.
                listener.setContinuation(continuation);

                // Get the continuation object (may wait and/or retry
                // request here).
                continuation.suspend(timeout);
            }

            // prepare the responds
            response.setContentType("text/xml");

            StringWriter swriter = new StringWriter();
            PrintWriter writer = new PrintWriter(swriter);

            Map consumerIdMap = getConsumerIdMap(request);
            response.setStatus(HttpServletResponse.SC_OK);
            writer.println("<ajax-response>");

            // Send any message we already have
            if (message != null) {
                String id = (String) consumerIdMap.get(consumer);
                writer.print("<response type='object' id='");
                writer.print(id);
                writer.print("'>\n");
                writeMessageResponse(writer, message);
                writer.println("</response>");
                messages++;
            }

            // Send the rest of the messages
            for (int i = 0; i < consumers.size() && messages < maximumMessages; i++) {
                consumer = (MessageAvailableConsumer) consumers.get(i);
                if (consumer.getAvailableListener() == null)
                    continue;

                // Look for any available messages
                message = consumer.receiveNoWait();
                // System.err.println("received "+message+" from "+consumer);
                while (message != null && messages < maximumMessages) {
                    Destination destination = message.getJMSDestination();
                    String id = (String) consumerIdMap.get(consumer);
                    writer.print("<response type='object' id='");
                    writer.print(id);
                    writer.print("'>");
                    writeMessageResponse(writer, message);
                    writer.println("</response>");
                    messages++;
                    message = consumer.receiveNoWait();
                }
            }

            // Add poll message
            writer.println("<response type='object' id='amqPoll'><ok/></response>");
            writer.println("</ajax-response>");

            writer.flush();
            String m = swriter.toString();
            // System.err.println(m);
            response.getWriter().write(m);
        }

    }

    /**
     * Subscribe or unsubscribe to a destination. The listen request parameter
     * is used to indicate subscribe (tree) or unsubscribe (false).
     * 
     * @param request
     * @param response
     * @throws ServletException
     * @throws IOException
     */
    protected void doSubscription(WebClient client, Destination destination, HttpServletRequest request, HttpServletResponse response) throws JMSException, ServletException, IOException {

        String s = request.getParameter("listen");
        if (s == null || s.length() == 0) {
            log.warn("No listen paramenter for subscribe");
            response.sendError(HttpServletResponse.SC_BAD_REQUEST, "No listen parameter");
            return;
        }
        boolean listen = Boolean.valueOf(s).booleanValue();

        String id = request.getParameter("id");
        if (listen && (id == null || id.length() == 0)) {
            log.warn("No id paramenter for subscribe");
            response.sendError(HttpServletResponse.SC_BAD_REQUEST, "No id parameter");
            return;
        }

        Listener listener = getListener(request);
        Map consumerIdMap = getConsumerIdMap(request);

        synchronized (client) {
            MessageAvailableConsumer consumer = (MessageAvailableConsumer) client.getConsumer(destination);

            if (listen) {
                consumer.setAvailableListener(listener);
                consumerIdMap.put(consumer, id);
                // System.err.println("Subscribed: "+consumer+" to
                // "+destination);
            } else {
                // TODO should we destroy consumer on unsubscribe?
                consumer.setAvailableListener(null);
                consumerIdMap.remove(consumer);
                // System.err.println("Unsubscribed: "+consumer);
            }
        }

        response.setStatus(HttpServletResponse.SC_NO_CONTENT);
    }

    protected void writeMessageResponse(PrintWriter writer, Message message) throws JMSException, IOException {
        if (message instanceof TextMessage) {
            TextMessage textMsg = (TextMessage) message;
            writer.print(textMsg.getText());
        } else if (message instanceof ObjectMessage) {
            ObjectMessage objectMsg = (ObjectMessage) message;
            Object object = objectMsg.getObject();
            writer.print(object.toString());
        }
    }

    protected Listener getListener(HttpServletRequest request) {
        HttpSession session = request.getSession();
        Listener listener = (Listener) session.getAttribute("mls.listener");
        if (listener == null) {
            listener = new Listener(getWebClient(request));
            session.setAttribute("mls.listener", listener);
        }
        return listener;
    }

    protected Map getConsumerIdMap(HttpServletRequest request) {
        HttpSession session = request.getSession();
        Map map = (Map) session.getAttribute("mls.consumerIdMap");
        if (map == null) {
            map = new HashMap();
            session.setAttribute("mls.consumerIdMap", map);
        }
        return map;
    }

    protected boolean isRicoAjax(HttpServletRequest request) {
        String rico = request.getParameter("rico");
        return rico != null && rico.equals("true");
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
     * Listen for available messages and wakeup any continuations.
     */
    private class Listener implements MessageAvailableListener {
        WebClient client;

        Continuation continuation;

        List queue = new LinkedList();

        Listener(WebClient client) {
            this.client = client;
        }

        public void setContinuation(Continuation continuation) {
            synchronized (client) {
                this.continuation = continuation;
            }
        }

        public void onMessageAvailable(MessageConsumer consumer) {
            synchronized (client) {
                // System.err.println("message for "+consumer+"
                // continuation="+continuation);
                if (continuation != null)
                    continuation.resume();
                continuation = null;
            }
        }

    }

}
