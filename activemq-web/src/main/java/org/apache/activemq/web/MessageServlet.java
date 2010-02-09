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
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;

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

import org.apache.activemq.MessageAvailableConsumer;
import org.apache.activemq.MessageAvailableListener;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.ActiveMQTextMessage;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.eclipse.jetty.continuation.Continuation;
import org.eclipse.jetty.continuation.ContinuationSupport;

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
public class MessageServlet extends MessageServletSupport {
    private static final Log LOG = LogFactory.getLog(MessageServlet.class);

    private String readTimeoutParameter = "readTimeout";
    private long defaultReadTimeout = -1;
    private long maximumReadTimeout = 20000;
    private long requestTimeout = 1000;
    
    private HashMap<String, WebClient> clients = new HashMap<String, WebClient>();

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

            if (sync) {
               String point = "activemq:" 
            	   + ((ActiveMQDestination)destination).getPhysicalName().replace("//", "")
            	   + "?requestTimeout=" + requestTimeout;
               try {
            	   String body = (String)client.getProducerTemplate().requestBody(point, text);
                   ActiveMQTextMessage answer = new ActiveMQTextMessage();
                   answer.setText(body);
            	   writeMessageResponse(response.getWriter(), answer);
               } catch (Exception e) {
            	   IOException ex = new IOException();
            	   ex.initCause(e);
            	   throw ex;
               }
            } else {
                appendParametersToMessage(request, message);
                boolean persistent = isSendPersistent(request);
                int priority = getSendPriority(request);
                long timeToLive = getSendTimeToLive(request);            	
                client.send(destination, message, persistent, priority, timeToLive);
            }

            // lets return a unique URI for reliable messaging
            response.setHeader("messageID", message.getJMSMessageID());
            response.setStatus(HttpServletResponse.SC_OK);
        } catch (JMSException e) {
            throw new ServletException("Could not post JMS message: " + e, e);
        }
    }

    /**
     * Supports a HTTP DELETE to be equivlanent of consuming a singe message
     * from a queue
     */
    protected void doDelete(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
        doMessages(request, response, 1);
    }

    /**
     * Supports a HTTP DELETE to be equivlanent of consuming a singe message
     * from a queue
     */
    protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
        doMessages(request, response, -1);
    }

    /**
     * Reads a message from a destination up to some specific timeout period
     * 
     * @param request
     * @param response
     * @throws ServletException
     * @throws IOException
     */
    protected void doMessages(HttpServletRequest request, HttpServletResponse response, int maxMessages) throws ServletException, IOException {

        int messages = 0;
        try {
            WebClient client = getWebClient(request);
            Destination destination = getDestination(client, request);
            if (destination == null) {
                throw new NoDestinationSuppliedException();
            }
            long timeout = getReadTimeout(request);
            boolean ajax = isRicoAjax(request);
            if (!ajax) {
                maxMessages = 1;
            }

            if (LOG.isDebugEnabled()) {
                LOG.debug("Receiving message(s) from: " + destination + " with timeout: " + timeout);
            }

            MessageAvailableConsumer consumer = (MessageAvailableConsumer)client.getConsumer(destination);
            Continuation continuation = null;
            Listener listener = null;
            Message message = null;

            synchronized (consumer) {
                // Fetch the listeners
                listener = (Listener)consumer.getAvailableListener();
                if (listener == null) {
                    listener = new Listener(consumer);
                    consumer.setAvailableListener(listener);
                }
                // Look for any available messages
                message = consumer.receiveNoWait();

                // Get an existing Continuation or create a new one if there are
                // no events.
                if (message == null) {
                    continuation = ContinuationSupport.getContinuation(request);

                    // register this continuation with our listener.
                    listener.setContinuation(continuation);

                    // Get the continuation object (may wait and/or retry
                    // request here).
                    continuation.suspend();
                }

                // Try again now
                if (message == null) {
                    message = consumer.receiveNoWait();
                }

                // write a responds
                response.setContentType("text/xml");
                PrintWriter writer = response.getWriter();

                if (ajax) {
                    writer.println("<ajax-response>");
                }

                // handle any message(s)
                if (message == null) {
                    // No messages so OK response of for ajax else no content.
                    response.setStatus(ajax ? HttpServletResponse.SC_OK : HttpServletResponse.SC_NO_CONTENT);
                } else {
                    // We have at least one message so set up the response
                    response.setStatus(HttpServletResponse.SC_OK);
                    String type = getContentType(request);
                    if (type != null) {
                        response.setContentType(type);
                    }

                    // send a response for each available message (up to max
                    // messages)
                    while ((maxMessages < 0 || messages < maxMessages) && message != null) {
                        if (ajax) {
                            writer.print("<response type='object' id='");
                            writer.print(request.getParameter("id"));
                            writer.println("'>");
                        } else {
                            // only ever 1 message for non ajax!
                            setResponseHeaders(response, message);
                        }

                        writeMessageResponse(writer, message);

                        if (ajax) {
                            writer.println("</response>");
                        }

                        // look for next message
                        messages++;
                        if(maxMessages < 0 || messages < maxMessages) {
                        	message = consumer.receiveNoWait();
                        }
                    }
                }

                if (ajax) {
                    writer.println("<response type='object' id='poll'><ok/></response>");
                    writer.println("</ajax-response>");
                }
            }
        } catch (JMSException e) {
            throw new ServletException("Could not post JMS message: " + e, e);
        } finally {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Received " + messages + " message(s)");
            }
        }
    }

    /**
     * Reads a message from a destination up to some specific timeout period
     * 
     * @param request
     * @param response
     * @throws ServletException
     * @throws IOException
     */
    protected void doMessagesWithoutContinuation(HttpServletRequest request, HttpServletResponse response, int maxMessages) throws ServletException, IOException {

        int messages = 0;
        try {
            WebClient client = getWebClient(request);
            Destination destination = getDestination(client, request);
            long timeout = getReadTimeout(request);
            boolean ajax = isRicoAjax(request);
            if (!ajax) {
                maxMessages = 1;
            }
            if (LOG.isDebugEnabled()) {
                LOG.debug("Receiving message(s) from: " + destination + " with timeout: " + timeout);
            }

            MessageAvailableConsumer consumer = (MessageAvailableConsumer)client.getConsumer(destination);
            Message message = null;

            // write a responds
            response.setContentType("text/xml");
            PrintWriter writer = response.getWriter();

            if (ajax) {
                writer.println("<ajax-response>");
            }

            // Only one client thread at a time should poll for messages.
            if (client.getSemaphore().tryAcquire()) {
                try {
                    // Look for any available messages
                    message = consumer.receive(timeout);

                    // handle any message(s)
                    if (message == null) {
                        // No messages so OK response of for ajax else no
                        // content.
                        response.setStatus(ajax ? HttpServletResponse.SC_OK : HttpServletResponse.SC_NO_CONTENT);
                    } else {
                        // We have at least one message so set up the
                        // response
                        response.setStatus(HttpServletResponse.SC_OK);
                        String type = getContentType(request);
                        if (type != null) {
                            response.setContentType(type);
                        }

                        // send a response for each available message (up to
                        // max
                        // messages)
                        while ((maxMessages < 0 || messages < maxMessages) && message != null) {
                            if (ajax) {
                                writer.print("<response type='object' id='");
                                writer.print(request.getParameter("id"));
                                writer.println("'>");
                            } else {
                                // only ever 1 message for non ajax!
                                setResponseHeaders(response, message);
                            }

                            writeMessageResponse(writer, message);

                            if (ajax) {
                                writer.println("</response>");
                            }

                            // look for next message
                            messages++;
                            if(maxMessages < 0 || messages < maxMessages) {
                            	message = consumer.receiveNoWait();
                            }

                        }
                    }
                } finally {
                    client.getSemaphore().release();
                }
            } else {
                // Client is using us in another thread.
                response.setStatus(ajax ? HttpServletResponse.SC_OK : HttpServletResponse.SC_NO_CONTENT);
            }

            if (ajax) {
                writer.println("<response type='object' id='poll'><ok/></response>");
                writer.println("</ajax-response>");
            }

        } catch (JMSException e) {
            throw new ServletException("Could not post JMS message: " + e, e);
        } finally {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Received " + messages + " message(s)");
            }
        }
    }

    protected void writeMessageResponse(PrintWriter writer, Message message) throws JMSException, IOException {
        if (message instanceof TextMessage) {
            TextMessage textMsg = (TextMessage)message;
            String txt = textMsg.getText();
            if (txt.startsWith("<?")) {
                txt = txt.substring(txt.indexOf("?>") + 2);
            }
            writer.print(txt);
        } else if (message instanceof ObjectMessage) {
            ObjectMessage objectMsg = (ObjectMessage)message;
            Object object = objectMsg.getObject();
            writer.print(object.toString());
        }
    }

    protected boolean isRicoAjax(HttpServletRequest request) {
        String rico = request.getParameter("rico");
        return rico != null && rico.equals("true");
    }
    
    public WebClient getWebClient(HttpServletRequest request) {
    	String clientId = request.getParameter("clientId");
    	if (clientId != null) {
    		synchronized(this) {
    			LOG.debug("Getting local client [" + clientId + "]");
    			WebClient client = clients.get(clientId);
    			if (client == null) {
    				LOG.debug("Creating new client [" + clientId + "]");
    				client = new WebClient();
    				clients.put(clientId, client);
    			}
    			return client;
    		}
    		
    	} else {
    		return WebClient.getWebClient(request);
    	}
    }    

    protected String getContentType(HttpServletRequest request) {
        /*
         * log("Params: " + request.getParameterMap()); Enumeration iter =
         * request.getHeaderNames(); while (iter.hasMoreElements()) { String
         * name = (String) iter.nextElement(); log("Header: " + name + " = " +
         * request.getHeader(name)); }
         */
        String value = request.getParameter("xml");
        if (value != null && "true".equalsIgnoreCase(value)) {
            return "text/xml";
        }
        return null;
    }

    protected void setResponseHeaders(HttpServletResponse response, Message message) throws JMSException {
        response.setHeader("destination", message.getJMSDestination().toString());
        response.setHeader("id", message.getJMSMessageID());
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
    private static class Listener implements MessageAvailableListener {
        MessageConsumer consumer;
        Continuation continuation;
        List queue = new LinkedList();

        Listener(MessageConsumer consumer) {
            this.consumer = consumer;
        }

        public void setContinuation(Continuation continuation) {
            synchronized (consumer) {
                this.continuation = continuation;
            }
        }

        public void onMessageAvailable(MessageConsumer consumer) {
            assert this.consumer == consumer;

            synchronized (this.consumer) {
                if (continuation != null) {
                    continuation.resume();
                }
                continuation = null;
            }
        }
    }

}
