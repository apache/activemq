/**
 *
 * Copyright 2004 The Apache Software Foundation
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

import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.command.ActiveMQTopic;

import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.TextMessage;
import javax.servlet.ServletConfig;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpSession;
import java.io.BufferedReader;
import java.io.IOException;
import java.util.Iterator;
import java.util.Map;

/**
 * A useful base class for any JMS related servlet;
 * there are various ways to map JMS operations to web requests
 * so we put most of the common behaviour in a reusable base class.
 *
 * @version $Revision: 1.1.1.1 $
 */
public abstract class MessageServletSupport extends HttpServlet {

    private boolean defaultTopicFlag = true;
    private Destination defaultDestination;
    private String destinationParameter = "destination";
    private String topicParameter = "topic";
    private String bodyParameter = "body";


    public void init(ServletConfig servletConfig) throws ServletException {
        super.init(servletConfig);

        String name = servletConfig.getInitParameter("topic");
        if (name != null) {
            defaultTopicFlag = asBoolean(name);
        }

        log("Defaulting to use topics: " + defaultTopicFlag);

        name = servletConfig.getInitParameter("destination");
        if (name != null) {
            if (defaultTopicFlag) {
                defaultDestination = new ActiveMQTopic(name);
            }
            else {
                defaultDestination = new ActiveMQQueue(name);
            }
        }

        // lets check to see if there's a connection factory set
        WebClient.initContext(getServletContext());
    }

    protected WebClient createWebClient(HttpServletRequest request) {
        return new WebClient(getServletContext());
    }

    public static boolean asBoolean(String param) {
        return asBoolean(param, false);
    }
    
    public static boolean asBoolean(String param, boolean defaultValue) {
        if (param == null) {
            return defaultValue;
        }
        else {
            return param.equalsIgnoreCase("true");
        }
    }

    /**
     * Helper method to get the client for the current session
     *
     * @param request is the current HTTP request
     * @return the current client or a newly creates
     */
    protected WebClient getWebClient(HttpServletRequest request) {
        HttpSession session = request.getSession(true);
        WebClient client = WebClient.getWebClient(session);
        if (client == null) {
            client = createWebClient(request);
            session.setAttribute(WebClient.webClientAttribute, client);
        }
        return client;
    }


    protected void appendParametersToMessage(HttpServletRequest request, TextMessage message) throws JMSException {
        for (Iterator iter = request.getParameterMap().entrySet().iterator(); iter.hasNext();) {
            Map.Entry entry = (Map.Entry) iter.next();
            String name = (String) entry.getKey();
            if (!destinationParameter.equals(name) && !topicParameter.equals(name) && !bodyParameter.equals(name)) {
                Object value = entry.getValue();
                if (value instanceof Object[]) {
                    Object[] array = (Object[]) value;
                    if (array.length == 1) {
                        value = array[0];
                    }
                    else {
                        log("Can't use property: " + name + " which is of type: " + value.getClass().getName() + " value");
                        value = null;
                        for (int i = 0, size = array.length; i < size; i++) {
                            log("value[" + i + "] = " + array[i]);
                        }
                    }
                }
                if (value != null) {
                    message.setObjectProperty(name, value);
                }
            }
        }
    }

    /**
     * @return the destination to use for the current request
     */
    protected Destination getDestination(WebClient client, HttpServletRequest request) throws JMSException, NoDestinationSuppliedException {
        String destinationName = request.getParameter(destinationParameter);
        if (destinationName == null) {
            if (defaultDestination == null) {
                return getDestinationFromURI(client, request);
            }
            else {
                return defaultDestination;
            }
        }

        return getDestination(client, request, destinationName);
    }

    /**
     * @return the destination to use for the current request using the relative URI from
     *         where this servlet was invoked as the destination name
     */
    protected Destination getDestinationFromURI(WebClient client, HttpServletRequest request) throws NoDestinationSuppliedException, JMSException {
        String uri = request.getPathInfo();
        if (uri == null) {
            throw new NoDestinationSuppliedException();
        }
        // replace URI separator with JMS destination separator
        if (uri.startsWith("/")) {
            uri = uri.substring(1);
        }
        uri = uri.replace('/', '.');
        return getDestination(client, request, uri);
    }

    /**
     * @return the Destination object for the given destination name
     */
    protected Destination getDestination(WebClient client, HttpServletRequest request, String destinationName) throws JMSException {
        if (isTopic(request)) {
            return client.getSession().createTopic(destinationName);
        }
        else {
            return client.getSession().createQueue(destinationName);
        }
    }

    /**
     * @return true if the current request is for a topic destination, else false if its for a queue
     */
    protected boolean isTopic
            (HttpServletRequest
            request) {
        boolean aTopic = defaultTopicFlag;
        String aTopicText = request.getParameter(topicParameter);
        if (aTopicText != null) {
            aTopic = asBoolean(aTopicText);
        }
        return aTopic;
    }

    protected long asLong(String name) {
        return Long.parseLong(name);
    }

    /**
     * @return the text that was posted to the servlet which is used as the body
     *         of the message to be sent
     */
    protected String getPostedMessageBody(HttpServletRequest request) throws IOException {
        String answer = request.getParameter(bodyParameter);
        if (answer == null) {
            // lets read the message body instead
            BufferedReader reader = request.getReader();
            StringBuffer buffer = new StringBuffer();
            while (true) {
                String line = reader.readLine();
                if (line == null) {
                    break;
                }
                buffer.append(line);
                buffer.append("\n");
            }
            return buffer.toString();
        }
        return answer;
    }
}
