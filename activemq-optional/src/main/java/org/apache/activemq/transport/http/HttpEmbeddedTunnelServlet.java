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
package org.apache.activemq.transport.http;

import java.net.URI;

import javax.servlet.ServletException;

import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.transport.TransportAcceptListener;

/**
 * This servlet embeds an ActiveMQ broker inside a servlet engine which is ideal
 * for deploying ActiveMQ inside a WAR and using this servlet as a HTTP tunnel.
 * 
 * @version $Revision$
 */
public class HttpEmbeddedTunnelServlet extends HttpTunnelServlet {
    private static final long serialVersionUID = -3705734740251302361L;

    protected BrokerService broker;
    protected HttpTransportServer transportConnector;

    public synchronized void init() throws ServletException {
        // lets initialize the ActiveMQ broker
        try {
            if (broker == null) {
                broker = createBroker();

                // Add the servlet connector
                String url = getConnectorURL();
                HttpTransportFactory factory = new HttpTransportFactory();
                transportConnector = (HttpTransportServer) factory.doBind(new URI(url));
                broker.addConnector(transportConnector);

                String brokerURL = getServletContext().getInitParameter("org.apache.activemq.brokerURL");
                if (brokerURL != null) {
                    log("Listening for internal communication on: " + brokerURL);
                }
            }
            broker.start();
        } catch (Exception e) {
            throw new ServletException("Failed to start embedded broker: " + e, e);
        }
        // now lets register the listener
        TransportAcceptListener listener = transportConnector.getAcceptListener();
        getServletContext().setAttribute("transportChannelListener", listener);
        super.init();
    }

    /**
     * Factory method to create a new broker
     * 
     * @throws Exception
     */
    protected BrokerService createBroker() throws Exception {
        BrokerService answer = new BrokerService();
        return answer;
    }

    protected String getConnectorURL() {
        return "http://localhost/" + getServletContext().getServletContextName();
    }
}
