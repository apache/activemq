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

package org.apache.activemq.transport.ws.jetty11;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;

import org.apache.activemq.Service;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.BrokerServiceAware;
import org.apache.activemq.transport.Transport;
import org.apache.activemq.transport.TransportAcceptListener;
import org.apache.activemq.transport.TransportFactory;
import org.apache.activemq.transport.tcp.ExceededMaximumConnectionsException;
import org.apache.activemq.transport.util.HttpTransportUtils;
import org.apache.activemq.transport.ws.WSTransportProxy;
import org.apache.activemq.util.ServiceListener;
import org.eclipse.jetty.websocket.api.WebSocketListener;
import org.eclipse.jetty.websocket.server.JettyServerUpgradeRequest;
import org.eclipse.jetty.websocket.server.JettyServerUpgradeResponse;
import org.eclipse.jetty.websocket.server.JettyWebSocketCreator;
import org.eclipse.jetty.websocket.server.JettyWebSocketServlet;
import org.eclipse.jetty.websocket.server.JettyWebSocketServletFactory;

/**
 * Handle connection upgrade requests and creates web sockets
 */
public class WSServlet extends JettyWebSocketServlet implements BrokerServiceAware, ServiceListener {

    private static final long serialVersionUID = -4716657876092884139L;

    private TransportAcceptListener listener;

    private final static Map<String, Integer> stompProtocols = new ConcurrentHashMap<>();
    private final static Map<String, Integer> mqttProtocols = new ConcurrentHashMap<>();

    private Map<String, Object> transportOptions;
    private BrokerService brokerService;

    private int maximumConnections = Integer.MAX_VALUE;
    protected final AtomicLong maximumConnectionsExceededCount = new AtomicLong();
    private final AtomicInteger currentTransportCount = new AtomicInteger();

    private enum Protocol {
        MQTT, STOMP, UNKNOWN
    }

    static {
        stompProtocols.put("v12.stomp", 3);
        stompProtocols.put("v11.stomp", 2);
        stompProtocols.put("v10.stomp", 1);
        stompProtocols.put("stomp", 0);

        mqttProtocols.put("mqttv3.1", 1);
        mqttProtocols.put("mqtt", 0);
    }

    @Override
    public void init() throws ServletException {
        super.init();
        listener = (TransportAcceptListener) getServletContext().getAttribute("acceptListener");
        if (listener == null) {
            throw new ServletException("No such attribute 'acceptListener' available in the ServletContext");
        }
    }

    @Override
    protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
        //return empty response - AMQ-6491
    }

    @Override
    public void configure(JettyWebSocketServletFactory factory) {
        factory.setCreator(new JettyWebSocketCreator() {
            @Override
            public Object createWebSocket(JettyServerUpgradeRequest req, JettyServerUpgradeResponse resp) {
                int currentCount;
                do {
                    currentCount = currentTransportCount.get();
                    if (currentCount >= maximumConnections) {
                        maximumConnectionsExceededCount.incrementAndGet();
                        listener.onAcceptError(new ExceededMaximumConnectionsException(
                                "Exceeded the maximum number of allowed client connections. See the '" +
                                        "maximumConnections' property on the WS transport configuration URI " +
                                        "in the ActiveMQ configuration file (e.g., activemq.xml)"));
                        return null;
                    }

                    //Increment this value before configuring the transport
                    //This is necessary because some of the transport servers must read from the
                    //socket during configureTransport() so we want to make sure this value is
                    //accurate as the transport server could pause here waiting for data to be sent from a client
                } while(!currentTransportCount.compareAndSet(currentCount, currentCount + 1));
                WebSocketListener socket;
                Protocol requestedProtocol = Protocol.UNKNOWN;

                // When no sub-protocol is requested we default to STOMP for legacy reasons.
                if (!req.getSubProtocols().isEmpty()) {
                    for (String subProtocol : req.getSubProtocols()) {
                        if (subProtocol.startsWith("mqtt")) {
                            requestedProtocol = Protocol.MQTT;
                        } else if (subProtocol.contains("stomp")) {
                            requestedProtocol = Protocol.STOMP;
                        }
                    }
                } else {
                    requestedProtocol = Protocol.STOMP;
                }

                switch (requestedProtocol) {
                    case MQTT:
                        socket = new MQTTSocket(HttpTransportUtils.generateWsRemoteAddress(req.getHttpServletRequest()));
                        ((MQTTSocket) socket).setTransportOptions(new HashMap<>(transportOptions));
                        ((MQTTSocket) socket).setPeerCertificates(req.getCertificates());
                        ((MQTTSocket) socket).addServiceListener(WSServlet.this);
                        resp.setAcceptedSubProtocol(getAcceptedSubProtocol(mqttProtocols, req.getSubProtocols(), "mqtt"));
                        break;
                    case UNKNOWN:
                        socket = findWSTransport(req, resp);
                        if (socket != null) {
                            break;
                        }
                    case STOMP:
                        socket = new StompSocket(HttpTransportUtils.generateWsRemoteAddress(req.getHttpServletRequest()));
                        ((StompSocket) socket).setPeerCertificates(req.getCertificates());
                        ((StompSocket) socket).addServiceListener(WSServlet.this);
                        resp.setAcceptedSubProtocol(getAcceptedSubProtocol(stompProtocols, req.getSubProtocols(), "stomp"));
                        break;
                    default:
                        socket = null;
                        listener.onAcceptError(new IOException("Unknown protocol requested"));
                        currentTransportCount.decrementAndGet();
                        break;
                }

                if (socket != null) {
                    listener.onAccept((Transport) socket);
                }

                return socket;
            }
        });
    }

    private WebSocketListener findWSTransport(JettyServerUpgradeRequest request, JettyServerUpgradeResponse response) {
        WSTransportProxy proxy = null;

        for (String subProtocol : request.getSubProtocols()) {
            try {
                String remoteAddress = HttpTransportUtils.generateWsRemoteAddress(request.getHttpServletRequest(), subProtocol);
                URI remoteURI = new URI(remoteAddress);

                TransportFactory factory = TransportFactory.findTransportFactory(remoteURI);

                if (factory instanceof BrokerServiceAware) {
                    ((BrokerServiceAware) factory).setBrokerService(brokerService);
                }

                Transport transport = factory.doConnect(remoteURI);

                proxy = new WSTransportProxy(remoteAddress, transport);
                proxy.setPeerCertificates(request.getCertificates());
                proxy.setTransportOptions(new HashMap<>(transportOptions));
                proxy.addServiceListener(this);

                response.setAcceptedSubProtocol(proxy.getSubProtocol());
            } catch (Exception e) {
                proxy = null;

                // Keep going and try any other sub-protocols present.
                continue;
            }
        }

        return proxy;
    }

    private String getAcceptedSubProtocol(final Map<String, Integer> protocols, List<String> subProtocols, String defaultProtocol) {
        List<SubProtocol> matchedProtocols = new ArrayList<>();
        if (subProtocols != null && subProtocols.size() > 0) {
            // detect which subprotocols match accepted protocols and add to the
            // list
            for (String subProtocol : subProtocols) {
                Integer priority = protocols.get(subProtocol);
                if (subProtocol != null && priority != null) {
                    // only insert if both subProtocol and priority are not null
                    matchedProtocols.add(new SubProtocol(subProtocol, priority));
                }
            }
            // sort the list by priority
            if (matchedProtocols.size() > 0) {
                Collections.sort(matchedProtocols, new Comparator<SubProtocol>() {
                    @Override
                    public int compare(SubProtocol s1, SubProtocol s2) {
                        return s2.priority.compareTo(s1.priority);
                    }
                });
                return matchedProtocols.get(0).protocol;
            }
        }
        return defaultProtocol;
    }

    private class SubProtocol {
        private String protocol;
        private Integer priority;

        public SubProtocol(String protocol, Integer priority) {
            this.protocol = protocol;
            this.priority = priority;
        }
    }

    public void setTransportOptions(Map<String, Object> transportOptions) {
        this.transportOptions = transportOptions;
    }

    @Override
    public void setBrokerService(BrokerService brokerService) {
        this.brokerService = brokerService;
    }


    @Override
    public void started(Service service) {
    }

    @Override
    public void stopped(Service service) {
        this.currentTransportCount.decrementAndGet();
    }

    /**
     * @return the maximumConnections
     */
    public int getMaximumConnections() {
        return maximumConnections;
    }


    public long getMaxConnectionExceededCount() {
        return this.maximumConnectionsExceededCount.get();
    }

    public void resetStatistics() {
        this.maximumConnectionsExceededCount.set(0L);
    }

    /**
     * @param maximumConnections
     *            the maximumConnections to set
     */
    public void setMaximumConnections(int maximumConnections) {
        this.maximumConnections = maximumConnections;
    }

    public AtomicInteger getCurrentTransportCount() {
        return currentTransportCount;
    }
}
