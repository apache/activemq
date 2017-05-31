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

package org.apache.activemq.transport.ws.jetty9;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.BrokerServiceAware;
import org.apache.activemq.transport.Transport;
import org.apache.activemq.transport.TransportAcceptListener;
import org.apache.activemq.transport.TransportFactory;
import org.apache.activemq.transport.util.HttpTransportUtils;
import org.apache.activemq.transport.ws.WSTransportProxy;
import org.eclipse.jetty.websocket.api.WebSocketListener;
import org.eclipse.jetty.websocket.servlet.ServletUpgradeRequest;
import org.eclipse.jetty.websocket.servlet.ServletUpgradeResponse;
import org.eclipse.jetty.websocket.servlet.WebSocketCreator;
import org.eclipse.jetty.websocket.servlet.WebSocketServlet;
import org.eclipse.jetty.websocket.servlet.WebSocketServletFactory;

/**
 * Handle connection upgrade requests and creates web sockets
 */
public class WSServlet extends WebSocketServlet implements BrokerServiceAware {

    private static final long serialVersionUID = -4716657876092884139L;

    private TransportAcceptListener listener;

    private final static Map<String, Integer> stompProtocols = new ConcurrentHashMap<>();
    private final static Map<String, Integer> mqttProtocols = new ConcurrentHashMap<>();

    private Map<String, Object> transportOptions;
    private BrokerService brokerService;

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
    public void configure(WebSocketServletFactory factory) {
        factory.setCreator(new WebSocketCreator() {
            @Override
            public Object createWebSocket(ServletUpgradeRequest req, ServletUpgradeResponse resp) {
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
                        resp.setAcceptedSubProtocol(getAcceptedSubProtocol(stompProtocols, req.getSubProtocols(), "stomp"));
                        break;
                    default:
                        socket = null;
                        listener.onAcceptError(new IOException("Unknown protocol requested"));
                        break;
                }

                if (socket != null) {
                    listener.onAccept((Transport) socket);
                }

                return socket;
            }
        });
    }

    private WebSocketListener findWSTransport(ServletUpgradeRequest request, ServletUpgradeResponse response) {
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
                proxy.setTransportOptions(transportOptions);

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
}
