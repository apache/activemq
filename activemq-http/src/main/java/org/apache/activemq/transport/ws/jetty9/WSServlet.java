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

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.activemq.transport.Transport;
import org.apache.activemq.transport.TransportAcceptListener;
import org.apache.activemq.transport.util.HttpTransportUtils;
import org.eclipse.jetty.websocket.api.WebSocketListener;
import org.eclipse.jetty.websocket.servlet.ServletUpgradeRequest;
import org.eclipse.jetty.websocket.servlet.ServletUpgradeResponse;
import org.eclipse.jetty.websocket.servlet.WebSocketCreator;
import org.eclipse.jetty.websocket.servlet.WebSocketServlet;
import org.eclipse.jetty.websocket.servlet.WebSocketServletFactory;

/**
 * Handle connection upgrade requests and creates web sockets
 */
public class WSServlet extends WebSocketServlet {

    private static final long serialVersionUID = -4716657876092884139L;

    private TransportAcceptListener listener;

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
        getServletContext().getNamedDispatcher("default").forward(request, response);
    }

    @Override
    public void configure(WebSocketServletFactory factory) {
        factory.setCreator(new WebSocketCreator() {
            @Override
            public Object createWebSocket(ServletUpgradeRequest req, ServletUpgradeResponse resp) {
                WebSocketListener socket;
                boolean isMqtt = false;
                for (String subProtocol : req.getSubProtocols()) {
                    subProtocol.startsWith("mqtt");
                    isMqtt = true;
                }
                if (isMqtt) {
                    socket = new MQTTSocket(HttpTransportUtils.generateWsRemoteAddress(req.getHttpServletRequest()));
                    resp.setAcceptedSubProtocol("mqtt");
                } else {
                    socket = new StompSocket(HttpTransportUtils.generateWsRemoteAddress(req.getHttpServletRequest()));
                    resp.setAcceptedSubProtocol("stomp");
                }
                listener.onAccept((Transport) socket);
                return socket;
            }
        });
    }
}
