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

package org.apache.activemq.transport.ws;

import java.io.IOException;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.activemq.transport.TransportAcceptListener;
import org.eclipse.jetty.websocket.WebSocket;
import org.eclipse.jetty.websocket.WebSocketServlet;

/**
 * 
 * Handle connection upgrade requests and creates web sockets
 *
 */
public class StompServlet extends WebSocketServlet {

    private static final long serialVersionUID = -4716657876092884139L;
    
    private TransportAcceptListener listener;
    
    public void init() throws ServletException {
        super.init();
        listener = (TransportAcceptListener)getServletContext().getAttribute("acceptListener");
        if (listener == null) {
            throw new ServletException("No such attribute 'acceptListener' available in the ServletContext");
        }
    }

    protected void doGet(HttpServletRequest request, HttpServletResponse response) 
        throws ServletException ,IOException  {
        getServletContext().getNamedDispatcher("default").forward(request,response);
    }
    
    protected WebSocket doWebSocketConnect(HttpServletRequest request, String protocol) {
        StompSocket socket = new StompSocket();
        listener.onAccept(socket);
        return socket;
    }
    
}
