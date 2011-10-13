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

import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import javax.servlet.ServletException;
import javax.servlet.ServletInputStream;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.apache.activemq.Service;
import org.apache.activemq.command.Command;
import org.apache.activemq.command.WireFormatInfo;
import org.apache.activemq.transport.Transport;
import org.apache.activemq.transport.TransportAcceptListener;
import org.apache.activemq.transport.util.TextWireFormat;
import org.apache.activemq.transport.xstream.XStreamWireFormat;
import org.apache.activemq.util.IOExceptionSupport;
import org.apache.activemq.util.ServiceListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A servlet which handles server side HTTP transport, delegating to the
 * ActiveMQ broker. This servlet is designed for being embedded inside an
 * ActiveMQ Broker using an embedded Jetty or Tomcat instance.
 */
public class HttpTunnelServlet extends HttpServlet {
    private static final long serialVersionUID = -3826714430767484333L;
    private static final Logger LOG = LoggerFactory.getLogger(HttpTunnelServlet.class);

    private TransportAcceptListener listener;
    private HttpTransportFactory transportFactory;
    private TextWireFormat wireFormat;
    private ConcurrentMap<String, BlockingQueueTransport> clients = new ConcurrentHashMap<String, BlockingQueueTransport>();
    private final long requestTimeout = 30000L;
    private HashMap<String, Object> transportOptions;

    @SuppressWarnings("unchecked")
    @Override
    public void init() throws ServletException {
        super.init();
        listener = (TransportAcceptListener)getServletContext().getAttribute("acceptListener");
        if (listener == null) {
            throw new ServletException("No such attribute 'acceptListener' available in the ServletContext");
        }
        transportFactory = (HttpTransportFactory)getServletContext().getAttribute("transportFactory");
        if (transportFactory == null) {
            throw new ServletException("No such attribute 'transportFactory' available in the ServletContext");
        }
        transportOptions = (HashMap<String, Object>)getServletContext().getAttribute("transportOptions");
        wireFormat = (TextWireFormat)getServletContext().getAttribute("wireFormat");
        if (wireFormat == null) {
            wireFormat = createWireFormat();
        }
    }

    @Override
    protected void doHead(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
        createTransportChannel(request, response);
    }

    @Override
    protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
        // lets return the next response
        Command packet = null;
        int count = 0;
        try {
            BlockingQueueTransport transportChannel = getTransportChannel(request, response);
            if (transportChannel == null) {
                return;
            }

            packet = (Command)transportChannel.getQueue().poll(requestTimeout, TimeUnit.MILLISECONDS);

            DataOutputStream stream = new DataOutputStream(response.getOutputStream());
            // while( packet !=null ) {
            wireFormat.marshal(packet, stream);
            count++;
            // packet = (Command) transportChannel.getQueue().poll(0,
            // TimeUnit.MILLISECONDS);
            // }

        } catch (InterruptedException ignore) {
        }
        if (count == 0) {
            response.setStatus(HttpServletResponse.SC_REQUEST_TIMEOUT);
        }
    }

    @Override
    protected void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException,
            IOException {

        // Read the command directly from the reader, assuming UTF8 encoding
        ServletInputStream sis = request.getInputStream();
        Command command = (Command) wireFormat.unmarshalText(new InputStreamReader(sis, "UTF-8"));

        if (command instanceof WireFormatInfo) {
            WireFormatInfo info = (WireFormatInfo) command;
            if (!canProcessWireFormatVersion(info.getVersion())) {
                response.sendError(HttpServletResponse.SC_NOT_FOUND, "Cannot process wire format of version: "
                        + info.getVersion());
            }

        } else {

            BlockingQueueTransport transport = getTransportChannel(request, response);
            if (transport == null) {
                return;
            }

            transport.doConsume(command);
        }
    }

    private boolean canProcessWireFormatVersion(int version) {
        // TODO:
        return true;
    }

    protected String readRequestBody(HttpServletRequest request) throws IOException {
        StringBuffer buffer = new StringBuffer();
        BufferedReader reader = request.getReader();
        while (true) {
            String line = reader.readLine();
            if (line == null) {
                break;
            } else {
                buffer.append(line);
                buffer.append("\n");
            }
        }
        return buffer.toString();
    }

    protected BlockingQueueTransport getTransportChannel(HttpServletRequest request, HttpServletResponse response) throws IOException {
        String clientID = request.getHeader("clientID");
        if (clientID == null) {
            response.sendError(HttpServletResponse.SC_BAD_REQUEST, "No clientID header specified");
            LOG.warn("No clientID header specified");
            return null;
        }
        BlockingQueueTransport answer = clients.get(clientID);
        if (answer == null) {
            LOG.warn("The clientID header specified is invalid. Client sesion has not yet been established for it: " + clientID);
            return null;
        }
        return answer;
    }

    protected BlockingQueueTransport createTransportChannel(HttpServletRequest request, HttpServletResponse response) throws IOException {
        final String clientID = request.getHeader("clientID");

        if (clientID == null) {
            response.sendError(HttpServletResponse.SC_BAD_REQUEST, "No clientID header specified");
            LOG.warn("No clientID header specified");
            return null;
        }

        // Optimistically create the client's transport; this transport may be thrown away if the client has already registered.
        BlockingQueueTransport answer = createTransportChannel();

        // Record the client's transport and ensure that it has not already registered; this is thread-safe and only allows one
        // thread to register the client
        if (clients.putIfAbsent(clientID, answer) != null) {
            response.sendError(HttpServletResponse.SC_BAD_REQUEST, "A session for clientID '" + clientID + "' has already been established");
            LOG.warn("A session for clientID '" + clientID + "' has already been established");
            return null;
        }

        // Ensure that the client's transport is cleaned up when no longer
        // needed.
        answer.addServiceListener(new ServiceListener() {
            public void started(Service service) {
                // Nothing to do.
            }

            public void stopped(Service service) {
                clients.remove(clientID);
            }
        });

        // Configure the transport with any additional properties or filters.  Although the returned transport is not explicitly
        // persisted, if it is a filter (e.g., InactivityMonitor) it will be linked to the client's transport as a TransportListener
        // and not GC'd until the client's transport is disposed.
        Transport transport = answer;
        try {
            // Preserve the transportOptions for future use by making a copy before applying (they are removed when applied).
            HashMap<String, Object> options = new HashMap<String, Object>(transportOptions);
            transport = transportFactory.serverConfigure(answer, null, options);
        } catch (Exception e) {
            throw IOExceptionSupport.create(e);
        }

        // Wait for the transport to be connected or disposed.
        listener.onAccept(transport);
        while (!transport.isConnected() && !transport.isDisposed()) {
            try {
                Thread.sleep(100);
            } catch (InterruptedException ignore) {
            }
        }

        // Ensure that the transport was not prematurely disposed.
        if (transport.isDisposed()) {
            response.sendError(HttpServletResponse.SC_BAD_REQUEST, "The session for clientID '" + clientID + "' was prematurely disposed");
            LOG.warn("The session for clientID '" + clientID + "' was prematurely disposed");
            return null;
        }

        return answer;
    }

    protected BlockingQueueTransport createTransportChannel() {
       return new BlockingQueueTransport(new LinkedBlockingQueue<Object>());
    }

    protected TextWireFormat createWireFormat() {
        return new XStreamWireFormat();
    }
}
