/**
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
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
import java.util.HashMap;
import java.util.Map;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.activemq.command.Command;
import org.apache.activemq.command.WireFormatInfo;
import org.apache.activemq.transport.TransportAcceptListener;
import org.apache.activemq.transport.util.TextWireFormat;
import org.apache.activemq.transport.xstream.XStreamWireFormat;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import edu.emory.mathcs.backport.java.util.concurrent.ArrayBlockingQueue;
import edu.emory.mathcs.backport.java.util.concurrent.TimeUnit;

/**
 * A servlet which handles server side HTTP transport, delegating to the
 * ActiveMQ broker. This servlet is designed for being embedded inside an
 * ActiveMQ Broker using an embedded Jetty or Tomcat instance.
 * 
 * @version $Revision$
 */
public class HttpTunnelServlet extends HttpServlet {
    private static final long serialVersionUID = -3826714430767484333L;
    private static final Log log = LogFactory.getLog(HttpTunnelServlet.class);

    private TransportAcceptListener listener;
    private TextWireFormat wireFormat;
    private Map clients = new HashMap();
    private long requestTimeout = 30000L;

    public void init() throws ServletException {
        super.init();
        listener = (TransportAcceptListener) getServletContext().getAttribute("acceptListener");
        if (listener == null) {
            throw new ServletException("No such attribute 'acceptListener' available in the ServletContext");
        }
        wireFormat = (TextWireFormat) getServletContext().getAttribute("wireFormat");
        if (wireFormat == null) {
            wireFormat = createWireFormat();
        }
    }
    
    protected void doHead(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
        createTransportChannel(request, response);
    }
    
    protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
        // lets return the next response
        Command packet = null;
        int count=0;
        try {
            BlockingQueueTransport transportChannel = getTransportChannel(request, response);
            if (transportChannel == null)
                return;
            
            packet = (Command) transportChannel.getQueue().poll(requestTimeout, TimeUnit.MILLISECONDS);
            
            DataOutputStream stream = new DataOutputStream(response.getOutputStream());
//            while( packet !=null ) {
            	wireFormat.marshal(packet, stream);
            	count++;
//            	packet = (Command) transportChannel.getQueue().poll(0, TimeUnit.MILLISECONDS);
//            }

        } catch (InterruptedException ignore) {
        }
        if (count == 0) {
            response.setStatus(HttpServletResponse.SC_REQUEST_TIMEOUT);
        }
    }

    protected void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {

        // Read the command directly from the reader
        Command command = (Command) wireFormat.unmarshalText(request.getReader());

        if (command instanceof WireFormatInfo) {
            WireFormatInfo info = (WireFormatInfo) command;
            if (!canProcessWireFormatVersion(info.getVersion())) {
                response.sendError(HttpServletResponse.SC_NOT_FOUND, "Cannot process wire format of version: " + info.getVersion());
            }

        } else {

            BlockingQueueTransport transport = getTransportChannel(request, response);
            if (transport == null)
                return;
            
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
            }
            else {
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
            log.warn("No clientID header specified");
            return null;
        }
        synchronized (this) {
            BlockingQueueTransport answer = (BlockingQueueTransport) clients.get(clientID);
            if (answer == null) {
                log.warn("The clientID header specified is invalid. Client sesion has not yet been established for it: "+clientID);
                return null;
            }
            return answer;
        }
    }
    
    protected BlockingQueueTransport createTransportChannel(HttpServletRequest request, HttpServletResponse response) throws IOException {
        String clientID = request.getHeader("clientID");
        
        if (clientID == null) {
            response.sendError(HttpServletResponse.SC_BAD_REQUEST, "No clientID header specified");
            log.warn("No clientID header specified");
            return null;
        }
        
        synchronized (this) {
            BlockingQueueTransport answer = (BlockingQueueTransport) clients.get(clientID);
            if (answer != null) {
                response.sendError(HttpServletResponse.SC_BAD_REQUEST, "A session for clientID '"+clientID+"' has allready been established");
                log.warn("A session for clientID '"+clientID+"' has allready been established");
                return null;
            }
            
            answer = createTransportChannel();
            clients.put(clientID, answer);
            listener.onAccept(answer);            
            return answer;
        }
    }

    protected BlockingQueueTransport createTransportChannel() {
        return new BlockingQueueTransport(new ArrayBlockingQueue(10));
    }

    protected TextWireFormat createWireFormat() {
        return new XStreamWireFormat();
    }
}
