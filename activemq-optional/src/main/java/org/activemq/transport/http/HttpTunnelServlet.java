/** 
 * 
 * Copyright 2004 Protique Ltd
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
 * 
 **/
package org.activemq.transport.http;

import edu.emory.mathcs.backport.java.util.concurrent.ArrayBlockingQueue;
import edu.emory.mathcs.backport.java.util.concurrent.TimeUnit;

import org.activemq.command.Command;
import org.activemq.command.CommandTypes;
import org.activemq.command.ConnectionInfo;
import org.activemq.command.KeepAliveInfo;
import org.activemq.command.WireFormatInfo;
import org.activemq.transport.TransportAcceptListener;
import org.activemq.transport.util.TextWireFormat;
import org.activemq.transport.xstream.XStreamWireFormat;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.HttpSession;

import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

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
    private KeepAliveInfo ping = new KeepAliveInfo();

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

    protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
        // lets return the next response
        Command packet = null;
        try {
            System.err.println("\nrequest="+request);
            BlockingQueueTransport transportChannel = getTransportChannel(request);
            if (transportChannel == null) {
                log("No transport available! ");
                return;
            }
            packet = (Command) transportChannel.getQueue().poll(requestTimeout, TimeUnit.MILLISECONDS);
            System.err.println("packet="+packet);
        }
        catch (InterruptedException e) {
            // ignore
        }
        if (packet == null) {
            // TODO temporary hack to prevent busy loop.  Replace with continuations
            try{ Thread.sleep(250);}catch (InterruptedException e) { e.printStackTrace(); }
            response.setStatus(HttpServletResponse.SC_REQUEST_TIMEOUT);
        }
        else {
            wireFormat.marshal(packet, new DataOutputStream(response.getOutputStream()));
        }
    }

    protected void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {

        // String body = readRequestBody(request);
        // Command command = wireFormat.readCommand(body);
        
        Command command = wireFormat.readCommand(request.getReader());

        if (command instanceof ConnectionInfo) {
            ConnectionInfo info = (ConnectionInfo) command;
            request.getSession(true).setAttribute("clientID", info.getClientId());
        }
        if (command instanceof WireFormatInfo) {
            WireFormatInfo info = (WireFormatInfo) command;
            if (!canProcessWireFormatVersion(info.getVersion())) {
                response.sendError(HttpServletResponse.SC_NOT_FOUND, "Cannot process wire format of version: " + info.getVersion());
            }

        }
        else {
            BlockingQueueTransport transport = getTransportChannel(request);
            if (transport == null) {
                response.setStatus(HttpServletResponse.SC_NOT_FOUND);
            }
            else {
                transport.doConsume(command);
            }
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

    protected BlockingQueueTransport getTransportChannel(HttpServletRequest request) {
        HttpSession session = request.getSession(true);
        String clientID = null;
        if (session != null) {
            clientID = (String) session.getAttribute("clientID");
        }
        if (clientID == null) {
            clientID = request.getHeader("clientID");
        }
        System.out.println("clientID="+clientID);
        /**
         * if (clientID == null) { clientID = request.getParameter("clientID"); }
         */
        if (clientID == null) {
            log.warn("No clientID header so ignoring request");
            return null;
        }
        synchronized (this) {
            BlockingQueueTransport answer = (BlockingQueueTransport) clients.get(clientID);
            if (answer == null) {
                answer = createTransportChannel();
                clients.put(clientID, answer);
                listener.onAccept(answer);
            }
            else {
                try {
                    answer.asyncRequest(ping);
                }
                catch (IOException e) {
                    log.warn("Failed to ping transport: " + e, e);
                }
            }
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
