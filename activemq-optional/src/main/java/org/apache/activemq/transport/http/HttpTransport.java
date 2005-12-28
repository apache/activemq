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
package org.apache.activemq.transport.http;

import org.apache.activemq.command.Command;
import org.apache.activemq.command.ConnectionInfo;
import org.apache.activemq.transport.util.TextWireFormat;
import org.apache.activemq.util.Callback;
import org.apache.activemq.util.IOExceptionSupport;
import org.apache.activemq.util.ServiceStopper;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;

/**
 * @version $Revision$
 */
public class HttpTransport extends HttpTransportSupport {
    private static final Log log = LogFactory.getLog(HttpTransport.class);
    private HttpURLConnection sendConnection;
    private HttpURLConnection receiveConnection;
    private URL url;
    private String clientID;
    private String sessionID;

    public HttpTransport(TextWireFormat wireFormat, URI remoteUrl) throws MalformedURLException {
        super(wireFormat, remoteUrl);
        url = new URL(remoteUrl.toString());
    }

    public void oneway(Command command) throws IOException {
        try {
            if (command.getDataStructureType()==ConnectionInfo.DATA_STRUCTURE_TYPE)
                clientID=((ConnectionInfo)command).getClientId();
            
            HttpURLConnection connection = getSendConnection();
            String text = getTextWireFormat().toString(command);
            Writer writer = new OutputStreamWriter(connection.getOutputStream());
            writer.write(text);
            writer.flush();
            int answer = connection.getResponseCode();
            if (answer != HttpURLConnection.HTTP_OK) {
                throw new IOException("Failed to post command: " + command + " as response was: " + answer);
            }
            checkSession(connection);
            
        }
        catch (IOException e) {
            throw IOExceptionSupport.create("Could not post command: " + command + " due to: " + e, e);
        }
    }

    public void run() {
        log.trace("HTTP GET consumer thread starting for transport: " + this);
        URI remoteUrl = getRemoteUrl();
        while (!isClosed()) {
            try {
                HttpURLConnection connection = getReceiveConnection();
                int answer = connection.getResponseCode();
                if (answer != HttpURLConnection.HTTP_OK) {
                    if (answer == HttpURLConnection.HTTP_CLIENT_TIMEOUT) {
                        log.trace("GET timed out");
                    }
                    else {
                        log.warn("Failed to perform GET on: " + remoteUrl + " as response was: " + answer);
                    }
                }
                else {
                    checkSession(connection);
                    Command command = getTextWireFormat().readCommand(new DataInputStream(connection.getInputStream()));
                    
                    if (command == null) {
                        log.warn("Received null packet from url: " + remoteUrl);
                    }
                    else {
                        doConsume(command);
                    }
                }
            }
            catch (Exception e) {
                if (!isClosed()) {
                    log.warn("Failed to perform GET on: " + remoteUrl + " due to: " + e, e);
                }
                else {
                    log.trace("Caught error after closed: " + e, e);
                }
            }
        }
    }
    
    

    // Implementation methods
    // -------------------------------------------------------------------------
    protected HttpURLConnection createSendConnection() throws IOException {
        HttpURLConnection conn = (HttpURLConnection) getRemoteURL().openConnection();
        conn.setDoOutput(true);
        conn.setRequestMethod("POST");
        configureConnection(conn);
        conn.connect();
        return conn;
    }

    protected HttpURLConnection createReceiveConnection() throws IOException {
        HttpURLConnection conn = (HttpURLConnection) getRemoteURL().openConnection();
        conn.setDoOutput(false);
        conn.setDoInput(true);
        conn.setRequestMethod("GET");
        configureConnection(conn);
        conn.connect();
        return conn;
    }

    protected void checkSession(HttpURLConnection connection)
    {
        String set_cookie=connection.getHeaderField("Set-Cookie");
        if (set_cookie!=null && set_cookie.startsWith("JSESSIONID="))
        {
            String[] bits=set_cookie.split("[=;]");
            sessionID=bits[1];
        }
    }
    
    protected void configureConnection(HttpURLConnection connection) {
        if (sessionID !=null) {
            connection.addRequestProperty("Cookie", "JSESSIONID="+sessionID);
        }
        else if (clientID != null) {
            connection.setRequestProperty("clientID", clientID);
        }
    }

    protected URL getRemoteURL() {
        return url;
    }

    protected HttpURLConnection getSendConnection() throws IOException {
        setSendConnection(createSendConnection());
        return sendConnection;
    }

    protected HttpURLConnection getReceiveConnection() throws IOException {
        setReceiveConnection(createReceiveConnection());
        return receiveConnection;
    }

    protected void setSendConnection(HttpURLConnection conn) {
        if (sendConnection != null) {
            sendConnection.disconnect();
        }
        sendConnection = conn;
    }

    protected void setReceiveConnection(HttpURLConnection conn) {
        if (receiveConnection != null) {
            receiveConnection.disconnect();
        }
        receiveConnection = conn;
    }

    protected void doStop(ServiceStopper stopper) throws Exception {
        if (sendConnection != null) {
            stopper.run(new Callback() {
                public void execute() throws Exception {
                    sendConnection.disconnect();
                }
            });
            sendConnection = null;
        }
        if (receiveConnection != null) {
            stopper.run(new Callback() {
                public void execute() throws Exception {
                    receiveConnection.disconnect();
                }
            });
            receiveConnection = null;
        }
    }

}
