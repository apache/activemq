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

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;

import org.apache.activemq.command.Command;
import org.apache.activemq.command.ConnectionInfo;
import org.apache.activemq.transport.util.TextWireFormat;
import org.apache.activemq.util.ByteArrayOutputStream;
import org.apache.activemq.util.ByteSequence;
import org.apache.activemq.util.Callback;
import org.apache.activemq.util.IOExceptionSupport;
import org.apache.activemq.util.ServiceStopper;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * @version $Revision$
 */
public class HttpTransport extends HttpTransportSupport {
    
    private static final Log LOG = LogFactory.getLog(HttpTransport.class);
    
    private HttpURLConnection sendConnection;
    private HttpURLConnection receiveConnection;
    private URL url;
    private String clientID;
    private volatile int receiveCounter;
    
    // private String sessionID;

    public HttpTransport(TextWireFormat wireFormat, URI remoteUrl) throws MalformedURLException {
        super(wireFormat, remoteUrl);
        url = new URL(remoteUrl.toString());
    }

    public void oneway(Object o) throws IOException {
        final Command command = (Command)o;
        try {
            if (command.getDataStructureType() == ConnectionInfo.DATA_STRUCTURE_TYPE) {
                boolean startGetThread = clientID == null;
                clientID = ((ConnectionInfo)command).getClientId();
                if (startGetThread && isStarted()) {
                    try {
                        super.doStart();
                    } catch (Exception e) {
                        throw IOExceptionSupport.create(e);
                    }
                }
            }

            HttpURLConnection connection = getSendConnection();
            String text = getTextWireFormat().marshalText(command);
            Writer writer = new OutputStreamWriter(connection.getOutputStream());
            writer.write(text);
            writer.flush();
            int answer = connection.getResponseCode();
            if (answer != HttpURLConnection.HTTP_OK) {
                throw new IOException("Failed to post command: " + command + " as response was: " + answer);
            }
            // checkSession(connection);
        } catch (IOException e) {
            throw IOExceptionSupport.create("Could not post command: " + command + " due to: " + e, e);
        }
    }

    public void run() {
        LOG.trace("HTTP GET consumer thread starting for transport: " + this);
        URI remoteUrl = getRemoteUrl();
        while (!isStopped()) {
            try {
                HttpURLConnection connection = getReceiveConnection();
                int answer = connection.getResponseCode();
                if (answer != HttpURLConnection.HTTP_OK) {
                    if (answer == HttpURLConnection.HTTP_CLIENT_TIMEOUT) {
                        LOG.trace("GET timed out");
                    } else {
                        LOG.warn("Failed to perform GET on: " + remoteUrl + " as response was: " + answer);
                    }
                } else {
                    // checkSession(connection);

                    // Create a String for the UTF content
                    receiveCounter++;
                    InputStream is = connection.getInputStream();
                    ByteArrayOutputStream baos = new ByteArrayOutputStream(connection.getContentLength() > 0 ? connection.getContentLength() : 1024);
                    int c = 0;
                    while ((c = is.read()) >= 0) {
                        baos.write(c);
                    }
                    ByteSequence sequence = baos.toByteSequence();
                    String data = new String(sequence.data, sequence.offset, sequence.length, "UTF-8");

                    Command command = (Command)getTextWireFormat().unmarshalText(data);

                    if (command == null) {
                        LOG.warn("Received null packet from url: " + remoteUrl);
                    } else {
                        doConsume(command);
                    }
                }
            } catch (Throwable e) {
                if (!isStopped()) {
                    LOG.error("Failed to perform GET on: " + remoteUrl + " due to: " + e, e);
                } else {
                    LOG.trace("Caught error after closed: " + e, e);
                }
            } finally {
                safeClose(receiveConnection);
                receiveConnection = null;
            }
        }
    }

    // Implementation methods
    // -------------------------------------------------------------------------
    protected HttpURLConnection createSendConnection() throws IOException {
        HttpURLConnection conn = (HttpURLConnection)getRemoteURL().openConnection();
        conn.setDoOutput(true);
        conn.setRequestMethod("POST");
        configureConnection(conn);
        conn.connect();
        return conn;
    }

    protected HttpURLConnection createReceiveConnection() throws IOException {
        HttpURLConnection conn = (HttpURLConnection)getRemoteURL().openConnection();
        conn.setDoOutput(false);
        conn.setDoInput(true);
        conn.setRequestMethod("GET");
        configureConnection(conn);
        conn.connect();
        return conn;
    }

    // protected void checkSession(HttpURLConnection connection)
    // {
    // String set_cookie=connection.getHeaderField("Set-Cookie");
    // if (set_cookie!=null && set_cookie.startsWith("JSESSIONID="))
    // {
    // String[] bits=set_cookie.split("[=;]");
    // sessionID=bits[1];
    // }
    // }

    protected void configureConnection(HttpURLConnection connection) {
        // if (sessionID !=null) {
        // connection.addRequestProperty("Cookie", "JSESSIONID="+sessionID);
        // }
        // else
        if (clientID != null) {
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
        safeClose(sendConnection);
        sendConnection = conn;
    }

    protected void setReceiveConnection(HttpURLConnection conn) {
        safeClose(receiveConnection);
        receiveConnection = conn;
    }

    protected void doStart() throws Exception {
        // Don't start the background thread until the clientId has been
        // established.
        if (clientID != null) {
            super.doStart();
        }
    }

    protected void doStop(ServiceStopper stopper) throws Exception {
        stopper.run(new Callback() {
            public void execute() throws Exception {
                safeClose(sendConnection);
            }
        });
        sendConnection = null;
        stopper.run(new Callback() {
            public void execute() {
                safeClose(receiveConnection);
            }
        });
    }

    /**
     * @param connection TODO
     */
    private void safeClose(HttpURLConnection connection) {
        if (connection != null) {
            connection.disconnect();
        }
    }

    public int getReceiveCounter() {
        return receiveCounter;
    }

}
