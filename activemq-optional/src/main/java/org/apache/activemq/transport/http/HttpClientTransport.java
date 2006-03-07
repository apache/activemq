/**
 *
 * Copyright 2005-2006 The Apache Software Foundation
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
 */
package org.apache.activemq.transport.http;

import org.apache.activemq.command.Command;
import org.apache.activemq.command.ConnectionInfo;
import org.apache.activemq.command.Response;
import org.apache.activemq.transport.FutureResponse;
import org.apache.activemq.transport.util.TextWireFormat;
import org.apache.activemq.util.IOExceptionSupport;
import org.apache.activemq.util.ServiceStopper;
import org.apache.commons.httpclient.Header;
import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.HttpMethod;
import org.apache.commons.httpclient.HttpStatus;
import org.apache.commons.httpclient.methods.GetMethod;
import org.apache.commons.httpclient.methods.PostMethod;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.DataInputStream;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URI;

/**
 * A HTTP {@link org.apache.activemq.transport.TransportChannel} which uses the <a
 * href="http://jakarta.apache.org/commons/httpclient/">commons-httpclient</a>
 * library
 * 
 * @version $Revision$
 */
public class HttpClientTransport extends HttpTransportSupport {
    private static final Log log = LogFactory.getLog(HttpClientTransport.class);

    public static final int MAX_CLIENT_TIMEOUT = 20000;

    private HttpClient sendHttpClient;
    private HttpClient receiveHttpClient;
    private String clientID;
    private String sessionID;

    public HttpClientTransport(TextWireFormat wireFormat, URI remoteUrl) {
        super(wireFormat, remoteUrl);
    }

    public FutureResponse asyncRequest(Command command) throws IOException {
        return null;
    }

    public void oneway(Command command) throws IOException {
        if (command.getDataStructureType() == ConnectionInfo.DATA_STRUCTURE_TYPE)
            clientID = ((ConnectionInfo) command).getClientId();

        PostMethod httpMethod = new PostMethod(getRemoteUrl().toString());
        configureMethod(httpMethod);
        httpMethod.setRequestBody(getTextWireFormat().toString(command));
        try {
            HttpClient client = getSendHttpClient();
            client.setTimeout(MAX_CLIENT_TIMEOUT);
            int answer = client.executeMethod(httpMethod);
            if (answer != HttpStatus.SC_OK) {
                throw new IOException("Failed to post command: " + command + " as response was: " + answer);
            }
            checkSession(httpMethod);
        }
        catch (IOException e) {
            throw IOExceptionSupport.create("Could not post command: " + command + " due to: " + e, e);
        } finally {
            httpMethod.getResponseBody();
            httpMethod.releaseConnection();
        }
    }

    public Response request(Command command) throws IOException {
        return null;
    }

    public void run() {
        log.trace("HTTP GET consumer thread starting: " + this);
        HttpClient httpClient = getReceiveHttpClient();
        URI remoteUrl = getRemoteUrl();
        while (!isClosed()) {

            GetMethod httpMethod = new GetMethod(remoteUrl.toString());
            configureMethod(httpMethod);

            try {
                int answer = httpClient.executeMethod(httpMethod);
                if (answer != HttpStatus.SC_OK) {
                    if (answer == HttpStatus.SC_REQUEST_TIMEOUT) {
                        log.info("GET timed out");
                    }
                    else {
                        log.warn("Failed to perform GET on: " + remoteUrl + " as response was: " + answer);
                    }
                }
                else {
                    checkSession(httpMethod);
                    Command command = getTextWireFormat().readCommand(new DataInputStream(httpMethod.getResponseBodyAsStream()));
                    if (command == null) {
                        log.warn("Received null command from url: " + remoteUrl);
                    }
                    else {
                        doConsume(command);
                    }
                }
            }
            catch (IOException e) {
                log.warn("Failed to perform GET on: " + remoteUrl + " due to: " + e, e);
            } finally {
                httpMethod.getResponseBody();
                httpMethod.releaseConnection();
            }
        }
    }

    // Properties
    // -------------------------------------------------------------------------
    public HttpClient getSendHttpClient() {
        if (sendHttpClient == null) {
            sendHttpClient = createHttpClient();
        }
        return sendHttpClient;
    }

    public void setSendHttpClient(HttpClient sendHttpClient) {
        this.sendHttpClient = sendHttpClient;
    }

    public HttpClient getReceiveHttpClient() {
        if (receiveHttpClient == null) {
            receiveHttpClient = createHttpClient();
        }
        return receiveHttpClient;
    }

    public void setReceiveHttpClient(HttpClient receiveHttpClient) {
        this.receiveHttpClient = receiveHttpClient;
    }

    // Implementation methods
    // -------------------------------------------------------------------------
    protected void doStop(ServiceStopper stopper) throws Exception {
        // TODO
    }

    protected HttpClient createHttpClient() {
        return new HttpClient();
    }

    protected void configureMethod(HttpMethod method) {
        if (sessionID != null) {
            method.addRequestHeader("Cookie", "JSESSIONID=" + sessionID);
        }
        else if (clientID != null) {
            method.setRequestHeader("clientID", clientID);
        }
    }

    protected void checkSession(HttpMethod client) {
        Header header = client.getRequestHeader("Set-Cookie");
        if (header != null) {
            String set_cookie = header.getValue();

            if (set_cookie != null && set_cookie.startsWith("JSESSIONID=")) {
                String[] bits = set_cookie.split("[=;]");
                sessionID = bits[1];
            }
        }
    }

}
