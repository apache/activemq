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

import java.io.DataInputStream;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.net.URI;

import org.apache.activemq.command.ShutdownInfo;
import org.apache.activemq.transport.FutureResponse;
import org.apache.activemq.transport.util.TextWireFormat;
import org.apache.activemq.util.ByteArrayInputStream;
import org.apache.activemq.util.IOExceptionSupport;
import org.apache.activemq.util.IdGenerator;
import org.apache.activemq.util.ServiceStopper;
import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.HttpMethod;
import org.apache.commons.httpclient.HttpStatus;
import org.apache.commons.httpclient.methods.GetMethod;
import org.apache.commons.httpclient.methods.HeadMethod;
import org.apache.commons.httpclient.methods.InputStreamRequestEntity;
import org.apache.commons.httpclient.methods.PostMethod;
import org.apache.commons.httpclient.params.HttpClientParams;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * A HTTP {@link org.apache.activemq.transport.TransportChannel} which uses the
 * <a href="http://jakarta.apache.org/commons/httpclient/">commons-httpclient</a>
 * library
 * 
 * @version $Revision$
 */
public class HttpClientTransport extends HttpTransportSupport {

    public static final int MAX_CLIENT_TIMEOUT = 30000;
    private static final Log LOG = LogFactory.getLog(HttpClientTransport.class);
    private static final IdGenerator CLIENT_ID_GENERATOR = new IdGenerator();

    private HttpClient sendHttpClient;
    private HttpClient receiveHttpClient;

    private final String clientID = CLIENT_ID_GENERATOR.generateId();
    private boolean trace;
    private GetMethod httpMethod;
    private volatile int receiveCounter;
    
    public HttpClientTransport(TextWireFormat wireFormat, URI remoteUrl) {
        super(wireFormat, remoteUrl);
    }

    public FutureResponse asyncRequest(Object command) throws IOException {
        return null;
    }

    public void oneway(Object command) throws IOException {

        if (isStopped()) {
            throw new IOException("stopped.");
        }
        PostMethod httpMethod = new PostMethod(getRemoteUrl().toString());
        configureMethod(httpMethod);
        String data = getTextWireFormat().marshalText(command);
        byte[] bytes = data.getBytes("UTF-8");
        InputStreamRequestEntity entity = new InputStreamRequestEntity(new ByteArrayInputStream(bytes));
        httpMethod.setRequestEntity(entity);

        try {

            HttpClient client = getSendHttpClient();
            HttpClientParams params = new HttpClientParams();
            params.setSoTimeout(MAX_CLIENT_TIMEOUT);
            client.setParams(params);
            int answer = client.executeMethod(httpMethod);
            if (answer != HttpStatus.SC_OK) {
                throw new IOException("Failed to post command: " + command + " as response was: " + answer);
            }
            if (command instanceof ShutdownInfo) {
            	try {
            		stop();
            	} catch (Exception e) {
            		LOG.warn("Error trying to stop HTTP client: "+ e, e);
            	}
            }
        } catch (IOException e) {
            throw IOExceptionSupport.create("Could not post command: " + command + " due to: " + e, e);
        } finally {
            httpMethod.getResponseBody();
            httpMethod.releaseConnection();
        }
    }

    public Object request(Object command) throws IOException {
        return null;
    }

    public void run() {

        LOG.trace("HTTP GET consumer thread starting: " + this);
        HttpClient httpClient = getReceiveHttpClient();
        URI remoteUrl = getRemoteUrl();

        while (!isStopped() && !isStopping()) {

            httpMethod = new GetMethod(remoteUrl.toString());
            configureMethod(httpMethod);

            try {
                int answer = httpClient.executeMethod(httpMethod);
                if (answer != HttpStatus.SC_OK) {
                    if (answer == HttpStatus.SC_REQUEST_TIMEOUT) {
                        LOG.debug("GET timed out");
                        try {
                            Thread.sleep(1000);
                        } catch (InterruptedException e) {
                            onException(new InterruptedIOException());
                            break;
                        }
                    } else {
                        onException(new IOException("Failed to perform GET on: " + remoteUrl + " as response was: " + answer));
                        break;
                    }
                } else {
                    receiveCounter++;
                    DataInputStream stream = new DataInputStream(httpMethod.getResponseBodyAsStream());
                    Object command = (Object)getTextWireFormat().unmarshal(stream);
                    if (command == null) {
                        LOG.debug("Received null command from url: " + remoteUrl);
                    } else {
                        doConsume(command);
                    }
                }
            } catch (IOException e) {
                onException(IOExceptionSupport.create("Failed to perform GET on: " + remoteUrl + " Reason: " + e.getMessage(), e));
                break;
            } finally {
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
    protected void doStart() throws Exception {

        LOG.trace("HTTP GET consumer thread starting: " + this);
        HttpClient httpClient = getReceiveHttpClient();
        URI remoteUrl = getRemoteUrl();

        HeadMethod httpMethod = new HeadMethod(remoteUrl.toString());
        configureMethod(httpMethod);

        int answer = httpClient.executeMethod(httpMethod);
        if (answer != HttpStatus.SC_OK) {
            throw new IOException("Failed to perform GET on: " + remoteUrl + " as response was: " + answer);
        }

        super.doStart();
    }

    protected void doStop(ServiceStopper stopper) throws Exception {
        if (httpMethod != null) {
            httpMethod.abort();
        }
    }

    protected HttpClient createHttpClient() {
        HttpClient client = new HttpClient();
        if (getProxyHost() != null) {
            client.getHostConfiguration().setProxy(getProxyHost(), getProxyPort());
        }
        return client;
    }

    protected void configureMethod(HttpMethod method) {
        method.setRequestHeader("clientID", clientID);
    }

    public boolean isTrace() {
        return trace;
    }

    public void setTrace(boolean trace) {
        this.trace = trace;
    }

    public int getReceiveCounter() {
        return receiveCounter;
    }

}
