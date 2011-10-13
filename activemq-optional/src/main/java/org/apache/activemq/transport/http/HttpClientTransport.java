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
import org.apache.activemq.util.IOExceptionSupport;
import org.apache.activemq.util.IdGenerator;
import org.apache.activemq.util.ServiceStopper;
import org.apache.http.HttpHost;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.HttpClient;
import org.apache.http.client.ResponseHandler;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpHead;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.conn.params.ConnRoutePNames;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.impl.client.BasicResponseHandler;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.message.AbstractHttpMessage;
import org.apache.http.params.HttpConnectionParams;
import org.apache.http.params.HttpParams;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A HTTP {@link org.apache.activemq.transport.TransportChannel} which uses the
 * <a href="http://jakarta.apache.org/commons/httpclient/">commons-httpclient</a>
 * library
 */
public class HttpClientTransport extends HttpTransportSupport {

    public static final int MAX_CLIENT_TIMEOUT = 30000;
    private static final Logger LOG = LoggerFactory.getLogger(HttpClientTransport.class);
    private static final IdGenerator CLIENT_ID_GENERATOR = new IdGenerator();

    private HttpClient sendHttpClient;
    private HttpClient receiveHttpClient;

    private final String clientID = CLIENT_ID_GENERATOR.generateId();
    private boolean trace;
    private HttpGet httpMethod;
    private volatile int receiveCounter;

    private int soTimeout = MAX_CLIENT_TIMEOUT;

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
        HttpPost httpMethod = new HttpPost(getRemoteUrl().toString());
        configureMethod(httpMethod);
        String data = getTextWireFormat().marshalText(command);
        byte[] bytes = data.getBytes("UTF-8");
        ByteArrayEntity entity = new ByteArrayEntity(bytes);
        httpMethod.setEntity(entity);

        HttpClient client = null;
        HttpResponse answer = null;
        try {
            client = getSendHttpClient();
            HttpParams params = client.getParams();
            HttpConnectionParams.setSoTimeout(params, soTimeout);
            answer = client.execute(httpMethod);
            int status = answer.getStatusLine().getStatusCode();
            if (status != HttpStatus.SC_OK) {
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
            if (answer != null) {
                EntityUtils.consume(answer.getEntity());
            }
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

            httpMethod = new HttpGet(remoteUrl.toString());
            configureMethod(httpMethod);
            HttpResponse answer = null;

            try {
                answer = httpClient.execute(httpMethod);
                int status = answer.getStatusLine().getStatusCode();
                if (status != HttpStatus.SC_OK) {
                    if (status == HttpStatus.SC_REQUEST_TIMEOUT) {
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
                    DataInputStream stream = new DataInputStream(answer.getEntity().getContent());
                    Object command = (Object)getTextWireFormat().unmarshal(stream);
                    if (command == null) {
                        LOG.debug("Received null command from url: " + remoteUrl);
                    } else {
                        doConsume(command);
                    }
                    stream.close();
                }
            } catch (IOException e) {
                onException(IOExceptionSupport.create("Failed to perform GET on: " + remoteUrl + " Reason: " + e.getMessage(), e));
                break;
            } finally {
                if (answer != null) {
                    try {
                        EntityUtils.consume(answer.getEntity());
                    } catch (IOException e) {
                    }
                }
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

        HttpHead httpMethod = new HttpHead(remoteUrl.toString());
        configureMethod(httpMethod);
        ResponseHandler<String> handler = new BasicResponseHandler();
        try {
            httpClient.execute(httpMethod, handler);
        } catch(Exception e) {
            throw new IOException("Failed to perform GET on: " + remoteUrl + " as response was: " + e.getMessage());
        }

        super.doStart();
    }

    protected void doStop(ServiceStopper stopper) throws Exception {
        if (httpMethod != null) {
            httpMethod.abort();
        }
    }

    protected HttpClient createHttpClient() {
        DefaultHttpClient client = new DefaultHttpClient();
        if (getProxyHost() != null) {
            HttpHost proxy = new HttpHost(getProxyHost(), getProxyPort());
            client.getParams().setParameter(ConnRoutePNames.DEFAULT_PROXY, proxy);

            if(getProxyUser() != null && getProxyPassword() != null) {
                client.getCredentialsProvider().setCredentials(
                    new AuthScope(getProxyHost(), getProxyPort()),
                    new UsernamePasswordCredentials(getProxyUser(), getProxyPassword()));
            }
        }
        return client;
    }

    protected void configureMethod(AbstractHttpMessage method) {
        method.setHeader("clientID", clientID);
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

    public int getSoTimeout() {
        return soTimeout;
    }

    public void setSoTimeout(int soTimeout) {
        this.soTimeout = soTimeout;
    }
}
