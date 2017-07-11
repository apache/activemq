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
import java.security.cert.X509Certificate;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import org.apache.activemq.command.ShutdownInfo;
import org.apache.activemq.transport.FutureResponse;
import org.apache.activemq.transport.util.TextWireFormat;
import org.apache.activemq.util.ByteArrayOutputStream;
import org.apache.activemq.util.IOExceptionSupport;
import org.apache.activemq.util.IdGenerator;
import org.apache.activemq.util.ServiceStopper;
import org.apache.activemq.wireformat.WireFormat;
import org.apache.http.Header;
import org.apache.http.HttpHost;
import org.apache.http.HttpRequest;
import org.apache.http.HttpRequestInterceptor;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.HttpClient;
import org.apache.http.client.HttpResponseException;
import org.apache.http.client.ResponseHandler;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpHead;
import org.apache.http.client.methods.HttpOptions;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.params.CookiePolicy;
import org.apache.http.client.params.HttpClientParams;
import org.apache.http.conn.ClientConnectionManager;
import org.apache.http.conn.params.ConnRoutePNames;
import org.apache.http.conn.scheme.PlainSocketFactory;
import org.apache.http.conn.scheme.Scheme;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.impl.client.BasicResponseHandler;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.impl.conn.PoolingClientConnectionManager;
import org.apache.http.message.AbstractHttpMessage;
import org.apache.http.params.HttpConnectionParams;
import org.apache.http.params.HttpParams;
import org.apache.http.protocol.HttpContext;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A HTTP {@link org.apache.activemq.transport.Transport} which uses the
 * <a href="http://hc.apache.org/index.html">Apache HTTP Client</a>
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

    private boolean useCompression = false;
    protected boolean canSendCompressed = false;
    private int minSendAsCompressedSize = 0;

    public HttpClientTransport(TextWireFormat wireFormat, URI remoteUrl) {
        super(wireFormat, remoteUrl);
    }

    public FutureResponse asyncRequest(Object command) throws IOException {
        return null;
    }

    @Override
    public void oneway(Object command) throws IOException {

        if (isStopped()) {
            throw new IOException("stopped.");
        }
        HttpPost httpMethod = new HttpPost(getRemoteUrl().toString());
        configureMethod(httpMethod);
        String data = getTextWireFormat().marshalText(command);
        byte[] bytes = data.getBytes("UTF-8");
        if (useCompression && canSendCompressed && bytes.length > minSendAsCompressedSize) {
            ByteArrayOutputStream bytesOut = new ByteArrayOutputStream();
            GZIPOutputStream stream = new GZIPOutputStream(bytesOut);
            stream.write(bytes);
            stream.close();
            httpMethod.addHeader("Content-Type", "application/x-gzip");
            if (LOG.isTraceEnabled()) {
                LOG.trace("Sending compressed, size = " + bytes.length + ", compressed size = " + bytesOut.size());
            }
            bytes = bytesOut.toByteArray();
        }
        ByteArrayEntity entity = new ByteArrayEntity(bytes);
        httpMethod.setEntity(entity);

        HttpClient client = null;
        HttpResponse answer = null;
        try {
            client = getSendHttpClient();
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

    @Override
    public Object request(Object command) throws IOException {
        return null;
    }

    private DataInputStream createDataInputStream(HttpResponse answer) throws IOException {
        Header encoding = answer.getEntity().getContentEncoding();
        if (encoding != null && "gzip".equalsIgnoreCase(encoding.getValue())) {
            return new DataInputStream(new GZIPInputStream(answer.getEntity().getContent()));
        } else {
            return new DataInputStream(answer.getEntity().getContent());
        }
    }

    @Override
    public void run() {

        if (LOG.isTraceEnabled()) {
            LOG.trace("HTTP GET consumer thread starting: " + this);
        }
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
                            Thread.currentThread().interrupt();
                            break;
                        }
                    } else {
                        onException(new IOException("Failed to perform GET on: " + remoteUrl + " as response was: " + answer));
                        break;
                    }
                } else {
                    receiveCounter++;
                    DataInputStream stream = createDataInputStream(answer);
                    Object command = getTextWireFormat().unmarshal(stream);
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
    @Override
    protected void doStart() throws Exception {

        if (LOG.isTraceEnabled()) {
            LOG.trace("HTTP GET consumer thread starting: " + this);
        }
        HttpClient httpClient = getReceiveHttpClient();
        URI remoteUrl = getRemoteUrl();

        HttpHead httpMethod = new HttpHead(remoteUrl.toString());
        configureMethod(httpMethod);

        // Request the options from the server so we can find out if the broker we are
        // talking to supports GZip compressed content.  If so and useCompression is on
        // then we can compress our POST data, otherwise we must send it uncompressed to
        // ensure backwards compatibility.
        HttpOptions optionsMethod = new HttpOptions(remoteUrl.toString());
        ResponseHandler<String> handler = new BasicResponseHandler() {
            @Override
            public String handleResponse(HttpResponse response) throws HttpResponseException, IOException {

                for(Header header : response.getAllHeaders()) {
                    if (header.getName().equals("Accepts-Encoding") && header.getValue().contains("gzip")) {
                        LOG.info("Broker Servlet supports GZip compression.");
                        canSendCompressed = true;
                        break;
                    }
                }

                return super.handleResponse(response);
            }
        };

        try {
            httpClient.execute(httpMethod, new BasicResponseHandler());
            httpClient.execute(optionsMethod, handler);
        } catch(Exception e) {
            LOG.trace("Error on start: ", e);
            throw new IOException("Failed to perform GET on: " + remoteUrl + " as response was: " + e.getMessage());
        }

        super.doStart();
    }

    @Override
    protected void doStop(ServiceStopper stopper) throws Exception {
        if (httpMethod != null) {
            // In some versions of the JVM a race between the httpMethod and the completion
            // of the method when using HTTPS can lead to a deadlock.  This hack attempts to
            // detect that and interrupt the thread that's locked so that they can complete
            // on another attempt.
            for (int i = 0; i < 3; ++i) {
                Thread abortThread = new Thread(new Runnable() {

                    @Override
                    public void run() {
                        try {
                            httpMethod.abort();
                        } catch (Exception e) {
                        }
                    }
                });

                abortThread.start();
                abortThread.join(2000);
                if (abortThread.isAlive() && !httpMethod.isAborted()) {
                    abortThread.interrupt();
                }
            }
        }
    }

    protected HttpClient createHttpClient() {
        DefaultHttpClient client = new DefaultHttpClient(createClientConnectionManager());
        if (useCompression) {
            client.addRequestInterceptor( new HttpRequestInterceptor() {
                @Override
                public void process(HttpRequest request, HttpContext context) {
                    // We expect to received a compression response that we un-gzip
                    request.addHeader("Accept-Encoding", "gzip");
                }
            });
        }
        if (getProxyHost() != null) {
            HttpHost proxy = new HttpHost(getProxyHost(), getProxyPort());
            client.getParams().setParameter(ConnRoutePNames.DEFAULT_PROXY, proxy);

            if (client.getConnectionManager().getSchemeRegistry().get("http") == null) {
                client.getConnectionManager().getSchemeRegistry().register(
                    new Scheme("http", getProxyPort(), PlainSocketFactory.getSocketFactory()));
            }

            if(getProxyUser() != null && getProxyPassword() != null) {
                client.getCredentialsProvider().setCredentials(
                    new AuthScope(getProxyHost(), getProxyPort()),
                    new UsernamePasswordCredentials(getProxyUser(), getProxyPassword()));
            }
        }

        HttpParams params = client.getParams();
        HttpConnectionParams.setSoTimeout(params, soTimeout);
        HttpClientParams.setCookiePolicy(params, CookiePolicy.BROWSER_COMPATIBILITY);

        return client;
    }

    protected ClientConnectionManager createClientConnectionManager() {
        return new PoolingClientConnectionManager();
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

    @Override
    public int getReceiveCounter() {
        return receiveCounter;
    }

    public int getSoTimeout() {
        return soTimeout;
    }

    public void setSoTimeout(int soTimeout) {
        this.soTimeout = soTimeout;
    }

    public void setUseCompression(boolean useCompression) {
        this.useCompression = useCompression;
    }

    public boolean isUseCompression() {
        return this.useCompression;
    }

    public int getMinSendAsCompressedSize() {
        return minSendAsCompressedSize;
    }

    /**
     * Sets the minimum size that must be exceeded on a send before compression is used if
     * the useCompression option is specified.  For very small payloads compression can be
     * inefficient compared to the transmission size savings.
     *
     * Default value is 0.
     *
     * @param minSendAsCompressedSize
     */
    public void setMinSendAsCompressedSize(int minSendAsCompressedSize) {
        this.minSendAsCompressedSize = minSendAsCompressedSize;
    }

    @Override
    public X509Certificate[] getPeerCertificates() {
        return null;
    }

    @Override
    public void setPeerCertificates(X509Certificate[] certificates) {
    }

    @Override
    public WireFormat getWireFormat() {
        return getTextWireFormat();
    }
}
