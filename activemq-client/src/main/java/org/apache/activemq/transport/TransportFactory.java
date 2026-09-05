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
package org.apache.activemq.transport;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executor;

import org.apache.activemq.broker.SslContext;
import org.apache.activemq.util.FactoryFinder;
import org.apache.activemq.util.IOExceptionSupport;
import org.apache.activemq.util.IntrospectionSupport;
import org.apache.activemq.util.URISupport;
import org.apache.activemq.wireformat.WireFormat;
import org.apache.activemq.wireformat.WireFormatFactory;

public abstract class TransportFactory {

    private static final FactoryFinder<TransportFactory> TRANSPORT_FACTORY_FINDER
            = new FactoryFinder<>("META-INF/services/org/apache/activemq/transport/", TransportFactory.class,
            null);
    private static final FactoryFinder<WireFormatFactory> WIREFORMAT_FACTORY_FINDER
            = new FactoryFinder<>("META-INF/services/org/apache/activemq/wireformat/", WireFormatFactory.class,
            null);
    private static final ConcurrentMap<String, TransportFactory> TRANSPORT_FACTORYS = new ConcurrentHashMap<String, TransportFactory>();

    private static final String WRITE_TIMEOUT_FILTER = "soWriteTimeout";
    private static final String THREAD_NAME_FILTER = "threadName";

    public abstract TransportServer doBind(URI location) throws IOException;

    /**
     * Default implementation delegates to the single-arg method, ignoring the
     * SslContext. Subclasses (e.g. SslTransportFactory) override to use the
     * context for SSL socket creation.
     */
    public TransportServer doBind(URI location, SslContext sslContext) throws IOException {
        return doBind(location);
    }

    /**
     * Default implementation delegates to the single-arg method, ignoring the
     * SslContext. Subclasses (e.g. SslTransportFactory) override to use the
     * context for SSL socket creation.
     */
    public Transport doConnect(URI location, SslContext sslContext) throws IOException, URISyntaxException {
        return doConnect(location);
    }

    /**
     * Default implementation delegates to the single-arg method, ignoring the
     * SslContext. Subclasses (e.g. SslTransportFactory) override to use the
     * context for SSL socket creation.
     */
    public Transport doCompositeConnect(URI location, SslContext sslContext) throws IOException, URISyntaxException {
        return doCompositeConnect(location);
    }

    public Transport doConnect(URI location, Executor ex) throws IOException, URISyntaxException {
        return doConnect(location);
    }

    public Transport doCompositeConnect(URI location, Executor ex) throws IOException, URISyntaxException {
        return doCompositeConnect(location);
    }

    /**
     * Creates a normal transport.
     *
     * @param location
     * @return the transport
     * @throws IOException
     * @throws URISyntaxException
     */
    public static Transport connect(URI location) throws IOException, URISyntaxException {
        TransportFactory tf = findTransportFactory(location);
        return tf.doConnect(location);
    }

    public static Transport connect(URI location, SslContext sslContext) throws IOException, URISyntaxException {
        TransportFactory tf = findTransportFactory(location);
        return tf.doConnect(location, sslContext);
    }

    /**
     * Creates a normal transport.
     *
     * @param location
     * @param ex
     * @return the transport
     * @throws IOException
     * @throws URISyntaxException
     */
    public static Transport connect(URI location, Executor ex) throws IOException, URISyntaxException {
        TransportFactory tf = findTransportFactory(location);
        return tf.doConnect(location, ex);
    }

    /**
     * Creates a slimmed down transport that is more efficient so that it can be
     * used by composite transports like reliable and HA.
     *
     * @param location
     * @return the Transport
     * @throws IOException
     * @throws URISyntaxException
     */
    public static Transport compositeConnect(URI location) throws IOException, URISyntaxException {
        TransportFactory tf = findTransportFactory(location);
        return tf.doCompositeConnect(location);
    }

    public static Transport compositeConnect(URI location, SslContext sslContext) throws IOException, URISyntaxException {
        TransportFactory tf = findTransportFactory(location);
        return tf.doCompositeConnect(location, sslContext);
    }

    /**
     * Creates a slimmed down transport that is more efficient so that it can be
     * used by composite transports like reliable and HA.
     *
     * @param location
     * @param ex
     * @return the Transport
     * @throws IOException
     * @throws URISyntaxException
     */
    public static Transport compositeConnect(URI location, Executor ex) throws IOException, URISyntaxException {
        TransportFactory tf = findTransportFactory(location);
        return tf.doCompositeConnect(location, ex);
    }

    public static TransportServer bind(URI location) throws IOException {
        TransportFactory tf = findTransportFactory(location);
        return tf.doBind(location);
    }

    public Transport doConnect(URI location) throws IOException, URISyntaxException {
        return doConnectInternal(location, null, false);
    }

    public Transport doCompositeConnect(URI location) throws IOException, URISyntaxException {
        return doConnectInternal(location, null, true);
    }

    /**
     * Shared implementation behind {@link #doConnect(URI)} /
     * {@link #doCompositeConnect(URI)} and the SslContext-carrying overrides in
     * SSL capable subclasses. {@code composite} selects the slimmed-down form
     * used by reliable/HA transports (no {@code wireFormat.host} default,
     * {@link #compositeConfigure} instead of {@link #configure}, and no
     * {@code auto.*} strip). The SslContext is threaded to
     * {@link #createTransport(URI, WireFormat, SslContext)} — plain transports
     * ignore it, SSL capable ones derive their socket factory from it — so the
     * connect template lives here once rather than being copied per transport.
     */
    protected Transport doConnectInternal(URI location, SslContext sslContext, boolean composite) throws IOException {
        try {
            Map<String, String> options = new HashMap<String, String>(URISupport.parseParameters(location));
            if (!composite && !options.containsKey("wireFormat.host")) {
                options.put("wireFormat.host", location.getHost());
            }
            WireFormat wf = createWireFormat(options);
            Transport transport = createTransport(location, wf, sslContext);
            Transport rc = composite ? compositeConfigure(transport, wf, options)
                                     : configure(transport, wf, options);
            if (!composite) {
                //remove auto
                IntrospectionSupport.extractProperties(options, "auto.");
            }
            if (!options.isEmpty()) {
                throw new IllegalArgumentException("Invalid connect parameters: " + options);
            }
            return rc;
        } catch (URISyntaxException e) {
            throw IOExceptionSupport.create(e);
        }
    }

     /**
      * Allow registration of a transport factory without wiring via META-INF classes
     * @param scheme
     * @param tf
     */
    public static void registerTransportFactory(String scheme, TransportFactory tf) {
        TRANSPORT_FACTORYS.put(scheme, tf);
      }

    /**
     * Factory method to create a new transport
     *
     * @throws IOException
     */
    protected Transport createTransport(URI location, WireFormat wf) throws IOException {
        throw new IOException("createTransport() method not implemented!");
    }

    /**
     * SSL-aware createTransport override point used by {@link #doConnectInternal}.
     * The default ignores the SslContext and delegates to
     * {@link #createTransport(URI, WireFormat)};
     * SSL capable subclasses (e.g. TcpTransportFactory) override this to derive
     * their socket factory from the context, so the connect template does not
     * have to be duplicated per transport.
     *
     * @param sslContext the SslContext to use, or null for the JVM default.
     */
    protected Transport createTransport(URI location, WireFormat wf, SslContext sslContext) throws IOException {
        return createTransport(location, wf);
    }

    /**
     * @param location
     * @return
     * @throws IOException
     */
    public static TransportFactory findTransportFactory(URI location) throws IOException {
        String scheme = location.getScheme();
        if (scheme == null) {
            throw new IOException("Transport not scheme specified: [" + location + "]");
        }
        TransportFactory tf = TRANSPORT_FACTORYS.get(scheme);
        if (tf == null) {
            // Try to load if from a META-INF property.
            try {
                tf = TRANSPORT_FACTORY_FINDER.newInstance(scheme);
                TRANSPORT_FACTORYS.put(scheme, tf);
            } catch (Throwable e) {
                throw IOExceptionSupport.create("Transport scheme NOT recognized: [" + scheme + "]", e);
            }
        }
        return tf;
    }

    protected WireFormat createWireFormat(Map<String, String> options) throws IOException {
        WireFormatFactory factory = createWireFormatFactory(options);
        WireFormat format = factory.createWireFormat();
        return format;
    }

    protected WireFormatFactory createWireFormatFactory(Map<String, String> options) throws IOException {
        String wireFormat = options.remove("wireFormat");
        if (wireFormat == null) {
            wireFormat = getDefaultWireFormatType();
        }

        try {
            WireFormatFactory wff = WIREFORMAT_FACTORY_FINDER.newInstance(wireFormat);
            IntrospectionSupport.setProperties(wff, options, "wireFormat.");
            return wff;
        } catch (Throwable e) {
            throw IOExceptionSupport.create("Could not create wire format factory for: " + wireFormat + ", reason: " + e, e);
        }
    }

    protected String getDefaultWireFormatType() {
        return "default";
    }

    /**
     * Fully configures and adds all need transport filters so that the
     * transport can be used by the JMS client.
     *
     * @param transport
     * @param wf
     * @param options
     * @return
     * @throws IOException
     */
    @SuppressWarnings("rawtypes")
    public Transport configure(Transport transport, WireFormat wf, Map options) throws IOException {
        transport = compositeConfigure(transport, wf, options);

        transport = new MutexTransport(transport);
        transport = new ResponseCorrelator(transport);

        return transport;
    }

    /**
     * Fully configures and adds all need transport filters so that the
     * transport can be used by the ActiveMQ message broker. The main difference
     * between this and the configure() method is that the broker does not issue
     * requests to the client so the ResponseCorrelator is not needed.
     *
     * @param transport
     * @param format
     * @param options
     * @return
     */
    @SuppressWarnings("rawtypes")
    public Transport serverConfigure(Transport transport, WireFormat format, HashMap options) {
        if (options.containsKey(THREAD_NAME_FILTER)) {
            transport = new ThreadNameFilter(transport);
        }
        transport = compositeConfigure(transport, format, options);
        transport = new MutexTransport(transport);
        return transport;
    }

    /**
     * Similar to configure(...) but this avoid adding in the MutexTransport and
     * ResponseCorrelator transport layers so that the resulting transport can
     * more efficiently be used as part of a composite transport.
     *
     * @param transport
     * @param format
     * @param options
     * @return
     */
    @SuppressWarnings("rawtypes")
    public Transport compositeConfigure(Transport transport, WireFormat format, Map options) {
        if (options.containsKey(WRITE_TIMEOUT_FILTER)) {
            transport = new WriteTimeoutFilter(transport);
            String soWriteTimeout = (String)options.remove(WRITE_TIMEOUT_FILTER);
            if (soWriteTimeout!=null) {
                ((WriteTimeoutFilter)transport).setWriteTimeout(Long.parseLong(soWriteTimeout));
            }
        }
        IntrospectionSupport.setProperties(transport, options);
        return transport;
    }

    @SuppressWarnings("rawtypes")
    protected String getOption(Map options, String key, String def) {
        String rc = (String) options.remove(key);
        if( rc == null ) {
            rc = def;
        }
        return rc;
    }
}
