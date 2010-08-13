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
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;

import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.BrokerServiceAware;
import org.apache.activemq.broker.SslContext;
import org.apache.activemq.util.FactoryFinder;
import org.apache.activemq.util.IOExceptionSupport;
import org.apache.activemq.util.IntrospectionSupport;
import org.apache.activemq.util.URISupport;
import org.apache.activemq.wireformat.WireFormat;
import org.apache.activemq.wireformat.WireFormatFactory;

public abstract class TransportFactory {

    private static final FactoryFinder TRANSPORT_FACTORY_FINDER = new FactoryFinder("META-INF/services/org/apache/activemq/transport/");
    private static final FactoryFinder WIREFORMAT_FACTORY_FINDER = new FactoryFinder("META-INF/services/org/apache/activemq/wireformat/");
    private static final ConcurrentHashMap<String, TransportFactory> TRANSPORT_FACTORYS = new ConcurrentHashMap<String, TransportFactory>();

    private static final String WRITE_TIMEOUT_FILTER = "soWriteTimeout";
    private static final String THREAD_NAME_FILTER = "threadName";
    
    public abstract TransportServer doBind(URI location) throws IOException;

    public Transport doConnect(URI location, Executor ex) throws Exception {
        return doConnect(location);
    }

    public Transport doCompositeConnect(URI location, Executor ex) throws Exception {
        return doCompositeConnect(location);
    }

    /**
     * Creates a normal transport.
     * 
     * @param location
     * @return the transport
     * @throws Exception
     */
    public static Transport connect(URI location) throws Exception {
        TransportFactory tf = findTransportFactory(location);
        return tf.doConnect(location);
    }

    /**
     * Creates a normal transport.
     * 
     * @param location
     * @param ex
     * @return the transport
     * @throws Exception
     */
    public static Transport connect(URI location, Executor ex) throws Exception {
        TransportFactory tf = findTransportFactory(location);
        return tf.doConnect(location, ex);
    }

    /**
     * Creates a slimmed down transport that is more efficient so that it can be
     * used by composite transports like reliable and HA.
     * 
     * @param location
     * @return the Transport
     * @throws Exception
     */
    public static Transport compositeConnect(URI location) throws Exception {
        TransportFactory tf = findTransportFactory(location);
        return tf.doCompositeConnect(location);
    }

    /**
     * Creates a slimmed down transport that is more efficient so that it can be
     * used by composite transports like reliable and HA.
     * 
     * @param location
     * @param ex
     * @return the Transport
     * @throws Exception
     */
    public static Transport compositeConnect(URI location, Executor ex) throws Exception {
        TransportFactory tf = findTransportFactory(location);
        return tf.doCompositeConnect(location, ex);
    }

    public static TransportServer bind(URI location) throws IOException {
        TransportFactory tf = findTransportFactory(location);
        return tf.doBind(location);
    }

    /**
     * @deprecated 
     */
    public static TransportServer bind(String brokerId, URI location) throws IOException {
        return bind(location);
    }
    
    public static TransportServer bind(BrokerService brokerService, URI location) throws IOException {
        TransportFactory tf = findTransportFactory(location);
        if( brokerService!=null && tf instanceof BrokerServiceAware ) {
            ((BrokerServiceAware)tf).setBrokerService(brokerService);
        }
        try {
            if( brokerService!=null ) {
                SslContext.setCurrentSslContext(brokerService.getSslContext());
            }
            return tf.doBind(location);
        } finally {
            SslContext.setCurrentSslContext(null);
        }
    }    

    public Transport doConnect(URI location) throws Exception {
        try {
            Map<String, String> options = new HashMap<String, String>(URISupport.parseParameters(location));
            WireFormat wf = createWireFormat(options);
            Transport transport = createTransport(location, wf);
            Transport rc = configure(transport, wf, options);
            if (!options.isEmpty()) {
                throw new IllegalArgumentException("Invalid connect parameters: " + options);
            }
            return rc;
        } catch (URISyntaxException e) {
            throw IOExceptionSupport.create(e);
        }
    }

    public Transport doCompositeConnect(URI location) throws Exception {
        try {
            Map<String, String> options = new HashMap<String, String>(URISupport.parseParameters(location));
            WireFormat wf = createWireFormat(options);
            Transport transport = createTransport(location, wf);
            Transport rc = compositeConfigure(transport, wf, options);
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
     * @throws UnknownHostException
     */
    protected Transport createTransport(URI location, WireFormat wf) throws MalformedURLException, UnknownHostException, IOException {
        throw new IOException("createTransport() method not implemented!");
    }

    /**
     * @param location
     * @return
     * @throws IOException
     */
    private static TransportFactory findTransportFactory(URI location) throws IOException {
        String scheme = location.getScheme();
        if (scheme == null) {
            throw new IOException("Transport not scheme specified: [" + location + "]");
        }
        TransportFactory tf = TRANSPORT_FACTORYS.get(scheme);
        if (tf == null) {
            // Try to load if from a META-INF property.
            try {
                tf = (TransportFactory)TRANSPORT_FACTORY_FINDER.newInstance(scheme);
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
        String wireFormat = (String)options.remove("wireFormat");
        if (wireFormat == null) {
            wireFormat = getDefaultWireFormatType();
        }

        try {
            WireFormatFactory wff = (WireFormatFactory)WIREFORMAT_FACTORY_FINDER.newInstance(wireFormat);
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
     * @throws Exception
     */
    public Transport configure(Transport transport, WireFormat wf, Map options) throws Exception {
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
     * @throws Exception
     */
    public Transport serverConfigure(Transport transport, WireFormat format, HashMap options) throws Exception {
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

}
