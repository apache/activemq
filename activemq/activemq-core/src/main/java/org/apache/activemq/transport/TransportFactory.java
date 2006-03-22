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
package org.apache.activemq.transport;

import edu.emory.mathcs.backport.java.util.concurrent.ConcurrentHashMap;
import edu.emory.mathcs.backport.java.util.concurrent.Executor;

import org.activeio.FactoryFinder;
import org.activeio.command.WireFormat;
import org.activeio.command.WireFormatFactory;
import org.apache.activemq.util.IOExceptionSupport;
import org.apache.activemq.util.IntrospectionSupport;
import org.apache.activemq.util.URISupport;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Map;

public abstract class TransportFactory {

    public abstract TransportServer doBind(String brokerId, URI location) throws IOException;

    public Transport doConnect(URI location, Executor ex) throws Exception {
        return doConnect(location);
    }

    public Transport doCompositeConnect(URI location, Executor ex) throws Exception {
        return doCompositeConnect(location);
    }

    static final private FactoryFinder transportFactoryFinder = new FactoryFinder("META-INF/services/org/apache/activemq/transport/");
    static final private FactoryFinder wireFormatFactoryFinder = new FactoryFinder("META-INF/services/org/apache/activemq/wireformat/");

    static final private ConcurrentHashMap transportFactorys = new ConcurrentHashMap();

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

    public static TransportServer bind(String brokerId, URI location) throws IOException {
        TransportFactory tf = findTransportFactory(location);
        return tf.doBind(brokerId, location);
    }

    public Transport doConnect(URI location) throws Exception {
        try {
            Map options = new HashMap(URISupport.parseParamters(location));
            WireFormat wf = createWireFormat(options);
            Transport transport = createTransport(location, wf);
            Transport rc = configure(transport, wf, options);
            if (!options.isEmpty()) {
                throw new IllegalArgumentException("Invalid connect parameters: " + options);
            }
            return rc;
        }
        catch (URISyntaxException e) {
            throw IOExceptionSupport.create(e);
        }
    }

    public Transport doCompositeConnect(URI location) throws Exception {
        try {
            Map options = new HashMap(URISupport.parseParamters(location));
            WireFormat wf = createWireFormat(options);
            Transport transport = createTransport(location, wf);
            Transport rc = compositeConfigure(transport, wf, options);
            if (!options.isEmpty()) {
                throw new IllegalArgumentException("Invalid connect parameters: " + options);
            }
            return rc;

        }
        catch (URISyntaxException e) {
            throw IOExceptionSupport.create(e);
        }
    }

    /**
     * Factory method to create a new transport
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
        if( scheme == null )
            throw new IOException("Transport not scheme specified: [" + location + "]");
        TransportFactory tf = (TransportFactory) transportFactorys.get(scheme);
        if (tf == null) {
            // Try to load if from a META-INF property.
            try {
                tf = (TransportFactory) transportFactoryFinder.newInstance(scheme);
                transportFactorys.put(scheme, tf);
            }
            catch (Throwable e) {
                throw IOExceptionSupport.create("Transport scheme NOT recognized: [" + scheme + "]", e);
            }
        }
        return tf;
    }

    protected WireFormat createWireFormat(Map options) throws IOException {
        WireFormatFactory factory = createWireFormatFactory(options);
        WireFormat format = factory.createWireFormat();
        return format;
    }

    protected WireFormatFactory createWireFormatFactory(Map options) throws IOException {
        String wireFormat = (String) options.get("wireFormat");
        if (wireFormat == null)
            wireFormat = getDefaultWireFormatType();

        try {
            WireFormatFactory wff = (WireFormatFactory) wireFormatFactoryFinder.newInstance(wireFormat);
            IntrospectionSupport.setProperties(wff, options, "wireFormat.");
            return wff;
        }
        catch (Throwable e) {
            throw IOExceptionSupport.create("Could not create wire format factory for: " + wireFormat + ", reason: " + e, e);
        }
    }

    protected String getDefaultWireFormatType() {
        return "default";
    }

    protected Transport configure(Transport transport, WireFormat wf, Map options) throws Exception {
        IntrospectionSupport.setProperties(transport, options);
        transport = new MutexTransport(transport);
        transport = new ResponseCorrelator(transport);
        return transport;
    }

    public Transport compositeConfigure(Transport transport, WireFormat format, Map options) {
        IntrospectionSupport.setProperties(transport, options);
        return transport;
    }

}
