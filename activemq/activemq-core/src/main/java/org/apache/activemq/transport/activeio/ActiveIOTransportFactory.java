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
package org.apache.activemq.transport.activeio;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Map;

import org.activeio.AsyncChannel;
import org.activeio.Channel;
import org.activeio.ChannelFactory;
import org.activeio.adapter.SyncToAsyncChannel;
import org.activeio.command.AsyncChannelToAsyncCommandChannel;
import org.activeio.command.WireFormat;
import org.activeio.net.SocketMetadata;
import org.apache.activemq.transport.InactivityMonitor;
import org.apache.activemq.transport.MutexTransport;
import org.apache.activemq.transport.ResponseCorrelator;
import org.apache.activemq.transport.Transport;
import org.apache.activemq.transport.TransportFactory;
import org.apache.activemq.transport.TransportLogger;
import org.apache.activemq.transport.TransportServer;
import org.apache.activemq.transport.WireFormatNegotiator;
import org.apache.activemq.util.IOExceptionSupport;
import org.apache.activemq.util.IntrospectionSupport;
import org.apache.activemq.util.URISupport;

import edu.emory.mathcs.backport.java.util.concurrent.Executor;

public class ActiveIOTransportFactory extends TransportFactory {

    public Transport doConnect(URI location) throws IOException {
        try {
            Map options = new HashMap(URISupport.parseParamters(location));
            location = convertToActiveIOURI(location);
            Transport rc = connect(location, createWireFormat(options), options);
            if( !options.isEmpty() ) {
                throw new IllegalArgumentException("Invalid connect parameters: "+options);
            }
            return rc;
        } catch (URISyntaxException e) {
            throw IOExceptionSupport.create(e);
        }
    }
    
    /**
     * Do a connect to get the transport
     * @param location 
     * @param ex 
     * @return the transport
     * @throws IOException 
     * 
     */
    public Transport doConnect(URI location,Executor ex) throws IOException {
        try {
            Map options = new HashMap(URISupport.parseParamters(location));
            location = convertToActiveIOURI(location);
            Transport rc = connect(location, createWireFormat(options), options,ex);
            if( !options.isEmpty() ) {
                throw new IllegalArgumentException("Invalid connect parameters: "+options);
            }
            return rc;
        } catch (URISyntaxException e) {
            throw IOExceptionSupport.create(e);
        }
    }
    
    /**
     * do a Composite connect
     * @param location 
     * @return the Transport
     * @throws IOException 
     * 
     */
    public Transport doCompositeConnect(URI location) throws IOException {
        try {
            Map options = URISupport.parseParamters(location);
            location = convertToActiveIOURI(location);
            Transport rc = compositeConnect(location, createWireFormat(options), options);
            if( !options.isEmpty() ) {
                throw new IllegalArgumentException("Invalid connect parameters: "+options);
            }
            return rc;
        } catch (URISyntaxException e) {
            throw IOExceptionSupport.create(e);
        }
    }
    
    /**
     * Do a Composite Connect
     * @param location 
     * @param ex 
     * @return the Transport
     * @throws IOException 
     * 
     */
    public Transport doCompositeConnect(URI location,Executor ex) throws IOException {
        try {
            Map options = URISupport.parseParamters(location);
            location = convertToActiveIOURI(location);
            Transport rc = compositeConnect(location, createWireFormat(options), options,ex);
            if( !options.isEmpty() ) {
                throw new IllegalArgumentException("Invalid connect parameters: "+options);
            }
            return rc;
        } catch (URISyntaxException e) {
            throw IOExceptionSupport.create(e);
        }
    }

    public TransportServer doBind(String brokerId,final URI location) throws IOException {
        try {
            Map options = new HashMap(URISupport.parseParamters(location));
            
            ActiveIOTransportServer server = new ActiveIOTransportServer(convertToActiveIOURI(location), options) {
                public URI getBindURI() {
                    return location;
                }
                public URI getConnectURI() {
                    return convertFromActiveIOURI(super.getConnectURI());
                }
            };
            server.setWireFormatFactory(createWireFormatFactory(options));            
            IntrospectionSupport.setProperties(server, options);
                        
            return server;
        } catch (URISyntaxException e) {
            throw IOExceptionSupport.create(e);
        }
    }


    static private final HashMap toSchemeMap = new HashMap(); 
    static private final HashMap fromSchemeMap = new HashMap(); 
    static {
        toSchemeMap.put("tcp", "socket");
        fromSchemeMap.put("socket", "tcp");
        toSchemeMap.put("nio", "nio-async");
        fromSchemeMap.put("nio-async", "nio");
        toSchemeMap.put("aio", "aio");
        fromSchemeMap.put("aio", "aio");
        toSchemeMap.put("ssl", "ssl");
        fromSchemeMap.put("ssl", "ssl");
        toSchemeMap.put("vmpipe", "vmpipe");
        fromSchemeMap.put("vmpipe", "vmpipe");
        toSchemeMap.put("jxta", "jxta");
        fromSchemeMap.put("jxta", "jxta");
    }
    
    static URI convertToActiveIOURI(URI location) {
        try {
            String scheme = location.getScheme();
            String target = (String) toSchemeMap.get(scheme);
            if( target!=null ) {
                return new URI(target,
                        location.getSchemeSpecificPart(), 
                        location.getFragment());
            }
        } catch (URISyntaxException e) {
        }
        return location;
    }
    
    static URI convertFromActiveIOURI(URI location) {
        try {
            String scheme = location.getScheme();
            String target = (String) fromSchemeMap.get(scheme);
            if( target!=null ) {
                return new URI(target,
                        location.getSchemeSpecificPart(), 
                        location.getFragment());
            }
        } catch (URISyntaxException e) {
        }
        return location;
    }

    public static Transport connect(URI location, WireFormat format, Map options) throws IOException {
        return configure(new ChannelFactory().openAsyncChannel(location), format, options);
    }
    
    public static Transport connect(URI location, WireFormat format, Map options,Executor ex) throws IOException {
        return configure(new ChannelFactory().openAsyncChannel(location), format, options,ex);
    }
    
    public static Transport configure(Channel c, WireFormat format, Map options) {
        AsyncChannel channel = SyncToAsyncChannel.adapt(c); 
        return configure(channel,format,options);
    }
    
    public static Transport configure(Channel c, WireFormat format, Map options,Executor ex) {
        AsyncChannel channel = SyncToAsyncChannel.adapt(c,ex); 
        return configure(channel,format,options);
    }
    public static Transport configure(AsyncChannel channel, WireFormat format, Map options) {
                     
        ActivityMonitor activityMonitor = new ActivityMonitor(channel);
        channel = new PacketAggregatingAsyncChannel(activityMonitor);
        AsyncChannelToAsyncCommandChannel commandChannel = new AsyncChannelToAsyncCommandChannel(channel,format);
        
        // Flexible but dangerous!  Allows you to configure all the properties of the socket via the URI!
        SocketMetadata socketMetadata = (SocketMetadata)commandChannel.getAdapter(SocketMetadata.class);
        if( socketMetadata !=null ) {
            IntrospectionSupport.setProperties(socketMetadata, options);            
        }
        
        ActiveIOTransport activeIOTransport = new ActiveIOTransport(commandChannel);
        IntrospectionSupport.setProperties(activeIOTransport, options);
        activeIOTransport.setReadCounter(activityMonitor.getReadCounter());
        activeIOTransport.setWriteCounter(activityMonitor.getWriteCounter());
        
        Transport transport = activeIOTransport;
        if( activeIOTransport.isTrace() ) {
            transport = new TransportLogger(transport);
        }
        transport = new InactivityMonitor(transport, activeIOTransport.getMaxInactivityDuration(), activityMonitor.getReadCounter(), activityMonitor.getWriteCounter());
        transport = new WireFormatNegotiator(transport,format, activeIOTransport.getMinmumWireFormatVersion());
        transport = new MutexTransport(transport);
        transport = new ResponseCorrelator(transport);
        return transport;
    }
    
    public static Transport compositeConnect(URI location, WireFormat format, Map options) throws IOException {
        return compositeConfigure(new ChannelFactory().openAsyncChannel(location), format, options);
    }
    
    public static Transport compositeConnect(URI location, WireFormat format, Map options,Executor ex) throws IOException {
        return compositeConfigure(new ChannelFactory().openAsyncChannel(location), format, options,ex);
    }

    public static Transport compositeConfigure(Channel c, org.activeio.command.WireFormat format, Map options) {
        AsyncChannel channel = SyncToAsyncChannel.adapt(c); 
        return compositeConfigure(channel, format, options);
    }
    
    public static Transport compositeConfigure(Channel c, org.activeio.command.WireFormat format, Map options,Executor ex) {
        AsyncChannel channel = SyncToAsyncChannel.adapt(c,ex); 
        return compositeConfigure(channel, format, options);
    }
    
    
    public static Transport compositeConfigure(AsyncChannel channel, org.activeio.command.WireFormat format, Map options) {
        ActivityMonitor activityMonitor = new ActivityMonitor(channel);
        channel = new PacketAggregatingAsyncChannel(activityMonitor);
        AsyncChannelToAsyncCommandChannel commandChannel = new AsyncChannelToAsyncCommandChannel(channel,format);

        // Flexible but dangerous!  Allows you to configure all the properties of the socket via the URI!
        SocketMetadata socketMetadata = (SocketMetadata)commandChannel.getAdapter(SocketMetadata.class);
        if( socketMetadata !=null ) {
            IntrospectionSupport.setProperties(socketMetadata, options, "socket.");            
        }
        
        ActiveIOTransport activeIOTransport = new ActiveIOTransport(commandChannel);
        IntrospectionSupport.setProperties(activeIOTransport, options);
        activeIOTransport.setReadCounter(activityMonitor.getReadCounter());
        activeIOTransport.setWriteCounter(activityMonitor.getWriteCounter());

        Transport transport = activeIOTransport;
        if( activeIOTransport.isTrace() ) {
            transport = new TransportLogger(transport);
        }
        transport = new InactivityMonitor(transport, activeIOTransport.getMaxInactivityDuration(), activityMonitor.getReadCounter(), activityMonitor.getWriteCounter());
        transport = new WireFormatNegotiator(transport,format, activeIOTransport.getMinmumWireFormatVersion());
        return transport;        
    }
    

}
