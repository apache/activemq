/**
 *
 * Copyright 2004 The Apache Software Foundation
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
import java.util.Map;

import org.activeio.AcceptListener;
import org.activeio.AsyncChannelServer;
import org.activeio.Channel;
import org.activeio.ChannelFactory;
import org.activeio.command.WireFormat;
import org.activeio.command.WireFormatFactory;
import org.apache.activemq.ThreadPriorities;
import org.apache.activemq.command.BrokerInfo;
import org.apache.activemq.openwire.OpenWireFormatFactory;
import org.apache.activemq.transport.TransportAcceptListener;
import org.apache.activemq.transport.TransportServer;

import edu.emory.mathcs.backport.java.util.concurrent.Executor;
import edu.emory.mathcs.backport.java.util.concurrent.ScheduledThreadPoolExecutor;
import edu.emory.mathcs.backport.java.util.concurrent.ThreadFactory;

public class ActiveIOTransportServer implements TransportServer {

    private AsyncChannelServer server;
    private TransportAcceptListener acceptListener;
    private WireFormatFactory wireFormatFactory = new OpenWireFormatFactory();
    private long stopTimeout = 2000;
    static protected final Executor BROKER_CONNECTION_EXECUTOR = new ScheduledThreadPoolExecutor(5, new ThreadFactory() {
            public Thread newThread(Runnable run) {
                Thread thread = new Thread(run);
                thread.setPriority(ThreadPriorities.INBOUND_BROKER_CONNECTION);
                return thread;
            }
        });

    /**
     * @param location
     * @throws IOException 
     */
    public ActiveIOTransportServer(URI location, final Map options) throws IOException {
        server = new ChannelFactory().bindAsyncChannel(location);
        server.setAcceptListener(new AcceptListener(){
            public void onAccept(Channel c) {
                if( acceptListener==null ) {
                    c.dispose();
                } else {
                    WireFormat format = (WireFormat) wireFormatFactory.createWireFormat();
                    acceptListener.onAccept( ActiveIOTransportFactory.configure(c, format, options,BROKER_CONNECTION_EXECUTOR));
                }
            }
            public void onAcceptError(IOException error) {
                if( acceptListener!=null ) {
                    acceptListener.onAcceptError(error);
                }
            }
        });
    }
    
    public void setAcceptListener(TransportAcceptListener acceptListener) {
        this.acceptListener = acceptListener;        
    }

    public void start() throws Exception {
        server.start();
    }

    public void stop() throws Exception {
        server.stop(stopTimeout);
        server.dispose();
    }

    public URI getConnectURI() {
        return server.getConnectURI();
    }
    
    public URI getBindURI() {
        return server.getBindURI();
    }

    public WireFormatFactory getWireFormatFactory() {
        return wireFormatFactory;
    }

    public void setWireFormatFactory(WireFormatFactory wireFormatFactory) {
        this.wireFormatFactory = wireFormatFactory;
    }

    public void setBrokerInfo(BrokerInfo brokerInfo) {
    }

    public long getStopTimeout() {
        return stopTimeout;
    }

    public void setStopTimeout(long stopTimeout) {
        this.stopTimeout = stopTimeout;
    }
    
    
}
