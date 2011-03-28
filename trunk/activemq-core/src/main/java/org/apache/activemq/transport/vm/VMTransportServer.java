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
package org.apache.activemq.transport.vm;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.activemq.command.BrokerInfo;
import org.apache.activemq.transport.MutexTransport;
import org.apache.activemq.transport.ResponseCorrelator;
import org.apache.activemq.transport.Transport;
import org.apache.activemq.transport.TransportAcceptListener;
import org.apache.activemq.transport.TransportServer;

/**
 * Broker side of the VMTransport
 */
public class VMTransportServer implements TransportServer {

    private TransportAcceptListener acceptListener;
    private final URI location;
    private boolean disposed;

    private final AtomicInteger connectionCount = new AtomicInteger(0);
    private final boolean disposeOnDisconnect;

    /**
     * @param location
     * @param disposeOnDisconnect
     */
    public VMTransportServer(URI location, boolean disposeOnDisconnect) {
        this.location = location;
        this.disposeOnDisconnect = disposeOnDisconnect;
    }

    /**
     * @return a pretty print of this
     */
    public String toString() {
        return "VMTransportServer(" + location + ")";
    }

    /**
     * @return new VMTransport
     * @throws IOException
     */
    public VMTransport connect() throws IOException {
        TransportAcceptListener al;
        synchronized (this) {
            if (disposed) {
                throw new IOException("Server has been disposed.");
            }
            al = acceptListener;
        }
        if (al == null) {
            throw new IOException("Server TransportAcceptListener is null.");
        }

        connectionCount.incrementAndGet();
        VMTransport client = new VMTransport(location) {
            public void stop() throws Exception {
            	if (stopping.compareAndSet(false, true) && !disposed) {
					super.stop();
					if (connectionCount.decrementAndGet() == 0
							&& disposeOnDisconnect) {
						VMTransportServer.this.stop();
					}
				}
            };
        };

        VMTransport server = new VMTransport(location);
        client.setPeer(server);
        server.setPeer(client);
        al.onAccept(configure(server));
        return client;
    }

    /**
     * Configure transport
     * 
     * @param transport
     * @return the Transport
     */
    public static Transport configure(Transport transport) {
        transport = new MutexTransport(transport);
        transport = new ResponseCorrelator(transport);
        return transport;
    }

    /**
     * Set the Transport accept listener for new Connections
     * 
     * @param acceptListener
     */
    public synchronized void setAcceptListener(TransportAcceptListener acceptListener) {
        this.acceptListener = acceptListener;
    }

    public void start() throws IOException {
    }

    public void stop() throws IOException {
        VMTransportFactory.stopped(this);
    }

    public URI getConnectURI() {
        return location;
    }

    public URI getBindURI() {
        return location;
    }

    public void setBrokerInfo(BrokerInfo brokerInfo) {
    }

    public InetSocketAddress getSocketAddress() {
        return null;
    }
    
    public int getConnectionCount() {
        return connectionCount.intValue();
    }
}
