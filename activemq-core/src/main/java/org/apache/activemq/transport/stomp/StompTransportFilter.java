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
package org.apache.activemq.transport.stomp;

import java.io.IOException;
import java.security.cert.X509Certificate;
import java.util.concurrent.ConcurrentLinkedQueue;

import javax.jms.JMSException;

import org.apache.activemq.broker.BrokerContext;
import org.apache.activemq.command.Command;
import org.apache.activemq.thread.DefaultThreadPools;
import org.apache.activemq.thread.Task;
import org.apache.activemq.thread.TaskRunner;
import org.apache.activemq.transport.Transport;
import org.apache.activemq.transport.TransportFilter;
import org.apache.activemq.transport.TransportListener;
import org.apache.activemq.transport.tcp.SslTransport;
import org.apache.activemq.util.IOExceptionSupport;
import org.apache.activemq.wireformat.WireFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The StompTransportFilter normally sits on top of a TcpTransport that has been
 * configured with the StompWireFormat and is used to convert STOMP commands to
 * ActiveMQ commands. All of the conversion work is done by delegating to the
 * ProtocolConverter.
 *
 * @author <a href="http://hiramchirino.com">chirino</a>
 */
public class StompTransportFilter extends TransportFilter implements StompTransport {
    private static final Logger LOG = LoggerFactory.getLogger(StompTransportFilter.class);
    private static final Logger TRACE = LoggerFactory.getLogger(StompTransportFilter.class.getPackage().getName() + ".StompIO");
    private final ProtocolConverter protocolConverter;
    private StompInactivityMonitor monitor;
    private StompWireFormat wireFormat;
    private final TaskRunner asyncSendTask;
    private final ConcurrentLinkedQueue<Command> asyncCommands = new ConcurrentLinkedQueue<Command>();

    private boolean trace;
    private int maxAsyncBatchSize = 25;

    public StompTransportFilter(Transport next, WireFormat wireFormat, BrokerContext brokerContext) {
        super(next);
        this.protocolConverter = new ProtocolConverter(this, brokerContext);

        asyncSendTask = DefaultThreadPools.getDefaultTaskRunnerFactory().createTaskRunner(new Task() {
            public boolean iterate() {
                int iterations = 0;
                TransportListener listener = transportListener;
                if (listener != null) {
                    while (iterations++ < maxAsyncBatchSize && !asyncCommands.isEmpty()) {
                        Command command = asyncCommands.poll();
                        if (command != null) {
                            listener.onCommand(command);
                        }
                    }
                }
                return !asyncCommands.isEmpty();
            }

        }, "ActiveMQ StompTransport Async Worker: " + System.identityHashCode(this));

        if (wireFormat instanceof StompWireFormat) {
            this.wireFormat = (StompWireFormat) wireFormat;
        }
    }

    public void stop() throws Exception {
        asyncSendTask.shutdown();

        TransportListener listener = transportListener;
        if (listener != null) {
            Command commands[] = new Command[0];
            asyncCommands.toArray(commands);
            asyncCommands.clear();
            for(Command command : commands) {
                try {
                    listener.onCommand(command);
                } catch(Exception e) {
                    break;
                }
            }
        }

        super.stop();
    }

    public void oneway(Object o) throws IOException {
        try {
            final Command command = (Command) o;
            protocolConverter.onActiveMQCommand(command);
        } catch (JMSException e) {
            throw IOExceptionSupport.create(e);
        }
    }

    public void onCommand(Object command) {
        try {
            if (trace) {
                TRACE.trace("Received: \n" + command);
            }

            protocolConverter.onStompCommand((StompFrame) command);
        } catch (IOException e) {
            onException(e);
        } catch (JMSException e) {
            onException(IOExceptionSupport.create(e));
        }
    }

    public void sendToActiveMQ(Command command) {
        TransportListener l = transportListener;
        if (l != null) {
            l.onCommand(command);
        }
    }

    public void asyncSendToActiveMQ(Command command) {
        asyncCommands.offer(command);
        try {
            asyncSendTask.wakeup();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    public void sendToStomp(StompFrame command) throws IOException {
        if (trace) {
            TRACE.trace("Sending: \n" + command);
        }
        Transport n = next;
        if (n != null) {
            n.oneway(command);
        }
    }

    public X509Certificate[] getPeerCertificates() {
        if (next instanceof SslTransport) {
            X509Certificate[] peerCerts = ((SslTransport) next).getPeerCertificates();
            if (trace && peerCerts != null) {
                LOG.debug("Peer Identity has been verified\n");
            }
            return peerCerts;
        }
        return null;
    }

    public boolean isTrace() {
        return trace;
    }

    public void setTrace(boolean trace) {
        this.trace = trace;
    }

    @Override
    public StompInactivityMonitor getInactivityMonitor() {
        return monitor;
    }

    public void setInactivityMonitor(StompInactivityMonitor monitor) {
        this.monitor = monitor;
    }

    @Override
    public StompWireFormat getWireFormat() {
        return this.wireFormat;
    }

    public int getMaxAsyncBatchSize() {
        return maxAsyncBatchSize;
    }

    public void setMaxAsyncBatchSize(int maxAsyncBatchSize) {
        this.maxAsyncBatchSize = maxAsyncBatchSize;
    }
}
