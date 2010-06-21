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
package org.apache.activemq.usecases;

import java.io.IOException;
import java.net.InetAddress;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import javax.jms.ExceptionListener;
import javax.jms.JMSException;
import javax.net.SocketFactory;

import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.TransportConnector;
import org.apache.activemq.transport.TransportFactory;
import org.apache.activemq.transport.TransportListener;
import org.apache.activemq.transport.tcp.TcpTransportFactory;
import org.apache.activemq.util.URISupport;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class DurableConsumerCloseAndReconnectTcpTest extends DurableConsumerCloseAndReconnectTest
implements ExceptionListener, TransportListener {
    private static final Log LOG = LogFactory.getLog(DurableConsumerCloseAndReconnectTcpTest.class);
    
    private BrokerService broker;
    private TransportConnector connector;

    private CountDownLatch gotException = new CountDownLatch(1);

    private Exception reconnectException;

    private boolean reconnectInExceptionListener;

    private boolean reconnectInTransportListener;
    
    public void setUp() throws Exception {
        broker = new BrokerService();
        // let the client initiate the inactivity timeout
        connector = broker.addConnector("tcp://localhost:0?transport.useInactivityMonitor=false");
        broker.setPersistent(false);
        broker.start();
        broker.waitUntilStarted();
        
        class SlowCloseSocketTcpTransportFactory extends TcpTransportFactory {

            class SlowCloseSocketFactory extends SocketFactory {
                
                class SlowCloseSocket extends Socket {
                    public SlowCloseSocket(String host, int port) throws IOException {
                        super(host, port);
                    }

                    public SlowCloseSocket(InetAddress host, int port) throws IOException {
                        super(host, port);
                    }

                    public SlowCloseSocket(String host, int port, InetAddress localHost, int localPort) throws IOException {
                        super(host, port, localHost, localPort);
                    }

                    public SlowCloseSocket(InetAddress address, int port, InetAddress localAddress, int localPort) throws IOException {
                        super(address, port, localAddress, localPort);
                    }

                    @Override
                    public synchronized void close() throws IOException {
                        LOG.info("delaying close");
                        try {
                            TimeUnit.MILLISECONDS.sleep(500);
                        } catch (InterruptedException e) {
                            // TODO Auto-generated catch block
                            e.printStackTrace();
                        }
                        super.close();
                    }
                    
                    
                }
                @Override
                public Socket createSocket(String host, int port) throws IOException, UnknownHostException {
                    return new SlowCloseSocket(host, port);
                }

                @Override
                public Socket createSocket(InetAddress host, int port) throws IOException {
                    return new SlowCloseSocket(host, port);
                }

                @Override
                public Socket createSocket(String host, int port, InetAddress localHost, int localPort) throws IOException,
                        UnknownHostException {
                    return new SlowCloseSocket(host, port, localHost, localPort);
                }

                @Override
                public Socket createSocket(InetAddress address, int port, InetAddress localAddress, int localPort) throws IOException {
                    return new SlowCloseSocket(address, port, localAddress, localPort);
                }
                
            }
            @Override
            protected SocketFactory createSocketFactory() throws IOException {
                return new SlowCloseSocketFactory();
            }
            
        }
        
        TransportFactory.registerTransportFactory("tcp", new SlowCloseSocketTcpTransportFactory());
        
    }
    
    public void tearDown() throws Exception {
        broker.stop();
        broker.waitUntilStopped();
    }

    protected ActiveMQConnectionFactory createConnectionFactory() throws Exception {
        return new ActiveMQConnectionFactory(URISupport.removeQuery(connector.getConnectUri()) + "?useKeepAlive=false&wireFormat.maxInactivityDuration=2000");
    }

    @Override
    public void testCreateDurableConsumerCloseThenReconnect() throws Exception {
        reconnectInExceptionListener = true;
        makeConsumer();
        connection.setExceptionListener(this);
        ((ActiveMQConnection)connection).addTransportListener(this);
        assertTrue("inactive connection timedout", gotException.await(30, TimeUnit.SECONDS));
        assertNotNull("Got expected exception on close reconnect overlap: " + reconnectException, reconnectException);
    }

    
    public void testCreateDurableConsumerSlowCloseThenReconnectTransportListener() throws Exception {
        reconnectInTransportListener = true;
        makeConsumer();
        connection.setExceptionListener(this);
        ((ActiveMQConnection)connection).addTransportListener(this);
        assertTrue("inactive connection timedout", gotException.await(30, TimeUnit.SECONDS));
        assertNull("No exception: " + reconnectException, reconnectException);
    }
    
    public void onException(JMSException exception) {
        LOG.info("Exception listener exception:" + exception);
        if (reconnectInExceptionListener) {
            try {
                makeConsumer();
            } catch (Exception e) {
                reconnectException = e;
            }
        
            gotException.countDown();
        }
    }

    public void onCommand(Object command) {}

    public void onException(IOException error) {
       LOG.info("Transport listener exception:" + error);
       if (reconnectInTransportListener) {
           try {
               TimeUnit.MILLISECONDS.sleep(500);
               makeConsumer();
           } catch (Exception e) {
               reconnectException = e;
           }
       
           gotException.countDown();
       }
    }

    public void transportInterupted() {}

    public void transportResumed() {}
}
