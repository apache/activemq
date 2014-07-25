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
package org.apache.activemq.network;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.activemq.Service;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.command.ActiveMQTopic;
import org.apache.activemq.command.BrokerId;
import org.apache.activemq.command.BrokerInfo;
import org.apache.activemq.command.Command;
import org.apache.activemq.command.ConnectionId;
import org.apache.activemq.command.ConnectionInfo;
import org.apache.activemq.command.ConsumerInfo;
import org.apache.activemq.command.ExceptionResponse;
import org.apache.activemq.command.Message;
import org.apache.activemq.command.MessageAck;
import org.apache.activemq.command.MessageDispatch;
import org.apache.activemq.command.ProducerInfo;
import org.apache.activemq.command.Response;
import org.apache.activemq.command.SessionInfo;
import org.apache.activemq.command.ShutdownInfo;
import org.apache.activemq.transport.DefaultTransportListener;
import org.apache.activemq.transport.FutureResponse;
import org.apache.activemq.transport.ResponseCallback;
import org.apache.activemq.transport.Transport;
import org.apache.activemq.util.IdGenerator;
import org.apache.activemq.util.ServiceStopper;
import org.apache.activemq.util.ServiceSupport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Forwards all messages from the local broker to the remote broker.
 *
 * @org.apache.xbean.XBean
 *
 */
public class ForwardingBridge implements Service {

    private static final IdGenerator ID_GENERATOR = new IdGenerator();
    private static final Logger LOG = LoggerFactory.getLogger(ForwardingBridge.class);

    final AtomicLong enqueueCounter = new AtomicLong();
    final AtomicLong dequeueCounter = new AtomicLong();
    ConnectionInfo connectionInfo;
    SessionInfo sessionInfo;
    ProducerInfo producerInfo;
    ConsumerInfo queueConsumerInfo;
    ConsumerInfo topicConsumerInfo;
    BrokerId localBrokerId;
    BrokerId remoteBrokerId;
    BrokerInfo localBrokerInfo;
    BrokerInfo remoteBrokerInfo;

    private final Transport localBroker;
    private final Transport remoteBroker;
    private String clientId;
    private int prefetchSize = 1000;
    private boolean dispatchAsync;
    private String destinationFilter = ">";
    private NetworkBridgeListener bridgeFailedListener;
    private boolean useCompression = false;

    public ForwardingBridge(Transport localBroker, Transport remoteBroker) {
        this.localBroker = localBroker;
        this.remoteBroker = remoteBroker;
    }

    public void start() throws Exception {
        LOG.info("Starting a network connection between {} and {} has been established.", localBroker, remoteBroker);

        localBroker.setTransportListener(new DefaultTransportListener() {
            public void onCommand(Object o) {
                Command command = (Command)o;
                serviceLocalCommand(command);
            }

            public void onException(IOException error) {
                serviceLocalException(error);
            }
        });

        remoteBroker.setTransportListener(new DefaultTransportListener() {
            public void onCommand(Object o) {
                Command command = (Command)o;
                serviceRemoteCommand(command);
            }

            public void onException(IOException error) {
                serviceRemoteException(error);
            }
        });

        localBroker.start();
        remoteBroker.start();
    }

    protected void triggerStartBridge() throws IOException {
        Thread thead = new Thread() {
            public void run() {
                try {
                    startBridge();
                } catch (IOException e) {
                    LOG.error("Failed to start network bridge: ", e);
                }
            }
        };
        thead.start();
    }

    /**
     * @throws IOException
     */
    final void startBridge() throws IOException {
        connectionInfo = new ConnectionInfo();
        connectionInfo.setConnectionId(new ConnectionId(ID_GENERATOR.generateId()));
        connectionInfo.setClientId(clientId);
        localBroker.oneway(connectionInfo);
        remoteBroker.oneway(connectionInfo);

        sessionInfo = new SessionInfo(connectionInfo, 1);
        localBroker.oneway(sessionInfo);
        remoteBroker.oneway(sessionInfo);

        queueConsumerInfo = new ConsumerInfo(sessionInfo, 1);
        queueConsumerInfo.setDispatchAsync(dispatchAsync);
        queueConsumerInfo.setDestination(new ActiveMQQueue(destinationFilter));
        queueConsumerInfo.setPrefetchSize(prefetchSize);
        queueConsumerInfo.setPriority(ConsumerInfo.NETWORK_CONSUMER_PRIORITY);
        localBroker.oneway(queueConsumerInfo);

        producerInfo = new ProducerInfo(sessionInfo, 1);
        producerInfo.setResponseRequired(false);
        remoteBroker.oneway(producerInfo);

        if (connectionInfo.getClientId() != null) {
            topicConsumerInfo = new ConsumerInfo(sessionInfo, 2);
            topicConsumerInfo.setDispatchAsync(dispatchAsync);
            topicConsumerInfo.setSubscriptionName("topic-bridge");
            topicConsumerInfo.setRetroactive(true);
            topicConsumerInfo.setDestination(new ActiveMQTopic(destinationFilter));
            topicConsumerInfo.setPrefetchSize(prefetchSize);
            topicConsumerInfo.setPriority(ConsumerInfo.NETWORK_CONSUMER_PRIORITY);
            localBroker.oneway(topicConsumerInfo);
        }

        LOG.info("Network connection between {} and {} has been established.", localBroker, remoteBroker);
    }

    public void stop() throws Exception {
        try {
            if (connectionInfo != null) {
                localBroker.request(connectionInfo.createRemoveCommand());
                remoteBroker.request(connectionInfo.createRemoveCommand());
            }
            localBroker.setTransportListener(null);
            remoteBroker.setTransportListener(null);
            localBroker.oneway(new ShutdownInfo());
            remoteBroker.oneway(new ShutdownInfo());
        } finally {
            ServiceStopper ss = new ServiceStopper();
            ss.stop(localBroker);
            ss.stop(remoteBroker);
            ss.throwFirstException();
        }
    }

    public void serviceRemoteException(Throwable error) {
        LOG.info("Unexpected remote exception: {}", error.getMessage());
        LOG.debug("Exception trace: ", error);
    }

    protected void serviceRemoteCommand(Command command) {
        try {
            if (command.isBrokerInfo()) {
                synchronized (this) {
                    remoteBrokerInfo = (BrokerInfo)command;
                    remoteBrokerId = remoteBrokerInfo.getBrokerId();
                    if (localBrokerId != null) {
                        if (localBrokerId.equals(remoteBrokerId)) {
                            LOG.info("Disconnecting loop back connection.");
                            ServiceSupport.dispose(this);
                        } else {
                            triggerStartBridge();
                        }
                    }
                }
            } else {
                LOG.warn("Unexpected remote command: {}", command);
            }
        } catch (IOException e) {
            serviceLocalException(e);
        }
    }

    public void serviceLocalException(Throwable error) {
        LOG.info("Unexpected local exception: {}", error.getMessage());
        LOG.debug("Exception trace: ", error);
        fireBridgeFailed();
    }

    protected void serviceLocalCommand(Command command) {
        try {
            if (command.isMessageDispatch()) {

                enqueueCounter.incrementAndGet();

                final MessageDispatch md = (MessageDispatch)command;
                Message message = md.getMessage();
                message.setProducerId(producerInfo.getProducerId());

                if (message.getOriginalTransactionId() == null) {
                    message.setOriginalTransactionId(message.getTransactionId());
                }
                message.setTransactionId(null);

                if (isUseCompression()) {
                    message.compress();
                }

                if (!message.isResponseRequired()) {
                    // If the message was originally sent using async send, we will preserve that
                    // QOS by bridging it using an async send (small chance of message loss).
                    remoteBroker.oneway(message);
                    dequeueCounter.incrementAndGet();
                    localBroker.oneway(new MessageAck(md, MessageAck.STANDARD_ACK_TYPE, 1));

                } else {

                    // The message was not sent using async send, so we should
                    // only ack the local
                    // broker when we get confirmation that the remote broker
                    // has received the message.
                    ResponseCallback callback = new ResponseCallback() {
                        public void onCompletion(FutureResponse future) {
                            try {
                                Response response = future.getResult();
                                if (response.isException()) {
                                    ExceptionResponse er = (ExceptionResponse)response;
                                    serviceLocalException(er.getException());
                                } else {
                                    dequeueCounter.incrementAndGet();
                                    localBroker.oneway(new MessageAck(md, MessageAck.STANDARD_ACK_TYPE, 1));
                                }
                            } catch (IOException e) {
                                serviceLocalException(e);
                            }
                        }
                    };

                    remoteBroker.asyncRequest(message, callback);
                }

                // Ack on every message since we don't know if the broker is
                // blocked due to memory
                // usage and is waiting for an Ack to un-block him.

                // Acking a range is more efficient, but also more prone to
                // locking up a server
                // Perhaps doing something like the following should be policy
                // based.
                // if(
                // md.getConsumerId().equals(queueConsumerInfo.getConsumerId())
                // ) {
                // queueDispatched++;
                // if( queueDispatched > (queueConsumerInfo.getPrefetchSize()/2)
                // ) {
                // localBroker.oneway(new MessageAck(md,
                // MessageAck.STANDARD_ACK_TYPE, queueDispatched));
                // queueDispatched=0;
                // }
                // } else {
                // topicDispatched++;
                // if( topicDispatched > (topicConsumerInfo.getPrefetchSize()/2)
                // ) {
                // localBroker.oneway(new MessageAck(md,
                // MessageAck.STANDARD_ACK_TYPE, topicDispatched));
                // topicDispatched=0;
                // }
                // }
            } else if (command.isBrokerInfo()) {
                synchronized (this) {
                    localBrokerInfo = (BrokerInfo)command;
                    localBrokerId = localBrokerInfo.getBrokerId();
                    if (remoteBrokerId != null) {
                        if (remoteBrokerId.equals(localBrokerId)) {
                            LOG.info("Disconnecting loop back connection.");
                            ServiceSupport.dispose(this);
                        } else {
                            triggerStartBridge();
                        }
                    }
                }
            } else {
                LOG.debug("Unexpected local command: {}", command);
            }
        } catch (IOException e) {
            serviceLocalException(e);
        }
    }

    public String getClientId() {
        return clientId;
    }

    public void setClientId(String clientId) {
        this.clientId = clientId;
    }

    public int getPrefetchSize() {
        return prefetchSize;
    }

    public void setPrefetchSize(int prefetchSize) {
        this.prefetchSize = prefetchSize;
    }

    public boolean isDispatchAsync() {
        return dispatchAsync;
    }

    public void setDispatchAsync(boolean dispatchAsync) {
        this.dispatchAsync = dispatchAsync;
    }

    public String getDestinationFilter() {
        return destinationFilter;
    }

    public void setDestinationFilter(String destinationFilter) {
        this.destinationFilter = destinationFilter;
    }

    public void setNetworkBridgeFailedListener(NetworkBridgeListener listener) {
        this.bridgeFailedListener = listener;
    }

    private void fireBridgeFailed() {
        NetworkBridgeListener l = this.bridgeFailedListener;
        if (l != null) {
            l.bridgeFailed();
        }
    }

    public String getRemoteAddress() {
        return remoteBroker.getRemoteAddress();
    }

    public String getLocalAddress() {
        return localBroker.getRemoteAddress();
    }

    public String getLocalBrokerName() {
        return localBrokerInfo == null ? null : localBrokerInfo.getBrokerName();
    }

    public String getRemoteBrokerName() {
        return remoteBrokerInfo == null ? null : remoteBrokerInfo.getBrokerName();
    }

    public long getDequeueCounter() {
        return dequeueCounter.get();
    }

    public long getEnqueueCounter() {
        return enqueueCounter.get();
    }

    /**
     * @param useCompression
     *      True if forwarded Messages should have their bodies compressed.
     */
    public void setUseCompression(boolean useCompression) {
        this.useCompression = useCompression;
    }

    /**
     * @return the vale of the useCompression setting, true if forwarded messages will be compressed.
     */
    public boolean isUseCompression() {
        return useCompression;
    }
}
