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
package org.apache.activemq.partition;

import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketAddress;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.activemq.broker.Broker;
import org.apache.activemq.broker.BrokerFilter;
import org.apache.activemq.broker.ConnectionContext;
import org.apache.activemq.broker.ProducerBrokerExchange;
import org.apache.activemq.broker.TransportConnection;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.ConnectionControl;
import org.apache.activemq.command.ConnectionId;
import org.apache.activemq.command.ConnectionInfo;
import org.apache.activemq.command.Message;
import org.apache.activemq.partition.dto.Partitioning;
import org.apache.activemq.partition.dto.Target;
import org.apache.activemq.state.ConsumerState;
import org.apache.activemq.state.SessionState;
import org.apache.activemq.transport.Transport;
import org.apache.activemq.util.LRUCache;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A BrokerFilter which partitions client connections over a cluster of brokers.
 *
 * It can use a client identifier like client id, authenticated user name, source ip
 * address or even destination being used by the connection to figure out which
 * is the best broker in the cluster that the connection should be using and then
 * redirects failover clients to that broker.
 */
public class PartitionBroker extends BrokerFilter {

    protected static final Logger LOG = LoggerFactory.getLogger(PartitionBroker.class);
    protected final PartitionBrokerPlugin plugin;
    protected boolean reloadConfigOnPoll = true;

    public PartitionBroker(Broker broker, PartitionBrokerPlugin plugin) {
        super(broker);
        this.plugin = plugin;
    }

    @Override
    public void start() throws Exception {
        super.start();
        getExecutor().execute(new Runnable() {
            @Override
            public void run() {
                Thread.currentThread().setName("Partition Monitor");
                onMonitorStart();
                try {
                    runPartitionMonitor();
                } catch (Exception e) {
                    onMonitorStop();
                }
            }
        });
    }

    protected void onMonitorStart() {
    }
    protected void onMonitorStop() {
    }

    protected void runPartitionMonitor() {
        while( !isStopped() ) {
            try {
                monitorWait();
            } catch (InterruptedException e) {
                break;
            }

            if(reloadConfigOnPoll) {
                try {
                    reloadConfiguration();
                } catch (Exception e) {
                    continue;
                }
            }

            for( ConnectionMonitor monitor: monitors.values()) {
                checkTarget(monitor);
            }
        }
    }

    protected void monitorWait() throws InterruptedException {
        synchronized (this) {
            this.wait(1000);
        }
    }

    protected void monitorWakeup()  {
        synchronized (this) {
            this.notifyAll();
        }
    }

    protected void reloadConfiguration() throws Exception {
    }

    protected void checkTarget(ConnectionMonitor monitor) {

        // can we find a preferred target for the connection?
        Target targetDTO = pickBestBroker(monitor);
        if( targetDTO == null || targetDTO.ids==null) {
            LOG.debug("No partition target found for connection: "+monitor.context.getConnectionId());
            return;
        }

        // Are we one the the targets?
        if( targetDTO.ids.contains(getBrokerName()) ) {
            LOG.debug("We are a partition target for connection: "+monitor.context.getConnectionId());
            return;
        }

        // Then we need to move the connection over.
        String connectionString = getConnectionString(targetDTO.ids);
        if( connectionString==null ) {
            LOG.debug("Could not convert to partition targets to connection string: " + targetDTO.ids);
            return;
        }

        LOG.info("Redirecting connection to: " + connectionString);
        TransportConnection connection = (TransportConnection)monitor.context.getConnection();
        ConnectionControl cc = new ConnectionControl();
        cc.setConnectedBrokers(connectionString);
        cc.setRebalanceConnection(true);
        connection.dispatchAsync(cc);
    }

    protected String getConnectionString(HashSet<String> ids) {
        StringBuilder rc = new StringBuilder();
        for (String id : ids) {
            String url = plugin.getBrokerURL(this, id);
            if( url!=null ) {
                if( rc.length()!=0 ) {
                    rc.append(',');
                }
                rc.append(url);
            }
        }
        if( rc.length()==0 )
            return null;
        return rc.toString();
    }

    static private class Score {
        int value;
    }

    protected Target pickBestBroker(ConnectionMonitor monitor) {

        if( getConfig() ==null )
            return null;

        if( getConfig().bySourceIp !=null && !getConfig().bySourceIp.isEmpty() ) {
            TransportConnection connection = (TransportConnection)monitor.context.getConnection();
            Transport transport = connection.getTransport();
            Socket socket = transport.narrow(Socket.class);
            if( socket !=null ) {
                SocketAddress address = socket.getRemoteSocketAddress();
                if( address instanceof InetSocketAddress) {
                    String ip = ((InetSocketAddress) address).getAddress().getHostAddress();
                    Target targetDTO = getConfig().bySourceIp.get(ip);
                    if( targetDTO!=null ) {
                        return targetDTO;
                    }
                }
            }
        }

        if( getConfig().byUserName !=null && !getConfig().byUserName.isEmpty() ) {
            String userName = monitor.context.getUserName();
            if( userName !=null ) {
                Target targetDTO = getConfig().byUserName.get(userName);
                if( targetDTO!=null ) {
                    return targetDTO;
                }
            }
        }

        if( getConfig().byClientId !=null && !getConfig().byClientId.isEmpty() ) {
            String clientId = monitor.context.getClientId();
            if( clientId!=null ) {
                Target targetDTO = getConfig().byClientId.get(clientId);
                if( targetDTO!=null ) {
                    return targetDTO;
                }
            }
        }

        if(
             (getConfig().byQueue !=null && !getConfig().byQueue.isEmpty())
          || (getConfig().byTopic !=null && !getConfig().byTopic.isEmpty())
          ) {

            // Collect the destinations the connection is consuming from...
            HashSet<ActiveMQDestination> dests = new HashSet<ActiveMQDestination>();
            for (SessionState session : monitor.context.getConnectionState().getSessionStates()) {
                for (ConsumerState consumer : session.getConsumerStates()) {
                    ActiveMQDestination destination = consumer.getInfo().getDestination();
                    if( destination.isComposite() ) {
                        dests.addAll(Arrays.asList(destination.getCompositeDestinations()));
                    } else {
                        dests.addAll(Collections.singletonList(destination));
                    }
                }
            }

            // Group them by the partitioning target for the destinations and score them..
            HashMap<Target, Score> targetScores = new HashMap<Target, Score>();
            for (ActiveMQDestination dest : dests) {
                Target target = getTarget(dest);
                if( target!=null ) {
                    Score score = targetScores.get(target);
                    if( score == null ) {
                        score = new Score();
                        targetScores.put(target, score);
                    }
                    score.value++;
                }
            }

            // The target with largest score wins..
            if( !targetScores.isEmpty() ) {
                Target bestTarget = null;
                int bestScore=0;
                for (Map.Entry<Target, Score> entry : targetScores.entrySet()) {
                    if( entry.getValue().value > bestScore ) {
                        bestTarget = entry.getKey();
                    }
                }
                return bestTarget;
            }

            // If we get here is because there were no consumers, or the destinations for those
            // consumers did not have an assigned destination..  So partition based on producer
            // usage.
            Target best = monitor.findBestProducerTarget(this);
            if( best!=null ) {
                return best;
            }
        }
        return null;
    }

    protected Target getTarget(ActiveMQDestination dest) {
        Partitioning config = getConfig();
        if( dest.isQueue() && config.byQueue !=null && !config.byQueue.isEmpty() ) {
            return config.byQueue.get(dest.getPhysicalName());
        } else if( dest.isTopic() && config.byTopic !=null && !config.byTopic.isEmpty() ) {
            return config.byTopic.get(dest.getPhysicalName());
        }
        return null;
    }

    protected final ConcurrentMap<ConnectionId, ConnectionMonitor> monitors = new ConcurrentHashMap<ConnectionId, ConnectionMonitor>();

    @Override
    public void addConnection(ConnectionContext context, ConnectionInfo info) throws Exception {
        if( info.isFaultTolerant() ) {
            ConnectionMonitor monitor = new ConnectionMonitor(context);
            monitors.put(info.getConnectionId(), monitor);
            super.addConnection(context, info);
            checkTarget(monitor);
        } else {
            super.addConnection(context, info);
        }
    }

    @Override
    public void removeConnection(ConnectionContext context, ConnectionInfo info, Throwable error) throws Exception {
        super.removeConnection(context, info, error);
        if( info.isFaultTolerant() ) {
            monitors.remove(info.getConnectionId());
        }
    }

    @Override
    public void send(ProducerBrokerExchange producerExchange, Message messageSend) throws Exception {
        ConnectionMonitor monitor = monitors.get(producerExchange.getConnectionContext().getConnectionId());
        if( monitor!=null ) {
            monitor.onSend(producerExchange, messageSend);
        }
    }

    protected Partitioning getConfig() {
        return plugin.getConfig();
    }


    static class Traffic {
        long messages;
        long bytes;
    }

    static class ConnectionMonitor {

        final ConnectionContext context;
        LRUCache<ActiveMQDestination, Traffic> trafficPerDestination =  new LRUCache<ActiveMQDestination, Traffic>();

        public ConnectionMonitor(ConnectionContext context) {
            this.context = context;
        }

        synchronized public Target findBestProducerTarget(PartitionBroker broker) {
            Target best = null;
            long bestSize = 0 ;
            for (Map.Entry<ActiveMQDestination, Traffic> entry : trafficPerDestination.entrySet()) {
                Traffic t = entry.getValue();
                // Once we get enough messages...
                if( t.messages < broker.plugin.getMinTransferCount()) {
                    continue;
                }
                if( t.bytes > bestSize) {
                    bestSize = t.bytes;
                    Target target = broker.getTarget(entry.getKey());
                    if( target!=null ) {
                        best = target;
                    }
                }
            }
            return best;
        }

        synchronized public void onSend(ProducerBrokerExchange producerExchange, Message message) {
            ActiveMQDestination dest = message.getDestination();
            Traffic traffic = trafficPerDestination.get(dest);
            if( traffic == null ) {
                traffic = new Traffic();
                trafficPerDestination.put(dest, traffic);
            }
            traffic.messages += 1;
            traffic.bytes += message.getSize();
        }


    }

}
