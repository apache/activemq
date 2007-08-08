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
package org.apache.activemq.broker;

import java.util.ArrayList;
import java.util.List;
import org.apache.activemq.broker.region.Destination;
import org.apache.activemq.broker.region.Subscription;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.BrokerInfo;
import org.apache.activemq.command.ConnectionInfo;
import org.apache.activemq.command.ConsumerInfo;
import org.apache.activemq.command.Message;
import org.apache.activemq.command.MessageAck;
import org.apache.activemq.command.ProducerInfo;
import org.apache.activemq.command.RemoveSubscriptionInfo;
import org.apache.activemq.command.SessionInfo;
import org.apache.activemq.command.TransactionId;

/**
 * Used to add listeners for Broker actions
 * 
 * @version $Revision: 1.10 $
 */
public class BrokerBroadcaster extends BrokerFilter {
    protected volatile Broker[] listeners = new Broker[0];

    public BrokerBroadcaster(Broker next) {
        super(next);
    }

    public void acknowledge(ConsumerBrokerExchange consumerExchange, MessageAck ack) throws Exception {
        next.acknowledge(consumerExchange, ack);
        Broker brokers[] = getListeners();
        for (int i = 0; i < brokers.length; i++) {
            brokers[i].acknowledge(consumerExchange, ack);
        }
    }

    public void addConnection(ConnectionContext context, ConnectionInfo info) throws Exception {
        next.addConnection(context, info);
        Broker brokers[] = getListeners();
        for (int i = 0; i < brokers.length; i++) {
            brokers[i].addConnection(context, info);
        }
    }

    public Subscription addConsumer(ConnectionContext context, ConsumerInfo info) throws Exception {
        Subscription answer = next.addConsumer(context, info);
        Broker brokers[] = getListeners();
        for (int i = 0; i < brokers.length; i++) {
            brokers[i].addConsumer(context, info);
        }
        return answer;
    }

    public void addProducer(ConnectionContext context, ProducerInfo info) throws Exception {
        next.addProducer(context, info);
        Broker brokers[] = getListeners();
        for (int i = 0; i < brokers.length; i++) {
            brokers[i].addProducer(context, info);
        }
    }

    public void commitTransaction(ConnectionContext context, TransactionId xid, boolean onePhase) throws Exception {
        next.commitTransaction(context, xid, onePhase);
        Broker brokers[] = getListeners();
        for (int i = 0; i < brokers.length; i++) {
            brokers[i].commitTransaction(context, xid, onePhase);
        }
    }

    public void removeSubscription(ConnectionContext context, RemoveSubscriptionInfo info) throws Exception {
        next.removeSubscription(context, info);
        Broker brokers[] = getListeners();
        for (int i = 0; i < brokers.length; i++) {
            brokers[i].removeSubscription(context, info);
        }
    }

    public int prepareTransaction(ConnectionContext context, TransactionId xid) throws Exception {
        int result = next.prepareTransaction(context, xid);
        Broker brokers[] = getListeners();
        for (int i = 0; i < brokers.length; i++) {
            // TODO decide what to do with return values
            brokers[i].prepareTransaction(context, xid);
        }
        return result;
    }

    public void removeConnection(ConnectionContext context, ConnectionInfo info, Throwable error) throws Exception {
        next.removeConnection(context, info, error);
        Broker brokers[] = getListeners();
        for (int i = 0; i < brokers.length; i++) {
            brokers[i].removeConnection(context, info, error);
        }
    }

    public void removeConsumer(ConnectionContext context, ConsumerInfo info) throws Exception {
        next.removeConsumer(context, info);
        Broker brokers[] = getListeners();
        for (int i = 0; i < brokers.length; i++) {
            brokers[i].removeConsumer(context, info);
        }
    }

    public void removeProducer(ConnectionContext context, ProducerInfo info) throws Exception {
        next.removeProducer(context, info);
        Broker brokers[] = getListeners();
        for (int i = 0; i < brokers.length; i++) {
            brokers[i].removeProducer(context, info);
        }
    }

    public void rollbackTransaction(ConnectionContext context, TransactionId xid) throws Exception {
        next.rollbackTransaction(context, xid);
        Broker brokers[] = getListeners();
        for (int i = 0; i < brokers.length; i++) {
            brokers[i].rollbackTransaction(context, xid);
        }
    }

    public void send(ProducerBrokerExchange producerExchange, Message messageSend) throws Exception {
        next.send(producerExchange, messageSend);
        Broker brokers[] = getListeners();
        for (int i = 0; i < brokers.length; i++) {
            brokers[i].send(producerExchange, messageSend);
        }
    }

    public void beginTransaction(ConnectionContext context, TransactionId xid) throws Exception {
        next.beginTransaction(context, xid);
        Broker brokers[] = getListeners();
        for (int i = 0; i < brokers.length; i++) {
            brokers[i].beginTransaction(context, xid);
        }
    }

    public void forgetTransaction(ConnectionContext context, TransactionId transactionId) throws Exception {
        next.forgetTransaction(context, transactionId);
        Broker brokers[] = getListeners();
        for (int i = 0; i < brokers.length; i++) {
            brokers[i].forgetTransaction(context, transactionId);
        }
    }

    public Destination addDestination(ConnectionContext context, ActiveMQDestination destination) throws Exception {
        Destination result = next.addDestination(context, destination);
        Broker brokers[] = getListeners();
        for (int i = 0; i < brokers.length; i++) {
            brokers[i].addDestination(context, destination);
        }
        return result;
    }

    public void removeDestination(ConnectionContext context, ActiveMQDestination destination, long timeout) throws Exception {
        next.removeDestination(context, destination, timeout);
        Broker brokers[] = getListeners();
        for (int i = 0; i < brokers.length; i++) {
            brokers[i].removeDestination(context, destination, timeout);
        }
    }

    public void start() throws Exception {
        next.start();
        Broker brokers[] = getListeners();
        for (int i = 0; i < brokers.length; i++) {
            brokers[i].start();
        }
    }

    public void stop() throws Exception {
        next.stop();
        Broker brokers[] = getListeners();
        for (int i = 0; i < brokers.length; i++) {
            brokers[i].stop();
        }
    }

    public void addSession(ConnectionContext context, SessionInfo info) throws Exception {
        next.addSession(context, info);
        Broker brokers[] = getListeners();
        for (int i = 0; i < brokers.length; i++) {
            brokers[i].addSession(context, info);
        }
    }

    public void removeSession(ConnectionContext context, SessionInfo info) throws Exception {
        next.removeSession(context, info);
        Broker brokers[] = getListeners();
        for (int i = 0; i < brokers.length; i++) {
            brokers[i].removeSession(context, info);
        }
    }

    public void gc() {
        next.gc();
        Broker brokers[] = getListeners();
        for (int i = 0; i < brokers.length; i++) {
            brokers[i].gc();
        }
    }

    public void addBroker(Connection connection, BrokerInfo info) {
        next.addBroker(connection, info);
        Broker brokers[] = getListeners();
        for (int i = 0; i < brokers.length; i++) {
            brokers[i].addBroker(connection, info);
        }
    }

    protected Broker[] getListeners() {
        return listeners;
    }

    public synchronized void addListener(Broker broker) {
        List tmp = getListenersAsList();
        tmp.add(broker);
        listeners = (Broker[])tmp.toArray(new Broker[tmp.size()]);
    }

    public synchronized void removeListener(Broker broker) {
        List tmp = getListenersAsList();
        tmp.remove(broker);
        listeners = (Broker[])tmp.toArray(new Broker[tmp.size()]);
    }

    protected List getListenersAsList() {
        List tmp = new ArrayList();
        Broker brokers[] = getListeners();
        for (int i = 0; i < brokers.length; i++) {
            tmp.add(brokers[i]);
        }
        return tmp;
    }
}
