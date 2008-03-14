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
package org.apache.activemq.broker.cluster;

import java.util.ArrayList;
import java.util.List;

import org.apache.activemq.broker.Broker;
import org.apache.activemq.broker.BrokerFilter;
import org.apache.activemq.broker.ConnectionContext;
import org.apache.activemq.broker.region.Subscription;
import org.apache.activemq.command.ConsumerId;
import org.apache.activemq.command.ConsumerInfo;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Monitors for client connections that may fail to another broker - but this
 * broker isn't aware they've gone. Can occur with network glitches or client
 * error
 * 
 * @version $Revision$
 */
public class ConnectionSplitBroker extends BrokerFilter{
    private static final Log LOG = LogFactory.getLog(ConnectionSplitBroker.class);
    private List<ConsumerInfo>networkConsumerList = new ArrayList<ConsumerInfo>();
    public ConnectionSplitBroker(Broker next) {
        super(next);
    }

        
    public Subscription addConsumer(ConnectionContext context, ConsumerInfo info) throws Exception{
       
        synchronized (networkConsumerList) {
            if (info.isNetworkSubscription()) {
                networkConsumerList.add(info);
            } else {
                if(!networkConsumerList.isEmpty()) {
                    List<ConsumerInfo> gcList = new ArrayList<ConsumerInfo>();
                    for (ConsumerInfo nc : networkConsumerList) {
                        if (!nc.isNetworkConsumersEmpty()) {
                            for (ConsumerId id : nc.getNetworkConsumerIds()) {
                                if (id.equals(info.getConsumerId())) {
                                    nc.removeNetworkConsumerId(id);
                                    if (nc.isNetworkConsumersEmpty()) {
                                        gcList.add(nc);
                                    }
                                }
                            }
                        } else {
                            gcList.add(nc);
                        }
                    }
                    for (ConsumerInfo nc : gcList) {
                        networkConsumerList.remove(nc);
                        super.removeConsumer(context, nc);
                        LOG.warn("Removed stale network consumer " + nc);
                    }
                }
            }
        }
        return super.addConsumer(context, info);
    }

    public void removeConsumer(ConnectionContext context, ConsumerInfo info) throws Exception {
        if (info.isNetworkSubscription()) {

            synchronized (networkConsumerList) {
                networkConsumerList.remove(info);
            }
        }
        super.removeConsumer(context, info);
    }
}
