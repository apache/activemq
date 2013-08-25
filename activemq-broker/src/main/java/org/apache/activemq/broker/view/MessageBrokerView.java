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
package org.apache.activemq.broker.view;

import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.region.Destination;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.command.ActiveMQTempQueue;
import org.apache.activemq.command.ActiveMQTempTopic;
import org.apache.activemq.command.ActiveMQTopic;
import org.apache.activemq.util.LRUCache;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A view into the running Broker
 */
public class MessageBrokerView  {
    private static final Logger LOG = LoggerFactory.getLogger(MessageBrokerView.class);
    private final BrokerService brokerService;
    private Map<ActiveMQDestination,BrokerDestinationView> destinationViewMap = new LRUCache<ActiveMQDestination, BrokerDestinationView>();

    MessageBrokerView(BrokerService brokerService){
        this.brokerService = brokerService;
    }

    public String getBrokerName(){
        return brokerService.getBrokerName();
    }


    public int getMemoryPercentUsage() {
        return brokerService.getSystemUsage().getMemoryUsage().getPercentUsage();
    }



    public int getStorePercentUsage() {
        return brokerService.getSystemUsage().getStoreUsage().getPercentUsage();
    }

    public int getTempPercentUsage() {
        return brokerService.getSystemUsage().getTempUsage().getPercentUsage();
    }


    public int getJobSchedulerStorePercentUsage() {
        return brokerService.getSystemUsage().getJobSchedulerUsage().getPercentUsage();
    }

    public boolean isPersistent() {
        return brokerService.isPersistent();
    }

    public BrokerService getBrokerService(){
        return brokerService;
    }

    public Set<ActiveMQDestination> getDestinations(){
        Set<ActiveMQDestination> result;

        try {
            ActiveMQDestination[] destinations =  brokerService.getBroker().getDestinations();
            result = new HashSet<ActiveMQDestination>();
            Collections.addAll(result, destinations);
        }catch (Exception e){
           result = Collections.emptySet();
        }
        return result;
    }

    public Set<ActiveMQTopic> getTopics(){
        Set<ActiveMQTopic> result = new HashSet<ActiveMQTopic>();
        for (ActiveMQDestination destination:getDestinations()){
            if (destination.isTopic() && !destination.isTemporary()){
                result.add((ActiveMQTopic) destination);
            }
        }
        return result;
    }

    public Set<ActiveMQQueue> getQueues(){
        Set<ActiveMQQueue> result = new HashSet<ActiveMQQueue>();
        for (ActiveMQDestination destination:getDestinations()){
            if (destination.isQueue() && !destination.isTemporary()){
                result.add((ActiveMQQueue) destination);
            }
        }
        return result;
    }

    public Set<ActiveMQTempTopic> getTempTopics(){
        Set<ActiveMQTempTopic> result = new HashSet<ActiveMQTempTopic>();
        for (ActiveMQDestination destination:getDestinations()){
            if (destination.isTopic() && destination.isTemporary()){
                result.add((ActiveMQTempTopic) destination);
            }
        }
        return result;
    }

    public Set<ActiveMQTempQueue> getTempQueues(){
        Set<ActiveMQTempQueue> result = new HashSet<ActiveMQTempQueue>();
        for (ActiveMQDestination destination:getDestinations()){
            if (destination.isTopic() && destination.isTemporary()){
                result.add((ActiveMQTempQueue) destination);
            }
        }
        return result;
    }


    /**
     * It will be assumed the destinationName is prepended with topic:// or queue:// - but
     * will default to a Queue
     * @param destinationName
     * @return the BrokerDestinationView associated with the destinationName
     */

    public BrokerDestinationView getDestinationView(String destinationName){
        return getDestinationView(destinationName,ActiveMQDestination.QUEUE_TYPE);
    }

    /**
     * Get the BrokerDestinationView associated with the topic
     * @param destinationName
     * @return  BrokerDestinationView
     */

    public BrokerDestinationView getTopicDestinationView(String destinationName){
        return getDestinationView(destinationName,ActiveMQDestination.TOPIC_TYPE);
    }

    /**
     * Get the BrokerDestinationView associated with the queue
     * @param destinationName
     * @return  BrokerDestinationView
     */

    public BrokerDestinationView getQueueDestinationView(String destinationName){
        return getDestinationView(destinationName,ActiveMQDestination.QUEUE_TYPE);
    }

    public BrokerDestinationView getDestinationView (String destinationName, byte type)  {
        ActiveMQDestination activeMQDestination = ActiveMQDestination.createDestination(destinationName,type);
        return getDestinationView(activeMQDestination);
    }

    public BrokerDestinationView getDestinationView (ActiveMQDestination activeMQDestination)  {
        BrokerDestinationView view = null;
        synchronized(destinationViewMap){
            view = destinationViewMap.get(activeMQDestination);
            if (view==null){
                try {
                    /**
                     * If auto destinatons are allowed (on by default) - this will create a Broker Destination
                     * if it doesn't exist. We could query the regionBroker first to check - but this affords more
                     * flexibility - e.g. you might want to set up a query on destination statistics before any
                     * messaging clients have started (and hence created the destination themselves
                     */
                    Destination destination = brokerService.getDestination(activeMQDestination);
                    BrokerDestinationView brokerDestinationView = new BrokerDestinationView(destination);
                    destinationViewMap.put(activeMQDestination,brokerDestinationView);
                } catch (Exception e) {
                   LOG.warn("Failed to get Destination for " + activeMQDestination,e);
                }
                destinationViewMap.put(activeMQDestination,view);
            }
        }
        return view;
    }




}
