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

import org.apache.activemq.broker.BrokerRegistry;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.region.Destination;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.command.ActiveMQTempQueue;
import org.apache.activemq.command.ActiveMQTempTopic;
import org.apache.activemq.command.ActiveMQTopic;
import org.apache.activemq.util.LRUCache;

/**
 * A view into the running Broker
 */
public class MessageBrokerView  {
    private final BrokerService brokerService;
    private final Map<ActiveMQDestination,BrokerDestinationView> destinationViewMap = new LRUCache<ActiveMQDestination, BrokerDestinationView>();


    /**
     * Create a view of a running Broker
     * @param brokerService
     */
    public MessageBrokerView(BrokerService brokerService){
        this.brokerService = brokerService;
        if (brokerService == null){
            throw new NullPointerException("BrokerService is null");
        }
        if (!brokerService.isStarted()){
            throw new IllegalStateException("BrokerService " + brokerService.getBrokerName() + " is not started");
        }
    }

    /**
     * Create a view of a running Broker
     * @param brokerName
     */
    public MessageBrokerView(String brokerName){
        this.brokerService = BrokerRegistry.getInstance().lookup(brokerName);
        if (brokerService == null){
            throw new NullPointerException("BrokerService is null");
        }
        if (!brokerService.isStarted()){
            throw new IllegalStateException("BrokerService " + brokerService.getBrokerName() + " is not started");
        }
    }



    /**
     * @return the brokerName
     */
    public String getBrokerName(){
        return brokerService.getBrokerName();
    }

    /**
     * @return the unique id of the Broker
     */
    public String getBrokerId(){
        try {
            return brokerService.getBroker().getBrokerId().toString();
        } catch (Exception e) {
            return "";
        }
    }


    /**
     * @return the memory used by the Broker as a percentage
     */
    public int getMemoryPercentUsage() {
        return brokerService.getSystemUsage().getMemoryUsage().getPercentUsage();
    }


    /**
     * @return  the space used by the Message Store as a percentage
     */

    public int getStorePercentUsage() {
        return brokerService.getSystemUsage().getStoreUsage().getPercentUsage();
    }

    /**
     * @return the space used by the store for temporary messages as a percentage
     */
    public int getTempPercentUsage() {
        return brokerService.getSystemUsage().getTempUsage().getPercentUsage();
    }

    /**
     * @return the space used by the store of scheduled messages
     */
    public int getJobSchedulerStorePercentUsage() {
        return brokerService.getSystemUsage().getJobSchedulerUsage().getPercentUsage();
    }

    /**
     * @return true if the Broker isn't using an in-memory store only for messages
     */
    public boolean isPersistent() {
        return brokerService.isPersistent();
    }

    public BrokerService getBrokerService(){
        return brokerService;
    }

    /**
     * Retrieve a set of all Destinations be used by the Broker
     * @return  all Destinations
     */
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

    /**
     * Retrieve a set of all Topics be used by the Broker
     * @return  all Topics
     */

    public Set<ActiveMQTopic> getTopics(){
        Set<ActiveMQTopic> result = new HashSet<ActiveMQTopic>();
        for (ActiveMQDestination destination:getDestinations()){
            if (destination.isTopic() && !destination.isTemporary()){
                result.add((ActiveMQTopic) destination);
            }
        }
        return result;
    }

    /**
     * Retrieve a set of all Queues be used by the Broker
     * @return  all Queues
     */

    public Set<ActiveMQQueue> getQueues(){
        Set<ActiveMQQueue> result = new HashSet<ActiveMQQueue>();
        for (ActiveMQDestination destination:getDestinations()){
            if (destination.isQueue() && !destination.isTemporary()){
                result.add((ActiveMQQueue) destination);
            }
        }
        return result;
    }

    /**
     * Retrieve a set of all TemporaryTopics be used by the Broker
     * @return  all TemporaryTopics
     */
    public Set<ActiveMQTempTopic> getTempTopics(){
        Set<ActiveMQTempTopic> result = new HashSet<ActiveMQTempTopic>();
        for (ActiveMQDestination destination:getDestinations()){
            if (destination.isTopic() && destination.isTemporary()){
                result.add((ActiveMQTempTopic) destination);
            }
        }
        return result;
    }


    /**
     * Retrieve a set of all TemporaryQueues be used by the Broker
     * @return  all TemporaryQueues
     */
    public Set<ActiveMQTempQueue> getTempQueues(){
        Set<ActiveMQTempQueue> result = new HashSet<ActiveMQTempQueue>();
        for (ActiveMQDestination destination:getDestinations()){
            if (destination.isQueue() && destination.isTemporary()){
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
     * @throws Exception
     */

    public BrokerDestinationView getDestinationView(String destinationName)   throws Exception{
        return getDestinationView(destinationName,ActiveMQDestination.QUEUE_TYPE);
    }

    /**
     * Get the BrokerDestinationView associated with the topic
     * @param destinationName
     * @return  BrokerDestinationView
     * @throws Exception
     */

    public BrokerDestinationView getTopicDestinationView(String destinationName)   throws Exception{
        return getDestinationView(destinationName,ActiveMQDestination.TOPIC_TYPE);
    }

    /**
     * Get the BrokerDestinationView associated with the queue
     * @param destinationName
     * @return  BrokerDestinationView
     * @throws Exception
     */

    public BrokerDestinationView getQueueDestinationView(String destinationName) throws Exception{
        return getDestinationView(destinationName,ActiveMQDestination.QUEUE_TYPE);
    }


    /**
     * Get the BrokerDestinationView associated with destination
     * @param destinationName
     * @param type  expects either ActiveMQDestination.QUEUE_TYPE, ActiveMQDestination.TOPIC_TYPE etc
     * @return  BrokerDestinationView
     * @throws Exception
     */
    public BrokerDestinationView getDestinationView (String destinationName, byte type)  throws Exception {
        ActiveMQDestination activeMQDestination = ActiveMQDestination.createDestination(destinationName,type);
        return getDestinationView(activeMQDestination);
    }

    /**
     *  Get the BrokerDestinationView associated with destination
     * @param activeMQDestination
     * @return   BrokerDestinationView
     * @throws Exception
     */
    public BrokerDestinationView getDestinationView (ActiveMQDestination activeMQDestination) throws Exception {
        BrokerDestinationView view = null;
        synchronized(destinationViewMap){
            view = destinationViewMap.get(activeMQDestination);
            if (view==null){

                    /**
                     * If auto destinations are allowed (on by default) - this will create a Broker Destination
                     * if it doesn't exist. We could query the regionBroker first to check - but this affords more
                     * flexibility - e.g. you might want to set up a query on destination statistics before any
                     * messaging clients have started (and hence created the destination themselves
                     */
                    Destination destination = brokerService.getDestination(activeMQDestination);
                    view = new BrokerDestinationView(destination);
                    destinationViewMap.put(activeMQDestination,view);

            }
        }
        return view;
    }




}
