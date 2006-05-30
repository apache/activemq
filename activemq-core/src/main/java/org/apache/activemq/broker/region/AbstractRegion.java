/**
 *
 * Copyright 2005-2006 The Apache Software Foundation
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
package org.apache.activemq.broker.region;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import javax.jms.JMSException;

import org.apache.activemq.broker.ConnectionContext;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.ConsumerInfo;
import org.apache.activemq.command.Message;
import org.apache.activemq.command.MessageAck;
import org.apache.activemq.command.MessageDispatchNotification;
import org.apache.activemq.command.RemoveSubscriptionInfo;
import org.apache.activemq.filter.DestinationMap;
import org.apache.activemq.memory.UsageManager;
import org.apache.activemq.store.PersistenceAdapter;
import org.apache.activemq.thread.TaskRunnerFactory;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import edu.emory.mathcs.backport.java.util.concurrent.ConcurrentHashMap;

/**
 * 
 * @version $Revision: 1.14 $
 */
abstract public class AbstractRegion implements Region {
    
    private static final Log log = LogFactory.getLog(AbstractRegion.class);

    protected final ConcurrentHashMap destinations = new ConcurrentHashMap();
    protected final DestinationMap destinationMap = new DestinationMap();
    protected final ConcurrentHashMap subscriptions = new ConcurrentHashMap();
    protected final UsageManager memoryManager;
    protected final PersistenceAdapter persistenceAdapter;
    protected final DestinationStatistics destinationStatistics;
    protected final RegionBroker broker;
    protected boolean autoCreateDestinations=true;
    protected final TaskRunnerFactory taskRunnerFactory;
    protected final Object destinationsMutex = new Object();
    
    public AbstractRegion(RegionBroker broker,DestinationStatistics destinationStatistics, UsageManager memoryManager, TaskRunnerFactory taskRunnerFactory, PersistenceAdapter persistenceAdapter) {
        this.broker = broker;
        this.destinationStatistics = destinationStatistics;
        this.memoryManager = memoryManager;
        this.taskRunnerFactory = taskRunnerFactory;
        this.persistenceAdapter = persistenceAdapter;
    }

    public void start() throws Exception {
    }
    
    public void stop() throws Exception {
    }
    
    public Destination addDestination(ConnectionContext context, ActiveMQDestination destination) throws Exception {
        log.debug("Adding destination: "+destination);
        Destination dest = createDestination(context, destination);
        dest.start();
        synchronized(destinationsMutex){
            destinations.put(destination,dest);
            destinationMap.put(destination,dest);
            
            // Add all consumers that are interested in the destination. 
            for (Iterator iter = subscriptions.values().iterator(); iter.hasNext();) {
                Subscription sub = (Subscription) iter.next();
                if( sub.matches(destination) ) {
                    dest.addSubscription(context, sub);
                }
            }
            return dest;
        }
    }

    public void removeDestination(ConnectionContext context,ActiveMQDestination destination,long timeout)
                    throws Exception{
        
        // No timeout.. then try to shut down right way, fails if there are current subscribers.
        if( timeout == 0 ) {
            for(Iterator iter=subscriptions.values().iterator();iter.hasNext();){
                Subscription sub=(Subscription) iter.next();
                if(sub.matches(destination)){
                    throw new JMSException("Destination still has an active subscription: "+destination);
                }
            }
        }
        
        if( timeout > 0 ) {
            // TODO: implement a way to notify the subscribers that we want to take the down 
            // the destination and that they should un-subscribe..  Then wait up to timeout time before
            // dropping the subscription.
        
        }

        log.debug("Removing destination: "+destination);
        synchronized(destinationsMutex){
            Destination dest=(Destination) destinations.remove(destination);
            if(dest!=null){
                
                // timeout<0 or we timed out, we now force any remaining subscriptions to un-subscribe.
                for(Iterator iter=subscriptions.values().iterator();iter.hasNext();){
                    Subscription sub=(Subscription) iter.next();
                    if(sub.matches(destination)){
                        dest.removeSubscription(context, sub);
                    }
                }
                
                destinationMap.removeAll(destination);
                dest.dispose(context);
                dest.stop();
                
            }else{
                log.debug("Destination doesn't exist: " + dest);
            }
        }
    }
    
    public Map getDestinationMap() {
        synchronized(destinationsMutex){
            return new HashMap(destinations);
        }
    }

    public Subscription addConsumer(ConnectionContext context, ConsumerInfo info) throws Exception {

        Subscription sub = createSubscription(context, info);

        // We may need to add some destinations that are in persistent store but not active 
        // in the broker.
        //
        // TODO: think about this a little more.  This is good cause destinations are not loaded into 
        // memory until a client needs to use the queue, but a management agent viewing the 
        // broker will not see a destination that exists in persistent store.  We may want to
        // eagerly load all destinations into the broker but have an inactive state for the
        // destination which has reduced memory usage.
        //
        if( persistenceAdapter!=null ) {
            Set inactiveDests = getInactiveDestinations();
            for (Iterator iter = inactiveDests.iterator(); iter.hasNext();) {
                ActiveMQDestination dest = (ActiveMQDestination) iter.next();
                if( sub.matches(dest) ) {
                    context.getBroker().addDestination(context, dest);
                }
            }
        }
        
        subscriptions.put(info.getConsumerId(), sub);

        // Add the subscription to all the matching queues.
        for (Iterator iter = destinationMap.get(info.getDestination()).iterator(); iter.hasNext();) {
            Destination dest = (Destination) iter.next();            
            dest.addSubscription(context, sub);
        }        

        if( info.isBrowser() ) {
            ((QueueBrowserSubscription)sub).browseDone();
        }
        
        return sub;
    }
    
    /**
     * Get all the Destinations that are in storage
     * @return Set of all stored destinations
     */
    public Set getDurableDestinations(){
        return persistenceAdapter.getDestinations();
    }

    /**
     * @return all Destinations that don't have active consumers
     */
    protected Set getInactiveDestinations() {
        Set inactiveDests = persistenceAdapter.getDestinations();
        inactiveDests.removeAll( destinations.keySet() );
        return inactiveDests;
    }
    
    public void removeConsumer(ConnectionContext context, ConsumerInfo info) throws Exception {
        
        Subscription sub = (Subscription) subscriptions.remove(info.getConsumerId());
        if( sub==null )
            throw new IllegalArgumentException("The subscription does not exist: "+info.getConsumerId());
        
        // remove the subscription from all the matching queues.
        for (Iterator iter = destinationMap.get(info.getDestination()).iterator(); iter.hasNext();) {
            Destination dest = (Destination) iter.next();
            dest.removeSubscription(context, sub);
        }
        
        destroySubscription(sub);
        
    }

    protected void destroySubscription(Subscription sub) {        
        sub.destroy();
    }

    public void removeSubscription(ConnectionContext context, RemoveSubscriptionInfo info) throws Exception {
        throw new JMSException("Invalid operation.");
    }

    public void send(ConnectionContext context, Message messageSend)
            throws Exception {
        Destination dest = lookup(context, messageSend.getDestination());
        dest.send(context, messageSend);
    }
    
    public void acknowledge(ConnectionContext context, MessageAck ack) throws Exception {
        
        Subscription sub = (Subscription) subscriptions.get(ack.getConsumerId());
        if( sub==null )
            throw new IllegalArgumentException("The subscription does not exist: "+ack.getConsumerId());
        sub.acknowledge(context, ack);
        
    }

    protected Destination lookup(ConnectionContext context, ActiveMQDestination destination) throws Exception {
        synchronized(destinationsMutex){
            Destination dest=(Destination) destinations.get(destination);
            if(dest==null){
                if(autoCreateDestinations){
                    // Try to auto create the destination... re-invoke broker from the
                    // top so that the proper security checks are performed.
                    context.getBroker().addDestination(context,destination);
                    // We should now have the dest created.
                    dest=(Destination) destinations.get(destination);
                }
                if(dest==null){
                    throw new JMSException("The destination "+destination+" does not exist.");
                }
            }
            return dest;
        }
    }
    
    public void processDispatchNotification(MessageDispatchNotification messageDispatchNotification) throws Exception{
        Subscription sub = (Subscription) subscriptions.get(messageDispatchNotification.getConsumerId());
        if (sub != null){
            sub.processMessageDispatchNotification(messageDispatchNotification);
        }
    }
    public void gc() {
        for (Iterator iter = subscriptions.values().iterator(); iter.hasNext();) {
            Subscription sub = (Subscription) iter.next();
            sub.gc();
        }        
        for (Iterator iter = destinations.values()  .iterator(); iter.hasNext();) {
            Destination dest = (Destination) iter.next();
            dest.gc();
        }        
    }

    protected abstract Subscription createSubscription(ConnectionContext context, ConsumerInfo info) throws Exception;
    abstract protected Destination createDestination(ConnectionContext context, ActiveMQDestination destination) throws Exception;

    public boolean isAutoCreateDestinations() {
        return autoCreateDestinations;
    }

    public void setAutoCreateDestinations(boolean autoCreateDestinations) {
        this.autoCreateDestinations = autoCreateDestinations;
    }
    

}
