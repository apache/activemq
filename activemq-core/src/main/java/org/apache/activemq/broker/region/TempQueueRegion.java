/**
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
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

import javax.jms.InvalidSelectorException;
import javax.jms.JMSException;

import org.apache.activemq.broker.Broker;
import org.apache.activemq.broker.ConnectionContext;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.ActiveMQTempDestination;
import org.apache.activemq.command.ConsumerInfo;
import org.apache.activemq.memory.UsageManager;
import org.apache.activemq.thread.TaskRunnerFactory;

/**
 * 
 * @version $Revision: 1.7 $
 */
public class TempQueueRegion extends AbstractRegion {

    public TempQueueRegion(RegionBroker broker,DestinationStatistics destinationStatistics, UsageManager memoryManager, TaskRunnerFactory taskRunnerFactory, DestinationFactory destinationFactory) {
        super(broker,destinationStatistics, memoryManager, taskRunnerFactory, destinationFactory);
        // We should allow the following to be configurable via a Destination Policy 
        // setAutoCreateDestinations(false);
    }

    protected Destination createDestination(ConnectionContext context, ActiveMQDestination destination) throws Exception {
        final ActiveMQTempDestination tempDest = (ActiveMQTempDestination) destination;
        return new Queue(destination, memoryManager, null, destinationStatistics, taskRunnerFactory) {
            
            public void addSubscription(ConnectionContext context,Subscription sub) throws Exception {

                // Only consumers on the same connection can consume from 
                // the temporary destination
                if( !context.isNetworkConnection() && !tempDest.getConnectionId().equals( sub.getConsumerInfo().getConsumerId().getConnectionId() ) ) {
                    throw new JMSException("Cannot subscribe to remote temporary destination: "+tempDest);
                }
                super.addSubscription(context, sub);
            };
            
        };
    }

    protected Subscription createSubscription(ConnectionContext context, ConsumerInfo info) throws InvalidSelectorException {
        if( info.isBrowser() ) {
            return new QueueBrowserSubscription(broker,context, info);
        } else {
            return new QueueSubscription(broker,context, info);
        }
    }
    
    public String toString() {
        return "TempQueueRegion: destinations="+destinations.size()+", subscriptions="+subscriptions.size()+", memory="+memoryManager.getPercentUsage()+"%";
    }
    
    public void removeDestination(ConnectionContext context, ActiveMQDestination destination, long timeout) throws Exception {
    	
    	// Force a timeout value so that we don't get an error that 
    	// there is still an active sub.  Temp destination may be removed   
    	// while a network sub is still active which is valid.
    	if( timeout == 0 ) 
    		timeout = 1;
    	
    	super.removeDestination(context, destination, timeout);
    }
        
}
