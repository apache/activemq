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

import org.apache.activemq.broker.Broker;
import org.apache.activemq.broker.ConnectionContext;
import org.apache.activemq.broker.region.policy.PolicyEntry;
import org.apache.activemq.broker.region.policy.PolicyMap;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.command.ConsumerInfo;
import org.apache.activemq.memory.UsageManager;
import org.apache.activemq.store.MessageStore;
import org.apache.activemq.store.PersistenceAdapter;
import org.apache.activemq.thread.TaskRunnerFactory;

import javax.jms.InvalidSelectorException;

import java.util.Iterator;
import java.util.Set;

/**
 * 
 * @version $Revision: 1.9 $
 */
public class QueueRegion extends AbstractRegion {

    private final PolicyMap policyMap;

    public QueueRegion(Broker broker,DestinationStatistics destinationStatistics, UsageManager memoryManager, TaskRunnerFactory taskRunnerFactory,
            PersistenceAdapter persistenceAdapter, PolicyMap policyMap) {
        super(broker,destinationStatistics, memoryManager, taskRunnerFactory, persistenceAdapter);
        this.policyMap = policyMap;
    }

    public String toString() {
        return "QueueRegion: destinations=" + destinations.size() + ", subscriptions=" + subscriptions.size() + ", memory=" + memoryManager.getPercentUsage()
                + "%";
    }

    // Implementation methods
    // -------------------------------------------------------------------------
    protected Destination createDestination(ConnectionContext context, ActiveMQDestination destination) throws Throwable {
        MessageStore store = persistenceAdapter.createQueueMessageStore((ActiveMQQueue) destination);
        Queue queue = new Queue(destination, memoryManager, store, destinationStatistics, taskRunnerFactory);
        configureQueue(queue, destination);
        return queue;
    }

    protected void configureQueue(Queue queue, ActiveMQDestination destination) {
        if (policyMap != null) {
            PolicyEntry entry = policyMap.getEntryFor(destination);
            if (entry != null) {
                entry.configure(queue);
            }
        }
    }

    protected Subscription createSubscription(ConnectionContext context, ConsumerInfo info) throws InvalidSelectorException {
        if (info.isBrowser()) {
            return new QueueBrowserSubscription(broker,context, info);
        }
        else {
            return new QueueSubscription(broker,context, info);
        }
    }

    protected Set getInactiveDestinations() {
        Set inactiveDestinations = super.getInactiveDestinations();
        for (Iterator iter = inactiveDestinations.iterator(); iter.hasNext();) {
            ActiveMQDestination dest = (ActiveMQDestination) iter.next();
            if (!dest.isQueue())
                iter.remove();
        }
        return inactiveDestinations;
    }
}
