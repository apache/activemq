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
package org.apache.activemq.broker.region;

import javax.jms.JMSException;

import org.apache.activemq.broker.ConnectionContext;
import org.apache.activemq.broker.region.policy.PolicyEntry;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.ConsumerInfo;
import org.apache.activemq.thread.TaskRunnerFactory;
import org.apache.activemq.usage.SystemUsage;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * @version $Revision: 1.7 $
 */
public class TempTopicRegion extends AbstractRegion {

    private static final Log LOG = LogFactory.getLog(TempTopicRegion.class);

    public TempTopicRegion(RegionBroker broker, DestinationStatistics destinationStatistics, SystemUsage memoryManager, TaskRunnerFactory taskRunnerFactory,
                           DestinationFactory destinationFactory) {
        super(broker, destinationStatistics, memoryManager, taskRunnerFactory, destinationFactory);
        // We should allow the following to be configurable via a Destination
        // Policy
        // setAutoCreateDestinations(false);
    }

    protected Subscription createSubscription(ConnectionContext context, ConsumerInfo info) throws JMSException {
        if (info.isDurable()) {
            throw new JMSException("A durable subscription cannot be created for a temporary topic.");
        }
        try {
            TopicSubscription answer = new TopicSubscription(broker, context, info, memoryManager);
            // lets configure the subscription depending on the destination
            ActiveMQDestination destination = info.getDestination();
            if (destination != null && broker.getDestinationPolicy() != null) {
                PolicyEntry entry = broker.getDestinationPolicy().getEntryFor(destination);
                if (entry != null) {
                    entry.configure(broker, memoryManager, answer);
                }
            }
            answer.init();
            return answer;
        } catch (Exception e) {
            LOG.error("Failed to create TopicSubscription ", e);
            JMSException jmsEx = new JMSException("Couldn't create TopicSubscription");
            jmsEx.setLinkedException(e);
            throw jmsEx;
        }
    }

    public String toString() {
        return "TempTopicRegion: destinations=" + destinations.size() + ", subscriptions=" + subscriptions.size() + ", memory=" + memoryManager.getMemoryUsage().getPercentUsage() + "%";
    }

    public void removeDestination(ConnectionContext context, ActiveMQDestination destination, long timeout) throws Exception {

        // Force a timeout value so that we don't get an error that
        // there is still an active sub. Temp destination may be removed
        // while a network sub is still active which is valid.
        if (timeout == 0) {
            timeout = 1;
        }

        super.removeDestination(context, destination, timeout);
    }
}
