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
package org.apache.activemq.web;

import org.apache.activemq.broker.jmx.BrokerViewMBean;
import org.apache.activemq.broker.jmx.QueueViewMBean;
import org.apache.activemq.broker.jmx.TopicViewMBean;
import org.apache.activemq.command.ActiveMQDestination;

import java.util.Collection;

/**
 * A facade for either a local in JVM broker or a remote broker over JMX
 *
 * @version $Revision$
 */
public interface BrokerFacade {

    BrokerViewMBean getBrokerAdmin() throws Exception;

    Collection getQueues() throws Exception;

    Collection getTopics() throws Exception;

    Collection getDurableTopicSubscribers() throws Exception;

    /**
     * Purges the given destination
     * @param destination
     * @throws Exception
     */
    void purgeQueue(ActiveMQDestination destination) throws Exception;

    QueueViewMBean getQueue(String name) throws Exception;

    TopicViewMBean getTopic(String name) throws Exception;
}
