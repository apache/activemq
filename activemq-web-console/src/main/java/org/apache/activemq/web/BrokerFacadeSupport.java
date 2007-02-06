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
package org.apache.activemq.web;

import org.apache.activemq.broker.jmx.BrokerViewMBean;
import org.apache.activemq.broker.jmx.DurableSubscriptionViewMBean;
import org.apache.activemq.broker.jmx.ManagementContext;
import org.apache.activemq.broker.jmx.TopicViewMBean;
import org.apache.activemq.broker.jmx.QueueViewMBean;

import javax.management.MBeanServer;
import javax.management.MBeanServerInvocationHandler;
import javax.management.ObjectName;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

/**
 * A useful base class for an implementation of {@link BrokerFacade}
 *
 * @version $Revision$
 */
public abstract class BrokerFacadeSupport implements BrokerFacade {
    public abstract ManagementContext getManagementContext();

    public Collection getQueues() throws Exception {
        BrokerViewMBean broker = getBrokerAdmin();
        if (broker == null) {
            return Collections.EMPTY_LIST;
        }
        ObjectName[] queues = broker.getQueues();
        return getManagedObjects(queues, QueueViewMBean.class);
    }

    public Collection getTopics() throws Exception {
        BrokerViewMBean broker = getBrokerAdmin();
        if (broker == null) {
            return Collections.EMPTY_LIST;
        }
        ObjectName[] queues = broker.getTopics();
        return getManagedObjects(queues, TopicViewMBean.class);
    }

    public Collection getDurableTopicSubscribers() throws Exception {
        BrokerViewMBean broker = getBrokerAdmin();
        if (broker == null) {
            return Collections.EMPTY_LIST;
        }
        ObjectName[] queues = broker.getDurableTopicSubscribers();
        return getManagedObjects(queues, DurableSubscriptionViewMBean.class);
    }

    protected Collection getManagedObjects(ObjectName[] names, Class type) {
        List answer = new ArrayList();
        MBeanServer mbeanServer = getManagementContext().getMBeanServer();
        if (mbeanServer != null) {
            for (int i = 0; i < names.length; i++) {
                ObjectName name = names[i];
                Object value = MBeanServerInvocationHandler.newProxyInstance(mbeanServer, name, type, true);
                if (value != null) {
                    answer.add(value);
                }
            }
        }
        return answer;
    }
}
