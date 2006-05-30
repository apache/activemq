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
package org.apache.activemq.web;

import org.apache.activemq.broker.Broker;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.jmx.BrokerView;
import org.apache.activemq.broker.jmx.BrokerViewMBean;
import org.apache.activemq.broker.jmx.DurableSubscriptionViewMBean;
import org.apache.activemq.broker.jmx.ManagedRegionBroker;
import org.apache.activemq.broker.jmx.ManagementContext;
import org.apache.activemq.broker.jmx.QueueViewMBean;
import org.apache.activemq.broker.jmx.TopicViewMBean;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import javax.management.MBeanServer;
import javax.management.MBeanServerInvocationHandler;
import javax.management.ObjectName;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

/**
 * 
 * @version $Revision$
 */
public class BrokerFacade {
    private static final Log log = LogFactory.getLog(BrokerFacade.class);

    private BrokerService brokerService;

    public BrokerFacade(BrokerService brokerService) {
        this.brokerService = brokerService;
    }

    public BrokerService getBrokerService() {
        return brokerService;
    }

    public Broker getBroker() throws Exception {
        return brokerService.getBroker();
    }

    public ManagementContext getManagementContext() {
        return brokerService.getManagementContext();
    }

    public BrokerViewMBean getBrokerAdmin() throws Exception {
        // TODO could use JMX to look this up
        return brokerService.getAdminView();
    }

    public ManagedRegionBroker getManagedBroker() throws Exception {
        BrokerView adminView = brokerService.getAdminView();
        if (adminView == null) {
            return null;
        }
        return adminView.getBroker();
    }

    // TODO - we should not have to use JMX to implement the following methods...
    
    /*
    public Collection getQueues() throws Exception {
        BrokerView broker = brokerService.getAdminView();
        if (broker == null) {
            return Collections.EMPTY_LIST;
        }
        ObjectName[] queues = broker.getQueues();
        return getManagedObjects(queues, QueueViewMBean.class);
    }
    */
    public Collection getQueues() throws Exception {
        ManagedRegionBroker broker = getManagedBroker();
        if (broker == null) {
            return new ArrayList();
        }
        return broker.getQueueRegion().getDestinationMap().values();
    }

    
    public Collection getTopics() throws Exception {
        BrokerView broker = brokerService.getAdminView();
        if (broker == null) {
            return Collections.EMPTY_LIST;
        }
        ObjectName[] queues = broker.getTopics();
        return getManagedObjects(queues, TopicViewMBean.class);
    }
    
    public Collection getDurableTopicSubscribers() throws Exception {
        BrokerView broker = brokerService.getAdminView();
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

    /**
     * 
     * 
     * public Collection getTopics() throws Exception { ManagedRegionBroker
     * broker = getManagedBroker(); if (broker == null) { return new
     * ArrayList(); } return
     * broker.getTopicRegion().getDestinationMap().values(); }
     */

}
