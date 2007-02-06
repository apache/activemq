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
import org.apache.activemq.broker.jmx.ManagementContext;
import org.apache.activemq.command.ActiveMQDestination;

import javax.management.MBeanServerConnection;
import javax.management.MBeanServerInvocationHandler;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;

/**
 * A {@link BrokerFacade} which uses JMX to communicate with a remote broker
 *
 * @version $Revision$
 */
public class JMXBrokerFacade extends BrokerFacadeSupport {
    private ManagementContext managementContext;
    private ObjectName brokerName;

    public BrokerViewMBean getBrokerAdmin() throws Exception {
        MBeanServerConnection mbeanServer = getManagementContext().getMBeanServer();
        return (BrokerViewMBean) MBeanServerInvocationHandler.newProxyInstance(mbeanServer, getBrokerName(), BrokerViewMBean.class, true);
    }

    public void purgeQueue(ActiveMQDestination destination) throws Exception {
        /** TODO */
    }

    public ManagementContext getManagementContext() {
        if (managementContext == null) {
            managementContext = new ManagementContext();
            managementContext.setCreateConnector(true);
        }
        return managementContext;
    }

    public void setManagementContext(ManagementContext managementContext) {
        this.managementContext = managementContext;
    }

    public ObjectName getBrokerName() throws MalformedObjectNameException {
        if (brokerName == null) {
            brokerName = createBrokerName();
        }
        return brokerName;
    }

    public void setBrokerName(ObjectName brokerName) {
        this.brokerName = brokerName;
    }

    protected ObjectName createBrokerName() throws MalformedObjectNameException {
        return new ObjectName(getManagementContext().getJmxDomainName() + ":Type=Broker");
    }
}
