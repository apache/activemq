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
package org.apache.activemq.broker.jmx;

import java.io.IOException;

import javax.management.ObjectName;

import org.apache.activemq.broker.Broker;
import org.apache.activemq.broker.TransportConnection;
import org.apache.activemq.broker.TransportConnector;
import org.apache.activemq.command.ConnectionInfo;
import org.apache.activemq.command.Response;
import org.apache.activemq.thread.TaskRunnerFactory;
import org.apache.activemq.transport.Transport;
import org.apache.activemq.util.IOExceptionSupport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A managed transport connection
 */
public class ManagedTransportConnection extends TransportConnection {
    private static final Logger LOG = LoggerFactory.getLogger(ManagedTransportConnection.class);

    private final ManagementContext managementContext;
    private final ObjectName connectorName;
    private final ConnectionViewMBean mbean;

    private ObjectName byClientIdName;
    private ObjectName byAddressName;

    private final boolean populateUserName;

    public ManagedTransportConnection(TransportConnector connector, Transport transport, Broker broker,
                                      TaskRunnerFactory factory, TaskRunnerFactory stopFactory,
                                      ManagementContext context, ObjectName connectorName)
        throws IOException {
        super(connector, transport, broker, factory, stopFactory);
        this.managementContext = context;
        this.connectorName = connectorName;
        this.mbean = new ConnectionView(this, managementContext);
        this.populateUserName = broker.getBrokerService().isPopulateUserNameInMBeans();
        if (managementContext.isAllowRemoteAddressInMBeanNames()) {
            byAddressName = createObjectName("remoteAddress", transport.getRemoteAddress());
            registerMBean(byAddressName);
        }
    }

    @Override
    public void stopAsync() {
        super.stopAsync();
        synchronized (this) {
            unregisterMBean(byClientIdName);
            unregisterMBean(byAddressName);
            byClientIdName = null;
            byAddressName = null;
        }
    }

    @Override
    public Response processAddConnection(ConnectionInfo info) throws Exception {
        Response answer = super.processAddConnection(info);
        String clientId = info.getClientId();
        if (populateUserName) {
            ((ConnectionView) mbean).setUserName(info.getUserName());
        }
        if (clientId != null) {
            if (byClientIdName == null) {
                byClientIdName = createObjectName("clientId", clientId);
                registerMBean(byClientIdName);
            }
        }
        return answer;
    }

    // Implementation methods
    // -------------------------------------------------------------------------
    protected void registerMBean(ObjectName name) {
        if (name != null) {
            try {
                AnnotatedMBean.registerMBean(managementContext, mbean, name);
            } catch (Throwable e) {
                LOG.warn("Failed to register MBean {}", name);
                LOG.debug("Failure reason: ", e);
            }
        }
    }

    protected void unregisterMBean(ObjectName name) {
        if (name != null) {
            try {
                managementContext.unregisterMBean(name);
            } catch (Throwable e) {
                LOG.warn("Failed to unregister MBean {}", name);
                LOG.debug("Failure reason: ", e);
            }
        }
    }

    protected ObjectName createObjectName(String type, String value) throws IOException {
        try {
            return BrokerMBeanSupport.createConnectionViewByType(connectorName, type, value);
        } catch (Throwable e) {
            throw IOExceptionSupport.create(e);
        }
    }

}
