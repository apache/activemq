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
package org.apache.activemq.broker.jmx;

import org.apache.activemq.broker.Broker;
import org.apache.activemq.broker.TransportConnection;
import org.apache.activemq.broker.TransportConnector;
import org.apache.activemq.command.ConnectionInfo;
import org.apache.activemq.command.Response;
import org.apache.activemq.thread.TaskRunnerFactory;
import org.apache.activemq.transport.Transport;
import org.apache.activemq.util.IOExceptionSupport;
import org.apache.activemq.util.JMXSupport;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import javax.management.MBeanServer;
import javax.management.ObjectName;

import java.io.IOException;
import java.util.Hashtable;

/**
 * A managed transport connection
 * 
 * @version $Revision: 1.1 $
 */
public class ManagedTransportConnection extends TransportConnection {
    private static final Log log = LogFactory.getLog(ManagedTransportConnection.class);

    private final MBeanServer server;
    private final ObjectName connectorName;
    private ConnectionViewMBean mbean;
    private ObjectName name;
    private String connectionId;

    public ManagedTransportConnection(TransportConnector connector, Transport transport, Broker broker, TaskRunnerFactory factory, MBeanServer server,
            ObjectName connectorName, String connectionId) throws IOException {
        super(connector, transport, broker, factory);
        this.server = server;
        this.connectorName = connectorName;
        this.mbean = new ConnectionView(this);
        setConnectionId(connectionId);
    }

    public void stop() throws Exception {
        unregisterMBean();
        super.stop();
    }

    public String getConnectionId() {
        return connectionId;
    }

    /**
     * Sets the connection ID of this connection. On startup this connection ID
     * is set to an incrementing counter; once the client registers it is set to
     * the clientID of the JMS client.
     */
    public void setConnectionId(String connectionId) throws IOException {
        this.connectionId = connectionId;
        unregisterMBean();
        name = createObjectName();
        registerMBean();
    }

    public Response processAddConnection(ConnectionInfo info) throws Throwable {
        Response answer = super.processAddConnection(info);
        String clientId = info.getClientId();
        if (clientId != null) {
            // lets update the MBean name
            setConnectionId(clientId);
        }
        return answer;
    }

    // Implementation methods
    // -------------------------------------------------------------------------
    protected void registerMBean() throws IOException {
        try {
            server.registerMBean(mbean, name);
        }
        catch (Throwable e) {
            throw IOExceptionSupport.create(e);
        }

    }

    protected void unregisterMBean() {
        if (name != null) {
            try {
                server.unregisterMBean(name);
            }
            catch (Throwable e) {
                log.warn("Failed to unregister mbean: " + name);
            }
        }
    }

    protected ObjectName createObjectName() throws IOException {
        // Build the object name for the destination
        Hashtable map = new Hashtable(connectorName.getKeyPropertyList());
        map.put("Type", "Connection");
        map.put("Connection", JMXSupport.encodeObjectNamePart(connectionId));
        try {
            return new ObjectName(connectorName.getDomain(), map);
        }
        catch (Throwable e) {
            throw IOExceptionSupport.create(e);
        }
    }
}
