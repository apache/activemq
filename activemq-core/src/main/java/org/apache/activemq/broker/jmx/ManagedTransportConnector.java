/**
 *
 * Copyright 2004 The Apache Software Foundation
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
import org.apache.activemq.broker.Connection;
import org.apache.activemq.broker.TransportConnector;
import org.apache.activemq.transport.Transport;
import org.apache.activemq.transport.TransportServer;

import javax.management.MBeanServer;
import javax.management.ObjectName;

import java.io.IOException;
import java.net.URISyntaxException;

/**
 * A managed transport connector which can create multiple managed connections
 * as clients connect.
 * 
 * @version $Revision: 1.1 $
 */
public class ManagedTransportConnector extends TransportConnector {

    private final MBeanServer mbeanServer;
    private final ObjectName connectorName;
    long nextConnectionId = 1;

    public ManagedTransportConnector(MBeanServer mbeanServer, ObjectName connectorName, Broker next, TransportServer server) {
        super(next, server);
        this.mbeanServer = mbeanServer;
        this.connectorName = connectorName;
    }

    public ManagedTransportConnector asManagedConnector(MBeanServer mbeanServer, ObjectName connectorName) throws IOException, URISyntaxException {
        return this;
    }

    protected Connection createConnection(Transport transport) throws IOException {

        final String connectionId;
        synchronized (this) {
            connectionId = "" + (nextConnectionId++);
        }

        return new ManagedTransportConnection(this, transport, getBrokerFilter(), getTaskRunnerFactory(), mbeanServer, connectorName, connectionId);
    }

}
