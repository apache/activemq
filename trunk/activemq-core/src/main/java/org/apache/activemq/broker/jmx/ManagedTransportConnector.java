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

import org.apache.activemq.broker.Connection;
import org.apache.activemq.broker.TransportConnector;
import org.apache.activemq.transport.Transport;
import org.apache.activemq.transport.TransportServer;
import java.io.IOException;
import java.net.URISyntaxException;
import javax.management.MBeanServer;
import javax.management.ObjectName;

/**
 * A managed transport connector which can create multiple managed connections
 * as clients connect.
 * 
 * 
 */
public class ManagedTransportConnector extends TransportConnector {

    static long nextConnectionId = 1;
    
    private final ManagementContext managementContext;
    private final ObjectName connectorName;

    public ManagedTransportConnector(ManagementContext context, ObjectName connectorName, TransportServer server) {
        super(server);
        this.managementContext = context;
        this.connectorName = connectorName;
    }

    public ManagedTransportConnector asManagedConnector(MBeanServer mbeanServer, ObjectName connectorName) throws IOException, URISyntaxException {
        return this;
    }

    protected Connection createConnection(Transport transport) throws IOException {
        return new ManagedTransportConnection(this, transport, getBroker(), isDisableAsyncDispatch() ? null : getTaskRunnerFactory(), managementContext, connectorName);
    }

    protected static synchronized long getNextConnectionId() {
        return nextConnectionId++;
    }

}
