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
package org.apache.activemq.transport;

import java.util.Map;

import org.apache.activemq.util.IntrospectionSupport;
import org.eclipse.jetty.server.Connector;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;

public class SocketConnectorFactory {

    private Map<String, Object> transportOptions;

    public Connector createConnector(Server server) throws Exception {
        ServerConnector connector = new ServerConnector(server);
        server.setStopTimeout(500);
        connector.setStopTimeout(500);
        if (transportOptions != null) {
            IntrospectionSupport.setProperties(connector, transportOptions, "");
        }
        return connector;
    }

    public Map<String, Object> getTransportOptions() {
        return transportOptions;
    }

    public void setTransportOptions(Map<String, Object> transportOptions) {
        this.transportOptions = transportOptions;
    }
}
