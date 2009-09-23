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
package org.apache.activemq.transport.discovery.http;

import java.net.URI;

import org.mortbay.jetty.Server;
import org.mortbay.jetty.nio.SelectChannelConnector;
import org.mortbay.jetty.servlet.Context;
import org.mortbay.jetty.servlet.ServletHolder;

public class EmbeddedJettyServer implements org.apache.activemq.Service {

    private HTTPDiscoveryAgent agent;
    private Server server;
    private SelectChannelConnector connector;
    private DiscoveryRegistryServlet camelServlet = new DiscoveryRegistryServlet();
    
    public void start() throws Exception {
        URI uri = new URI(agent.getRegistryURL());

        server = new Server();
        Context context = new Context(Context.NO_SECURITY | Context.NO_SESSIONS);
        
        context.setContextPath("/");
        ServletHolder holder = new ServletHolder();
        holder.setServlet(camelServlet);
        context.addServlet(holder, "/*");
        server.setHandler(context);
        server.start();
        
        int port = 80;
        if( uri.getPort() >=0 ) {
            port = uri.getPort();
        }
        
        connector = new SelectChannelConnector();
        connector.setPort(port);
        server.addConnector(connector);
        connector.start();
    }

    public void stop() throws Exception {
        if( connector!=null ) {
            connector.stop();
            connector = null;
        }
        if( server!=null ) {
            server.stop();
            server = null;
        }
    }

    public HTTPDiscoveryAgent getAgent() {
        return agent;
    }

    public void setAgent(HTTPDiscoveryAgent agent) {
        this.agent = agent;
    }
    

}
