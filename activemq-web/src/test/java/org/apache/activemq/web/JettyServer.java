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

import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.demo.DefaultQueueSender;
import org.eclipse.jetty.server.Connector;
import org.eclipse.jetty.server.Handler;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.nio.SelectChannelConnector;
import org.eclipse.jetty.webapp.WebAppContext;

/**
 * A simple bootstrap class for starting Jetty in your IDE using the local web
 * application.
 * 
 * @version $Revision$
 */
public final class JettyServer {

    public static final int PORT = 8080;

    public static final String WEBAPP_DIR = "src/main/webapp";

    public static final String WEBAPP_CTX = "/";

    private JettyServer() {
    }

    public static void main(String[] args) throws Exception {
        // lets create a broker
        BrokerService broker = new BrokerService();
        broker.setPersistent(false);
        broker.setUseJmx(true);
        broker.addConnector("tcp://localhost:61616");
        broker.addConnector("stomp://localhost:61613");
        broker.start();

        // lets publish some messages so that there is some stuff to browse
        DefaultQueueSender.main(new String[] {
            "FOO.BAR"
        });

        // now lets start the web server
        int port = PORT;
        if (args.length > 0) {
            String text = args[0];
            port = Integer.parseInt(text);
        }
        System.out.println("Starting Web Server on port: " + port);
        Server server = new Server();
        SelectChannelConnector connector = new SelectChannelConnector();
        connector.setPort(port);
        connector.setServer(server);
        WebAppContext context = new WebAppContext();

        context.setResourceBase(WEBAPP_DIR);
        context.setContextPath(WEBAPP_CTX);
        context.setServer(server);
        /*server.setHandlers(new Handler[] {
            context
        });*/
        server.setConnectors(new Connector[] {
            connector
        });
        server.start();
    }
}
