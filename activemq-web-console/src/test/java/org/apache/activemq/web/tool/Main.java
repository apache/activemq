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

package org.apache.activemq.web.tool;

import org.apache.activemq.web.config.JspConfigurer;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.handler.ContextHandlerCollection;
import org.eclipse.jetty.webapp.WebAppContext;

/**
 * A simple bootstrap class for starting Jetty in your IDE using the local web
 * application.
 *
 *
 */
public final class Main {

    public static final int PORT = 8080;

    public static final String WEBAPP_DIR = "src/main/webapp";

    public static final String WEBAPP_CTX = "/";

    private Main() {
    }

    public static void main(String[] args) throws Exception {
        // now lets start the web server
        int port = PORT;
        if (args.length > 0) {
            String text = args[0];
            port = Integer.parseInt(text);
        }

        System.out.println("Starting Web Server on port: " + port);
        System.setProperty("jetty.port", "" + port);
        Server server = new Server(port);

        //System.setProperty("webconsole.type","properties");
        //System.setProperty("webconsole.jms.url","tcp://localhost:61616");
        //System.setProperty("webconsole.jmx.url","service:jmx:rmi:///jndi/rmi://localhost:1099/karaf-root");

        WebAppContext context = new WebAppContext();
        ContextHandlerCollection handlers = new ContextHandlerCollection();
        handlers.setHandlers(new WebAppContext[] {context});

        JspConfigurer.configureJetty(server, handlers);


        context.setResourceBase(WEBAPP_DIR);
        context.setContextPath(WEBAPP_CTX);
        context.setServer(server);
        server.setHandler(handlers);
        server.start();

        System.out.println();
        System.out.println("==============================================================================");
        System.out.println("Started the ActiveMQ Console: point your web browser at http://localhost:" + port + "/");
        System.out.println("==============================================================================");
        System.out.println();
    }


}
