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
package org.apache.activemq.tool;
import org.mortbay.jetty.Connector;
import org.mortbay.jetty.Server;
import org.mortbay.jetty.bio.SocketConnector;
import org.mortbay.jetty.webapp.WebAppContext;

/**
 * @version $Revision$
 */
public class WebServer {
    public static final int PORT = 8080;
    //public static final String WEBAPP_DIR = "target/activemq";
    public static final String WEBAPP_DIR = "src/webapp";
    public static final String WEBAPP_CTX = "/";

    public static void main(String[] args) throws Exception {
        Server server = new Server();
        Connector context = new SocketConnector();
        context.setServer(server);
        context.setPort(PORT);
        
        String webappDir = WEBAPP_DIR;
        if( args.length > 0 ) {
        	webappDir = args[0];
        }
        
        WebAppContext webapp = new WebAppContext();
        webapp.setServer(server);
        webapp.setContextPath(WEBAPP_CTX);
        webapp.setResourceBase(webappDir);
       
        server.setHandler(webapp);
        
        server.setConnectors(new Connector[]{context});
        server.start();

    }
}
