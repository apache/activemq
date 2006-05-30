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
package org.apache.activeio.oneport.jetty;

import org.apache.activeio.oneport.OnePortAsyncChannelServerTest;
import org.apache.activeio.oneport.jetty.JettyOnePortSocketListener;
import org.mortbay.http.HttpContext;
import org.mortbay.http.HttpServer;
import org.mortbay.jetty.servlet.ServletHandler;

import edu.emory.mathcs.backport.java.util.concurrent.BlockingQueue;

/**
 * 
 */
public class JettyOnePortSocketListenerTest extends OnePortAsyncChannelServerTest {

    static public BlockingQueue staticResultSlot;

    private HttpServer jetty;

    protected void startHTTPServer() throws Exception {
        staticResultSlot = resultSlot;

        // Create the server
        jetty = new HttpServer();

        // Create a port listener
        JettyOnePortSocketListener listener = new JettyOnePortSocketListener(server);
        jetty.addListener(listener);

        // Create a context
        HttpContext context = new HttpContext();
        context.setContextPath("/");
        jetty.addContext(context);

        // Create a servlet container
        ServletHandler servlets = new ServletHandler();
        context.addHandler(servlets);

        // Map a servlet onto the container
        servlets.addServlet("Test", "*", TestServlet.class.getName());

        // Start the http server
        jetty.start();
    }

    protected void stopHTTPServer() throws InterruptedException {
        jetty.stop();
    }
}
