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

package org.apache.activemq.transport.http.openwire;

import org.apache.activemq.openwire.OpenWireFormatFactory;
import org.apache.activemq.transport.http.HttpTransportFactory;
import org.apache.activemq.transport.http.HttpTransportServer;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.util.component.AbstractLifeCycle.AbstractLifeCycleListener;
import org.eclipse.jetty.util.component.LifeCycle;

import java.net.URI;

public class CustomHttpTransportServer extends HttpTransportServer {
    private final HttpTransportFactory transportFactory;

    public CustomHttpTransportServer(final URI location, final CustomHttpTransportFactory transportFactory) {
        super(location, transportFactory);
        this.transportFactory = transportFactory;
    }

    @Override
    protected void createServer() {
        super.createServer();

        server.addLifeCycleListener(new AbstractLifeCycleListener()
        {
            @Override
            public void lifeCycleStarting(final LifeCycle event)
            {
                setupServletContext((ServletContextHandler)server.getHandler());
            }
        });
    }

    private void setupServletContext(final ServletContextHandler handler) {
        ServletContextAttributes.setAcceptListener(handler, getAcceptListener());
        ServletContextAttributes.setTransportOptions(handler, transportOptions);
        ServletContextAttributes.setTransportFactory(handler, transportFactory);
        ServletContextAttributes.setWireFormat(handler, new OpenWireFormatFactory().createWireFormat());
    }
}
