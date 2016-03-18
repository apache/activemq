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

import org.apache.activemq.transport.TransportAcceptListener;
import org.apache.activemq.transport.http.HttpTransportFactory;
import org.apache.activemq.wireformat.WireFormat;
import org.eclipse.jetty.servlet.ServletContextHandler;

import java.util.Map;

public final class ServletContextAttributes {

    private ServletContextAttributes() {}

    public static void setWireFormat(final ServletContextHandler servletContext, final WireFormat wireFormat) {
        servletContext.setAttribute("wireFormat", wireFormat);
    }

    public static void setTransportFactory(final ServletContextHandler servletContext, final HttpTransportFactory transportFactory) {
        servletContext.setAttribute("transportFactory", transportFactory);
    }

    public static void setTransportOptions(final ServletContextHandler servletContext, final Map<String, Object> transportOptions) {
        servletContext.setAttribute("transportOptions", transportOptions);
    }

    public static void setAcceptListener(final ServletContextHandler servletContext, final TransportAcceptListener acceptListener) {
        servletContext.setAttribute("acceptListener", acceptListener);
    }
}
