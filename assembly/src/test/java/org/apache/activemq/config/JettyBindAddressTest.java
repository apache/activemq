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
package org.apache.activemq.config;

import static org.junit.Assert.assertEquals;

import java.io.FileInputStream;
import java.io.InputStream;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.activemq.spring.jetty.JettyServerBean;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.util.resource.Resource;
import org.eclipse.jetty.util.resource.ResourceFactory;
import org.junit.Test;

/**
 * Applies the shipped Jetty XML files with the shipped jetty-spring.properties, without
 * starting the server. A connector without a host binds every interface, so the web
 * console relies on these properties to stay on loopback.
 */
public class JettyBindAddressTest {

    private static final Path CONF = Path.of("src/release/conf");

    @Test
    public void consoleConnectorsBindToLoopbackByDefault() throws Exception {
        Map<String, String> properties = new HashMap<>();
        Properties shipped = new Properties();
        try (InputStream in = new FileInputStream(CONF.resolve("jetty-spring.properties").toFile())) {
            shipped.load(in);
        }
        shipped.forEach((key, value) -> properties.put(String.valueOf(key), String.valueOf(value)));

        List<Resource> xmls = new ArrayList<>();
        for (String xml : new String[] {"jetty-bytebufferpool.xml", "jetty-threadpool.xml", "jetty-scheduler.xml",
                "jetty-http-config.xml", "jetty.xml", "jetty-http.xml", "jetty-ssl.xml"}) {
            xmls.add(ResourceFactory.root().newResource(CONF.resolve("jetty").resolve(xml)));
        }

        Map<String, Object> ids = new JettyServerBean().configure(xmls, properties);

        assertEquals("127.0.0.1", ((ServerConnector) ids.get("httpConnector")).getHost());
        assertEquals("127.0.0.1", ((ServerConnector) ids.get("sslConnector")).getHost());
    }
}
