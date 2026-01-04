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
package org.apache.activemq.spring.jetty;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;

import org.eclipse.jetty.server.HttpConfiguration;
import org.eclipse.jetty.server.HttpConnectionFactory;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.util.resource.Resource;
import org.eclipse.jetty.util.resource.ResourceFactory;
import org.eclipse.jetty.xml.XmlConfiguration;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.InitializingBean;

/**
 * Demonstration of using the XmlConfiguration to build up a server with multiple XML files and Properties.
 */
public class JettyServerBean implements InitializingBean, DisposableBean {

    boolean httpEnabled = true;
    boolean httpsEnabled = false;
    String jettyXmlDirectory = Path.of("conf", "jetty").toString();
    String jettyConfDirectory = Path.of("conf").toString();
    String webAppsContext = null;

    // List of configured IDs from XML;
    Map<String, Object> idMap;

    // The list of XMLs in the order they should be executed.
    List<Resource> xmls = new ArrayList<>();

    private Server server;


    /**
     * Configure for the list of XML Resources and Properties.
     *
     * @param xmls the xml resources (in order of execution)
     * @param properties the properties to use with the XML
     * @return the ID Map of configured objects (key is the id name in the XML, and the value is configured object)
     * @throws Exception if unable to create objects or read XML
     */
    public Map<String, Object> configure(List<Resource> xmls, Map<String, String> properties) throws Exception {
        Map<String, Object> idMap = new HashMap<>();

        // Configure everything
        for (Resource xmlResource : xmls)
        {
            XmlConfiguration configuration = new XmlConfiguration(xmlResource);
            configuration.getIdMap().putAll(idMap);
            configuration.getProperties().putAll(properties);
            configuration.configure();
            idMap.putAll(configuration.getIdMap());
        }

        return idMap;
    }

    private static void ensureDirExists(Path path) throws IOException {
        if (!Files.exists(path))
        {
            Files.createDirectories(path);
        }
    }

    private static Map<String, String> loadProperties(Resource resource) throws IOException {
        Properties properties = new Properties();

        try (InputStream in = resource.newInputStream()) {
            properties.load(in);
        }

        return properties.entrySet().stream().collect(
                Collectors.toMap(
                        e -> String.valueOf(e.getKey()),
                        e -> String.valueOf(e.getValue()),
                        (prev, next) -> next, HashMap::new
                ));
    }

    @Override
    public void destroy() throws Exception {
        // TODO: review if we need to check
        //  && !server.isStopping()
        if(server != null) {
            server.stop();
        }
    }

    @Override
    public void afterPropertiesSet() throws Exception {
        try(ResourceFactory.Closeable resourceFactory = ResourceFactory.closeable())
        {
            Resource homeXmlResource = resourceFactory.newResource(Path.of(getJettyXmlDirectory()));
            Resource customBaseResource = resourceFactory.newResource(Path.of(getJettyConfDirectory()));

            xmls.add(homeXmlResource.resolve("jetty-bytebufferpool.xml"));
            xmls.add(homeXmlResource.resolve("jetty-threadpool.xml"));
            xmls.add(homeXmlResource.resolve("jetty.xml"));
            if (isHttpEnabled()) {
                xmls.add(homeXmlResource.resolve("jetty-http.xml"));
            }
            if (isHttpsEnabled()) {
                xmls.add(homeXmlResource.resolve("jetty-ssl.xml"));
                xmls.add(homeXmlResource.resolve("jetty-ssl-context.xml"));
                xmls.add(homeXmlResource.resolve("jetty-https.xml"));
            }
            // xmls.add(homeXmlResource.resolve("jetty-customrequestlog.xml"));

            // Now we add our customizations
            // In this case, it's 2 ServletContextHandlers
            // xmls.add(homeXmlResource.resolve("jetty-webapps.xml/context-activemq-console.xml"));
            if(getWebAppsContext() != null) {
                xmls.add(homeXmlResource.resolve(getWebAppsContext()));
            }

            // Lets load our properties
            Map<String, String> customProps = loadProperties(customBaseResource.resolve("jetty.properties"));

            // Create a path suitable for output / work directory / etc.
            Path outputPath = Paths.get("target/xmlserver-output");
            Path resourcesPath = outputPath.resolve("resources");

            ensureDirExists(outputPath);
            ensureDirExists(outputPath.resolve("logs"));
            ensureDirExists(resourcesPath);
            ensureDirExists(resourcesPath.resolve("bar"));
            ensureDirExists(resourcesPath.resolve("foo"));

            // And define some common properties
            // These 2 properties are used in MANY PLACES, define them, even if you don't use them fully.
            customProps.put("jetty.home", outputPath.toString());
            customProps.put("jetty.base", outputPath.toString());
            // And define the resource paths for the contexts
            customProps.put("custom.resources", resourcesPath.toString());
            customProps.put("jetty.sslContext.keyStoreAbsolutePath", customBaseResource.resolve("keystore").toString());
            customProps.put("jetty.sslContext.trustStoreAbsolutePath", customBaseResource.resolve("keystore").toString());

            // Now lets tie it all together
            idMap = configure(xmls, customProps);
        }

        Server tmpServer = (Server)idMap.get("Server");
        tmpServer.start();
        System.out.println("Server is running, and listening on ...");
        for (ServerConnector connector : tmpServer.getBeans(ServerConnector.class))
        {
            for (HttpConnectionFactory connectionFactory : connector.getBeans(HttpConnectionFactory.class))
            {
                String scheme = "http";
                HttpConfiguration httpConfiguration = connectionFactory.getHttpConfiguration();
                if (httpConfiguration.getSecurePort() == connector.getLocalPort())
                    scheme = httpConfiguration.getSecureScheme();
                String host = connector.getHost();
                if (host == null)
                    host = InetAddress.getLocalHost().getHostAddress();
                System.out.printf("   %s://%s:%s/%n", scheme, host, connector.getLocalPort());
            }
        }
        tmpServer.join();
        this.server = tmpServer;
    }

    public void setHttpEnabled(boolean httpEnabled) {
        this.httpEnabled = httpEnabled;
    }

    public boolean isHttpEnabled() {
        return this.httpEnabled;
    }

    public void setHttpsEnabled(boolean httpsEnabled) {
        this.httpsEnabled = httpsEnabled;;
    }

    public boolean isHttpsEnabled() {
        return this.httpsEnabled;
    }

    public void setJettyConfDirectory(String jettyConfDirectory) {
        this.jettyConfDirectory = jettyConfDirectory;
    }

    public String getJettyConfDirectory() {
        return jettyConfDirectory;
    }

    public void setJettyXmlDirectory(String jettyXmlDirectory) {
        this.jettyXmlDirectory = jettyXmlDirectory;
    }

    public String getJettyXmlDirectory() {
        return jettyXmlDirectory;
    }

    public void setWebAppsContext(String webAppsContext) {
        this.webAppsContext = webAppsContext;
    }

    public String getWebAppsContext() {
        return webAppsContext;
    }
}