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

import org.osgi.framework.FrameworkUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.context.WebApplicationContext;
import org.springframework.web.context.support.WebApplicationContextUtils;
import org.springframework.web.context.support.XmlWebApplicationContext;

import javax.jms.ConnectionFactory;
import javax.servlet.ServletContext;
import javax.servlet.ServletContextEvent;
import javax.servlet.ServletContextListener;

/**
 * Starts the WebConsole.
 */
public class WebConsoleStarter implements ServletContextListener {
    
    private static final Logger LOG = LoggerFactory.getLogger(WebConsoleStarter.class);

    public void contextInitialized(ServletContextEvent event) {
        LOG.debug("Initializing ActiveMQ WebConsole...");

        String webconsoleType = getWebconsoleType();

        ServletContext servletContext = event.getServletContext();
        WebApplicationContext context = createWebapplicationContext(servletContext, webconsoleType);

        initializeWebClient(servletContext, context);

        // for embedded console log what port it uses
        if ("embedded".equals(webconsoleType)) {
            // show the url for the web consoles / main page so people can spot it
            String port = System.getProperty("jetty.port");
            String host = System.getProperty("jetty.host");
            if (host != null && port != null) {
                LOG.info("ActiveMQ WebConsole available at http://{}:{}/", host, port);
                LOG.info("ActiveMQ Jolokia REST API available at http://{}:{}/api/jolokia/", host, port);
            }
        }

        LOG.debug("ActiveMQ WebConsole initialized.");
    }

    private WebApplicationContext createWebapplicationContext(ServletContext servletContext, String webconsoleType) {
        String configuration = "/WEB-INF/webconsole-" + webconsoleType + ".xml";
        LOG.debug("Web console type: " + webconsoleType);

        XmlWebApplicationContext context = new XmlWebApplicationContext();
        context.setServletContext(servletContext);
        context.setConfigLocations(new String[] {
            configuration
        });
        context.refresh();
        context.start();

        servletContext.setAttribute(WebApplicationContext.ROOT_WEB_APPLICATION_CONTEXT_ATTRIBUTE, context);

        return context;
    }

    private void initializeWebClient(ServletContext servletContext, WebApplicationContext context) {
        ConnectionFactory connectionFactory = (ConnectionFactory)context.getBean("connectionFactory");
        servletContext.setAttribute(WebClient.CONNECTION_FACTORY_ATTRIBUTE, connectionFactory);
        WebClient.initContext(servletContext);
    }

    public void contextDestroyed(ServletContextEvent event) {
        XmlWebApplicationContext context = (XmlWebApplicationContext)WebApplicationContextUtils.getWebApplicationContext(event.getServletContext());
        if (context != null) {
            context.stop();
            context.destroy();
        }
        // do nothing, since the context is destroyed anyway
    }

    private static String getWebconsoleType() {
        String webconsoleType = System.getProperty("webconsole.type", "embedded");

        // detect osgi
        try {
            if (OsgiUtil.isOsgi()) {
                webconsoleType = "osgi";
            }
        } catch (NoClassDefFoundError ignore) {
        }

        return webconsoleType;
    }

    static class OsgiUtil {
        static boolean isOsgi() {
            return (FrameworkUtil.getBundle(WebConsoleStarter.class) != null);
        }
    }

}
