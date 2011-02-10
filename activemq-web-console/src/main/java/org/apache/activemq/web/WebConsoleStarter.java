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

import javax.jms.ConnectionFactory;
import javax.servlet.ServletContext;
import javax.servlet.ServletContextEvent;
import javax.servlet.ServletContextListener;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.context.WebApplicationContext;
import org.springframework.web.context.support.WebApplicationContextUtils;
import org.springframework.web.context.support.XmlWebApplicationContext;

/**
 * Starts the WebConsole.
 * 
 * @version $Revision: 1.1 $
 */
public class WebConsoleStarter implements ServletContextListener {
    
    private static final Logger LOG = LoggerFactory.getLogger(WebConsoleStarter.class);

    public void contextInitialized(ServletContextEvent event) {
        LOG.debug("Initializing ActiveMQ WebConsole...");

        ServletContext servletContext = event.getServletContext();
        WebApplicationContext context = createWebapplicationContext(servletContext);

        initializeWebClient(servletContext, context);

        LOG.info("ActiveMQ WebConsole initialized.");
    }

    private WebApplicationContext createWebapplicationContext(ServletContext servletContext) {
        String webconsoleType = System.getProperty("webconsole.type", "embedded");
        String configuration = "/WEB-INF/webconsole-" + webconsoleType + ".xml";

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
    }

    public void contextDestroyed(ServletContextEvent event) {
        XmlWebApplicationContext context = (XmlWebApplicationContext)WebApplicationContextUtils.getWebApplicationContext(event.getServletContext());
        if (context != null) {
            context.stop();
            context.destroy();
        }
        // do nothing, since the context is destoyed anyway
    }

}
