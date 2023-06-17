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
package org.apache.activemq.replica;

import org.apache.activemq.broker.BrokerContext;
import org.apache.activemq.broker.BrokerService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Method;
import java.util.Locale;
import java.util.Map;

public class WebConsoleAccessController {

    private final Logger logger = LoggerFactory.getLogger(WebConsoleAccessController.class);

    private final BrokerService brokerService;
    private Class<?> serverClass;
    private Class<?> connectorClass;
    private Method getConnectorsMethod;
    private Method startMethod;
    private Method stopMethod;
    private boolean initialized;

    public WebConsoleAccessController(BrokerService brokerService, boolean enabled) {
        this.brokerService = brokerService;
        if (!enabled) {
            return;
        }
        try {
            serverClass = getClass().getClassLoader().loadClass("org.eclipse.jetty.server.Server");
            connectorClass = getClass().getClassLoader().loadClass("org.eclipse.jetty.server.Connector");

            getConnectorsMethod = serverClass.getMethod("getConnectors");
            startMethod = connectorClass.getMethod("start");
            stopMethod = connectorClass.getMethod("stop");
            initialized = true;
        } catch (ClassNotFoundException | NoSuchMethodException e) {
            logger.error("Unable to initialize class", e);
        }
    }

    public void start() {
        invoke(startMethod);
    }

    public void stop() {
        invoke(stopMethod);
    }

    private void invoke(Method method) {
        if (!initialized) {
            return;
        }

        if (brokerService.getBrokerContext() != null) {
            invoke(method, brokerService.getBrokerContext());
            return;
        }

        new Thread(() -> {
            BrokerContext brokerContext;
            while ((brokerContext = brokerService.getBrokerContext()) == null) {
                try {
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                    logger.error("brokerContext initialization interrupted", e);
                    return;
                }
            }
            invoke(method, brokerContext);
        }).start();
    }

    private void invoke(Method method, BrokerContext brokerContext) {
        try {
            Map<String, ?> servers = brokerContext.getBeansOfType(serverClass);
            if (servers.size() > 0) {
                for (Map.Entry<String, ?> server : servers.entrySet()) {
                    if (server.getKey().toLowerCase(Locale.ROOT).contains("jolokia")) {
                        continue;
                    }

                    Object[] connectors = (Object[]) getConnectorsMethod.invoke(server.getValue());
                    for (Object connector : connectors) {
                        method.invoke(connector);
                    }
                }
            }
        } catch (Exception e) {
            logger.error("Unable to {} web console connectors", method.getName(), e);
        }
    }
}
