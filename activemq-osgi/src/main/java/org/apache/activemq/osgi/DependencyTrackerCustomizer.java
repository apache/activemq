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
package org.apache.activemq.osgi;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.osgi.framework.BundleContext;
import org.osgi.framework.ServiceReference;
import org.osgi.util.tracker.ServiceTrackerCustomizer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DependencyTrackerCustomizer implements ServiceTrackerCustomizer<Object, Object> {

    private static final Logger LOG = LoggerFactory.getLogger(DependencyTrackerCustomizer.class);

    private final BundleContext bundleContext;
    private final Map<String, Boolean> services;
    private final BrokerCallback callback;

    public DependencyTrackerCustomizer(BundleContext bundleContext, Set<String> requiredServices, BrokerCallback callback) {
        this.bundleContext = bundleContext;
        this.services = new HashMap<>();
        requiredServices.forEach((requiredProtocol) -> {
            this.services.put(requiredProtocol, false);
        });
        this.callback = callback;
    }

    @Override
    public Object addingService(ServiceReference<Object> reference) {
        String serviceName = (String) reference.getProperty("osgi.jndi.service.name");
        Object serviceObject = bundleContext.getService(reference);
        callback.addService(serviceName, serviceObject);
        Boolean present = services.get(serviceName);
        if (present != null && !present) {
            services.put(serviceName, true);
            List<String> missing = getMissing();
            if (missing.isEmpty()) {
                try {
                    callback.startBroker();
                } catch (Exception e) {
                    LOG.error("Error starting broker", e);
                }
            } else {
                LOG.info("Still waiting for " + missing);
            }
        }
        return serviceObject;
    }

    @Override
    public void modifiedService(ServiceReference<Object> reference, Object service) {
    }

    @Override
    public void removedService(ServiceReference<Object> reference, Object service) {
        String serviceName = (String) reference.getProperty("osgi.jndi.service.name");
        Boolean present = services.get(serviceName);
        if (present != null && present) {
            List<String> missing = getMissing();
            if (missing.isEmpty()) {
                try {
                    callback.stopBroker();
                } catch (Exception e) {
                    LOG.error("Error stopping broker", e);
                }
            }
            callback.removeService(serviceName);
            services.put(serviceName, false);
        }
    }

    private List<String> getMissing() {
        List<String> missing = new ArrayList<>();
        services.keySet().forEach((service) -> {
            Boolean present = services.get(service);
            if (!present) {
                missing.add(service);
            }
        });
        return missing;
    }
}
