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
package org.apache.activemq.broker;

import java.net.URI;
import java.util.HashMap;
import java.util.Map;

import org.apache.activemq.util.IntrospectionSupport;
import org.apache.activemq.util.URISupport;
import org.apache.activemq.util.URISupport.CompositeData;

/**
 * Simple BrokerFactorySPI which using the brokerURI to extract the
 * configuration parameters for the broker service. This directly configures the
 * pojo model so there is no dependency on spring for configuration.
 * 
 * 
 */
public class DefaultBrokerFactory implements BrokerFactoryHandler {

    public BrokerService createBroker(URI brokerURI) throws Exception {

        CompositeData compositeData = URISupport.parseComposite(brokerURI);
        Map<String, String> params = new HashMap<String, String>(compositeData.getParameters());

        BrokerService brokerService = new BrokerService();
        IntrospectionSupport.setProperties(brokerService, params);
        if (compositeData.getPath() != null) {
            brokerService.setBrokerName(compositeData.getPath());
        }

        URI[] components = compositeData.getComponents();
        for (int i = 0; i < components.length; i++) {
            if ("network".equals(components[i].getScheme())) {
                brokerService.addNetworkConnector(components[i].getSchemeSpecificPart());
            } else if ("proxy".equals(components[i].getScheme())) {
                brokerService.addProxyConnector(components[i].getSchemeSpecificPart());
            } else {
                brokerService.addConnector(components[i]);
            }
        }
        return brokerService;
    }

}
