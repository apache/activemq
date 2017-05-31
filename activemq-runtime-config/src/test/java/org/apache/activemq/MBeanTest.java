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
package org.apache.activemq;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

import java.util.HashMap;

import javax.management.ObjectName;

import org.apache.activemq.plugin.RuntimeConfigurationBroker;
import org.apache.activemq.plugin.jmx.RuntimeConfigurationViewMBean;
import org.apache.activemq.util.IntrospectionSupport;
import org.junit.Test;

public class MBeanTest extends RuntimeConfigTestSupport {

    @Test
    public void testUpdateNow() throws Exception {
        final String brokerConfig =  "mBeanTest-manual-broker";
        applyNewConfig(brokerConfig, "emptyManualUpdateConfig");
        startBroker(brokerConfig);
        assertTrue("broker alive", brokerService.isStarted());
        assertEquals("no network connectors", 0, brokerService.getNetworkConnectors().size());

        applyNewConfig(brokerConfig, "networkConnectorTest-one-nc", SLEEP);

        assertEquals("no network connectors", 0, brokerService.getNetworkConnectors().size());

        // apply via jmx
        ObjectName objectName =
                new ObjectName(brokerService.getBrokerObjectName().toString() +
                        RuntimeConfigurationBroker.objectNamePropsAppendage);
        RuntimeConfigurationViewMBean runtimeConfigurationView =
                (RuntimeConfigurationViewMBean) brokerService.getManagementContext().newProxyInstance(objectName,
                        RuntimeConfigurationViewMBean.class, false);

        HashMap<String, String> props = new HashMap<String, String>();
        IntrospectionSupport.getProperties(runtimeConfigurationView, props, null);
        LOG.info("mbean attributes before: " + props);

        String result = runtimeConfigurationView.updateNow();

        LOG.info("Result from update: " + result);

        assertTrue("got sensible result: " + result, result.contains("started"));

        assertEquals("one new network connectors", 1, brokerService.getNetworkConnectors().size());

        HashMap<String, String> propsAfter = new HashMap<String, String>();
        IntrospectionSupport.getProperties(runtimeConfigurationView, propsAfter, null);

        LOG.info("mbean attributes after: " + propsAfter);
        String propOfInterest = "modified";
        assertNotEquals("modified is different", props.get(propOfInterest), propsAfter.get(propOfInterest));
    }

    @Test
    public void testUpdateFailedModParseError() throws Exception {
        final String brokerConfig =  "mBeanTest-manual-broker";
        applyNewConfig(brokerConfig, "emptyManualUpdateConfig");
        startBroker(brokerConfig);
        assertTrue("broker alive", brokerService.isStarted());
        assertEquals("no network connectors", 0, brokerService.getNetworkConnectors().size());

        applyNewConfig(brokerConfig, "parseErrorConfig", SLEEP);

        // apply via jmx
        ObjectName objectName =
                new ObjectName(brokerService.getBrokerObjectName().toString() +
                        RuntimeConfigurationBroker.objectNamePropsAppendage);
        RuntimeConfigurationViewMBean runtimeConfigurationView =
                (RuntimeConfigurationViewMBean) brokerService.getManagementContext().newProxyInstance(objectName,
                        RuntimeConfigurationViewMBean.class, false);

        HashMap<String, String> props = new HashMap<String, String>();
        IntrospectionSupport.getProperties(runtimeConfigurationView, props, null);
        LOG.info("mbean attributes before: " + props);

        String result = runtimeConfigurationView.updateNow();
        LOG.info("Result from failed update: " + result);

        assertTrue("got sensible result: " + result, result.contains("dudElement"));

        HashMap<String, String> propsAfter = new HashMap<String, String>();
        IntrospectionSupport.getProperties(runtimeConfigurationView, propsAfter, null);

        LOG.info("mbean attributes after: " + propsAfter);
        String propOfInterest = "modified";
        assertEquals("modified is same", props.get(propOfInterest), propsAfter.get(propOfInterest));

        // apply good change now
        applyNewConfig(brokerConfig, "networkConnectorTest-one-nc", SLEEP);

        result = runtimeConfigurationView.updateNow();

        LOG.info("Result from update: " + result);
        assertTrue("got sensible result: " + result, result.contains("started"));
        assertEquals("one new network connectors", 1, brokerService.getNetworkConnectors().size());

        propsAfter = new HashMap<String, String>();
        IntrospectionSupport.getProperties(runtimeConfigurationView, propsAfter, null);

        assertNotEquals("modified is different", props.get(propOfInterest), propsAfter.get(propOfInterest));

    }
}
