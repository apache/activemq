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

import java.text.DateFormat;
import java.util.Date;
import java.util.HashMap;
import javax.management.ObjectName;
import org.apache.activemq.plugin.RuntimeConfigurationBroker;
import org.apache.activemq.plugin.jmx.RuntimeConfigurationViewMBean;
import org.apache.activemq.util.IntrospectionSupport;
import org.junit.Test;


import static org.junit.Assert.*;

public class SpringBeanTest extends RuntimeConfigTestSupport {

    @Test
    public void testUpdateNow() throws Exception {
        final String brokerConfig =  "SpromgBeanTest-broker";
        applyNewConfig(brokerConfig, "emptyUpdatableConfig1000-spring-bean");
        startBroker(brokerConfig);
        assertTrue("broker alive", brokerService.isStarted());

        // apply via jmx
        ObjectName objectName =
                new ObjectName(brokerService.getBrokerObjectName().toString() +
                        RuntimeConfigurationBroker.objectNamePropsAppendage);
        RuntimeConfigurationViewMBean runtimeConfigurationView =
                (RuntimeConfigurationViewMBean) brokerService.getManagementContext().newProxyInstance(objectName,
                        RuntimeConfigurationViewMBean.class, false);

        String propOfInterest = "modified";
        HashMap<String, String> props = new HashMap<String, String>();
        IntrospectionSupport.getProperties(runtimeConfigurationView, props, null);
        LOG.info("mbean attributes before: " + props);

        assertNotEquals("unknown", props.get(propOfInterest));

        String result = runtimeConfigurationView.updateNow();

        LOG.info("Result from update: " + result);

        assertTrue("got sensible result", result.contains("No material change"));

        HashMap<String, String> propsAfter = new HashMap<String, String>();
        IntrospectionSupport.getProperties(runtimeConfigurationView, propsAfter, null);
        LOG.info("mbean attributes after: " + propsAfter);

        assertEquals("modified is same", props.get(propOfInterest), propsAfter.get(propOfInterest));
    }

}
