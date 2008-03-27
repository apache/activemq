/**
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.activemq.camel.component;

import org.apache.activemq.camel.SetHeaderTest;
import org.apache.camel.CamelContext;
import org.apache.camel.CamelTemplate;
import org.apache.camel.component.uface.swing.SwingBrowser;
import org.apache.camel.impl.DefaultCamelContext;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit38.AbstractJUnit38SpringContextTests;

/**
 * @version $Revision: 1.1 $
 */
@ContextConfiguration
public class BrowseQueuesInUFace extends AbstractJUnit38SpringContextTests {
    private static final transient Log LOG = LogFactory.getLog(SetHeaderTest.class);
    @Autowired
    protected CamelContext camelContext;
    @Autowired
    protected CamelTemplate template;
    protected String[] queueNames = {"Sample.A", "Sample.B", "Sample.C"};

    public void testBrowseQueues() throws Exception {
        // lets send a bunch of messages
        for (int i = 0; i < queueNames.length; i++) {
            String queueName = queueNames[i];
            sendMessagesToQueue(queueName, i);
        }

        Thread.sleep(2000);

        // now lets spin up a browser
        SwingBrowser browser = new SwingBrowser((DefaultCamelContext) camelContext);
        browser.run();

        // now lets sleep a while...
        Thread.sleep(50000);
    }

    protected void sendMessagesToQueue(String queueName, int index) {
        String uri = "activemq:" + queueName;
        int count = index * 2 + 2;
        for (int i = 0; i < count; i++) {
            Object body = "Message: " + i;
            template.sendBodyAndHeader(uri, body, "counter", i);
        }
        System.out.println("Sent " + count + " messages to: " + uri);
    }
}