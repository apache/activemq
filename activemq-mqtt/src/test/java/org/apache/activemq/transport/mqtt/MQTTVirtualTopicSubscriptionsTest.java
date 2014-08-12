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
package org.apache.activemq.transport.mqtt;

import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

/**
 * Run the basic tests with the NIO Transport.
 */
public class MQTTVirtualTopicSubscriptionsTest extends MQTTTest {

    @Override
    @Before
    public void setUp() throws Exception {
        protocolConfig = "transport.subscriptionStrategy=mqtt-virtual-topic-subscriptions";
        super.setUp();
    }

    // TODO - This currently fails on the durable case because we have a hard time
    //        recovering the original Topic name when a client tries to subscribe
    //        durable to a VirtualTopic.* type topic.
    @Override
    @Ignore
    public void testRetainedMessageOnVirtualTopics() throws Exception {}

    @Override
    @Test(timeout = 60 * 1000)
    public void testSendMQTTReceiveJMS() throws Exception {
        doTestSendMQTTReceiveJMS("VirtualTopic.foo.*");
    }

    @Override
    @Test(timeout = 2 * 60 * 1000)
    public void testSendJMSReceiveMQTT() throws Exception {
        doTestSendJMSReceiveMQTT("VirtualTopic.foo.far");
    }

    @Override
    @Test(timeout = 30 * 10000)
    public void testJmsMapping() throws Exception {
        doTestJmsMapping("VirtualTopic.test.foo");
    }
}
