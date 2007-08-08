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
package org.apache.activemq.jndi;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.CombinationTestSupport;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.ActiveMQQueue;

import javax.naming.Reference;

public class ObjectFactoryTest extends CombinationTestSupport {
    public void testConnectionFactory() throws Exception {
        // Create sample connection factory
        ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory();
        factory.setDispatchAsync(true);
        factory.setBrokerURL("vm://test");
        factory.setClientID("test");
        factory.setCopyMessageOnSend(false);
        factory.setDisableTimeStampsByDefault(true);
        factory.setObjectMessageSerializationDefered(true);
        factory.setOptimizedMessageDispatch(false);
        factory.setPassword("pass");
        factory.setUseAsyncSend(true);
        factory.setUseCompression(true);
        factory.setUseRetroactiveConsumer(true);
        factory.setUserName("user");
        factory.getPrefetchPolicy().setQueuePrefetch(777);
        factory.getRedeliveryPolicy().setMaximumRedeliveries(15);
        factory.getRedeliveryPolicy().setBackOffMultiplier((short) 32);
        

        // Create reference
        Reference ref = JNDIReferenceFactory.createReference(factory.getClass().getName(), factory);

        // Get object created based on reference
        ActiveMQConnectionFactory temp;
        JNDIReferenceFactory refFactory = new JNDIReferenceFactory();
        temp = (ActiveMQConnectionFactory)refFactory.getObjectInstance(ref, null, null, null);

        // Check settings
        assertEquals(factory.isDispatchAsync(), temp.isDispatchAsync());
        assertEquals(factory.getBrokerURL(), temp.getBrokerURL());
        assertEquals(factory.getClientID(), temp.getClientID());
        assertEquals(factory.isCopyMessageOnSend(), temp.isCopyMessageOnSend());
        assertEquals(factory.isDisableTimeStampsByDefault(), temp.isDisableTimeStampsByDefault());
        assertEquals(factory.isObjectMessageSerializationDefered(), temp.isObjectMessageSerializationDefered());
        assertEquals(factory.isOptimizedMessageDispatch(), temp.isOptimizedMessageDispatch());
        assertEquals(factory.getPassword(), temp.getPassword());
        assertEquals(factory.isUseAsyncSend(), temp.isUseAsyncSend());
        assertEquals(factory.isUseCompression(), temp.isUseCompression());
        assertEquals(factory.isUseRetroactiveConsumer(), temp.isUseRetroactiveConsumer());
        assertEquals(factory.getUserName(), temp.getUserName());
        assertEquals(factory.getPrefetchPolicy().getQueuePrefetch(), temp.getPrefetchPolicy().getQueuePrefetch());
        assertEquals(factory.getRedeliveryPolicy().getMaximumRedeliveries(), temp.getRedeliveryPolicy().getMaximumRedeliveries());
        assertEquals(factory.getRedeliveryPolicy().getBackOffMultiplier(), temp.getRedeliveryPolicy().getBackOffMultiplier());
    }

    public void testDestination() throws Exception {
        // Create sample destination
        ActiveMQDestination dest = new ActiveMQQueue();
        dest.setPhysicalName("TEST.FOO");

        // Create reference
        Reference ref = JNDIReferenceFactory.createReference(dest.getClass().getName(), dest);

        // Get object created based on reference
        ActiveMQDestination temp;
        JNDIReferenceFactory refFactory = new JNDIReferenceFactory();
        temp = (ActiveMQDestination)refFactory.getObjectInstance(ref, null, null, null);

        // Check settings
        assertEquals(dest.getPhysicalName(), temp.getPhysicalName());
    }
}
