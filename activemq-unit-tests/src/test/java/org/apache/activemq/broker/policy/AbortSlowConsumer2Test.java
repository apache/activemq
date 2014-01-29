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
package org.apache.activemq.broker.policy;

import org.apache.activemq.util.MessageIdList;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jms.Connection;
import javax.jms.MessageConsumer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map.Entry;

@RunWith(value = Parameterized.class)
public class AbortSlowConsumer2Test extends AbortSlowConsumerBase {

    private static final Logger LOG = LoggerFactory.getLogger(AbortSlowConsumer2Test.class);

    @Parameterized.Parameters(name = "{0}")
    public static Collection<Object[]> getTestParameters() {

        List<Object[]> testParameters = new ArrayList<Object[]>();
        Boolean[] booleanValues = {Boolean.TRUE, Boolean.FALSE};
        for (Boolean topic : booleanValues) {
            Boolean[] value = {topic};
            testParameters.add(value);
        }

        return testParameters;
    }

    public AbortSlowConsumer2Test(Boolean isTopic) {
        this.topic = isTopic;
    }


    @Test(timeout = 60 * 1000)
    public void testLittleSlowConsumerIsNotAborted() throws Exception {
        startConsumers(destination);
        Entry<MessageConsumer, MessageIdList> consumertoAbort = consumers.entrySet().iterator().next();
        consumertoAbort.getValue().setProcessingDelay(500);
        for (Connection c : connections) {
            c.setExceptionListener(this);
        }
        startProducers(destination, 12);
        allMessagesList.waitForMessagesToArrive(10);
        allMessagesList.assertAtLeastMessagesReceived(10);
    }
}
