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
package org.apache.activemq.usecases;

import javax.jms.JMSException;
import javax.jms.Message;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConsumeQueuePrefetchTest extends ConsumeTopicPrefetchTest {
    private static final Logger LOG = LoggerFactory.getLogger(ConsumeQueuePrefetchTest.class);
    
    protected void setUp() throws Exception {
        topic = false;
        super.setUp();
    }
    
    public void testInflightWithConsumerPerMessage() throws JMSException {
        makeMessages(prefetchSize);

        LOG.info("About to send and receive: " + prefetchSize + " on destination: " + destination
                + " of type: " + destination.getClass().getName());

        for (int i = 0; i < prefetchSize; i++) {
            Message message = session.createTextMessage(messageTexts[i]);
            producer.send(message);
        }

        validateConsumerPrefetch(this.getSubject(), prefetchSize);
        
        // new consumer per 20 messages
        for (int i = 0; i < prefetchSize; i+=20) {
            consumer.close();
            consumer = session.createConsumer(destination);
            validateConsumerPrefetch(this.getSubject(), prefetchSize - i);
            for (int j=0; j<20; j++) {
                Message message = consumeMessge(i+j);
                message.acknowledge();
            }
        }
    }
}
