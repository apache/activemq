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
package org.apache.activemq.spring;

import org.apache.activemq.util.Wait;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.test.annotation.Commit;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.transaction.annotation.Transactional;

import jakarta.annotation.Resource;
import jakarta.jms.*;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations = {"classpath:spring/spring.xml"})
@Transactional
@Commit
public class ListenerTest {
    private static final Logger LOG = LoggerFactory.getLogger(ListenerTest.class);

    private static final int MSG_NUM = 10;

    @Resource
    private Listener listener;

    @Resource
    private ConnectionFactory connectionFactory;

    @Test
    @DirtiesContext
    public void testSimple() throws Exception {
        sendMessages("SIMPLE", MSG_NUM);

        Assert.assertTrue("Expected " + MSG_NUM + " messages but got " + listener.messages.size(),
            Wait.waitFor(() -> MSG_NUM == listener.messages.size(), 60_000));

        LOG.info("messages received= " + listener.messages.size());
    }


    @Test
    @DirtiesContext
    public void testComposite() throws Exception {
        final int expectedMessages = 6 * MSG_NUM;
        sendMessages("TEST.1,TEST.2,TEST.3,TEST.4,TEST.5,TEST.6", MSG_NUM);

        Assert.assertTrue("Expected " + expectedMessages + " messages but got " + listener.messages.size(),
            Wait.waitFor(() -> expectedMessages == listener.messages.size(), 120_000));

        LOG.info("messages received= " + listener.messages.size());
    }

    private void sendMessages(String destName, int messageCount) throws Exception {
        try (Connection conn = connectionFactory.createConnection()) {
            conn.start();
            final Session sess = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
            final Destination dest = sess.createQueue(destName);
            final MessageProducer producer = sess.createProducer(dest);
            for (int i = 0; i < messageCount; i++) {
                final String messageText = i + " test";
                LOG.info("sending message '{}' to {}", messageText, destName);
                producer.send(sess.createTextMessage(messageText));
            }
            LOG.info("Sent {} messages to {}", messageCount, destName);
        }
    }


}
