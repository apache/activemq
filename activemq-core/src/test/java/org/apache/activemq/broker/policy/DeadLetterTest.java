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

import javax.jms.Destination;
import javax.jms.Message;

import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.RedeliveryPolicy;
import org.apache.activemq.command.ActiveMQQueue;

/**
 * 
 * @version $Revision$
 */
public class DeadLetterTest extends DeadLetterTestSupport {

    private int rollbackCount;

    protected void doTest() throws Exception {
        connection.start();

        ActiveMQConnection amqConnection = (ActiveMQConnection) connection;
        rollbackCount = amqConnection.getRedeliveryPolicy().getMaximumRedeliveries() + 1;
        log.info("Will redeliver messages: " + rollbackCount + " times");

        makeConsumer();
        makeDlqConsumer();

        sendMessages();

        // now lets receive and rollback N times
        for (int i = 0; i < messageCount; i++) {
            consumeAndRollback(i);
        }

        for (int i = 0; i < messageCount; i++) {
            Message msg = dlqConsumer.receive(1000);
            assertMessage(msg, i);
            assertNotNull("Should be a DLQ message for loop: " + i, msg);
        }
    }

    protected void consumeAndRollback(int messageCounter) throws Exception {
        for (int i = 0; i < rollbackCount; i++) {
            Message message = consumer.receive(5000);
            assertNotNull("No message received for message: " + messageCounter + " and rollback loop: " + i, message);
            assertMessage(message, messageCounter);

            session.rollback();
        }
        log.info("Rolled back: " + rollbackCount + " times");
    }

    protected void setUp() throws Exception {
        transactedMode = true;
        super.setUp();
    }

    protected ActiveMQConnectionFactory createConnectionFactory() throws Exception {
        ActiveMQConnectionFactory answer = super.createConnectionFactory();
        RedeliveryPolicy policy = new RedeliveryPolicy();
        policy.setMaximumRedeliveries(3);
        policy.setBackOffMultiplier((short) 1);
        policy.setInitialRedeliveryDelay(10);
        policy.setUseExponentialBackOff(false);
        answer.setRedeliveryPolicy(policy);
        return answer;
    }

    protected Destination createDlqDestination() {
        return new ActiveMQQueue("ActiveMQ.DLQ");
    }

}
