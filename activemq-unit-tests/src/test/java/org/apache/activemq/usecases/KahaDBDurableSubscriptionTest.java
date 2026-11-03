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

import static org.junit.Assert.assertThrows;

import jakarta.jms.DeliveryMode;
import jakarta.jms.JMSException;
import jakarta.jms.Topic;
import java.io.IOException;
import org.apache.activemq.store.PersistenceAdapter;
import org.junit.experimental.categories.Category;
import org.apache.activemq.test.annotations.ParallelTest;

@Category(ParallelTest.class)
public class KahaDBDurableSubscriptionTest extends DurableSubscriptionTestSupport {

    protected PersistenceAdapter createPersistenceAdapter() throws IOException {
        return null; // use default
    }

    public void testDurableSubscriptionKeyValidation() throws Exception {
        connection.start();
        session = connection.createSession(false, jakarta.jms.Session.AUTO_ACKNOWLEDGE);

        Topic topic = session.createTopic("TestTopic");
        // This should throw an exception back to the client because the sub key
        // value is too large and validation should catch it
        assertThrows(JMSException.class, () -> session.createDurableSubscriber(topic, "s".repeat(65535)));

        // Ensure we can restart the broker without error
        restartBroker();
    }
}
