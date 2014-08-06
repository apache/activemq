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
package org.apache.activemq.broker.scheduler;

import java.io.File;

import javax.jms.Connection;
import javax.jms.Message;
import javax.jms.Session;

import org.apache.activemq.EmbeddedBrokerTestSupport;
import org.apache.activemq.ScheduledMessage;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.util.IOHelper;
import org.apache.activemq.util.ProducerThread;

public class JobSchedulerBrokerShutdownTest extends EmbeddedBrokerTestSupport {

    @Override
    protected BrokerService createBroker() throws Exception {
        File schedulerDirectory = new File("target/scheduler");

        IOHelper.mkdirs(schedulerDirectory);
        IOHelper.deleteChildren(schedulerDirectory);

        BrokerService broker = super.createBroker();
        broker.setSchedulerSupport(true);
        broker.setDataDirectory("target");
        broker.setSchedulerDirectoryFile(schedulerDirectory);
        broker.getSystemUsage().getStoreUsage().setLimit(1 * 512);
        broker.deleteAllMessages();
        return broker;
    }

    @Override
    protected boolean isPersistent() {
        return true;
    }

    public void testSchedule() throws Exception {

        Connection connection = createConnection();
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

        connection.start();
        final long time = 1000;

        ProducerThread producer = new ProducerThread(session, destination) {
            @Override
            protected Message createMessage(int i) throws Exception {
                Message message = super.createMessage(i);
                message.setLongProperty(ScheduledMessage.AMQ_SCHEDULED_DELAY, time);
                return message;
            }
        };

        producer.setMessageCount(200);
        producer.setDaemon(true);

        producer.start();

        Thread.sleep(5000);
    }
}
