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

package org.apache.activemq.bugs;

import java.util.concurrent.atomic.AtomicBoolean;
import javax.jms.Connection;
import javax.jms.DeliveryMode;
import javax.jms.MessageProducer;
import javax.jms.Session;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.AutoFailTestSupport;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.util.LoggingBrokerPlugin;
import org.apache.activemq.util.DefaultTestAppender;
import org.apache.log4j.Appender;
import org.apache.log4j.Logger;
import org.apache.log4j.spi.LoggingEvent;

public class AMQ3779Test extends AutoFailTestSupport {

    private static final Logger logger = Logger.getLogger(AMQ3779Test.class);
    private static final String qName = "QNameToFind";

    public void testLogPerDest() throws Exception {

        final AtomicBoolean ok = new AtomicBoolean(false);
        Appender appender = new DefaultTestAppender() {
            @Override
            public void doAppend(LoggingEvent event) {
                if (event.getLoggerName().toString().contains(qName)) {
                    ok.set(true);
                }
            }
        };
        logger.getRootLogger().addAppender(appender);

        try {

            BrokerService broker = new BrokerService();
            LoggingBrokerPlugin loggingBrokerPlugin = new LoggingBrokerPlugin();
            loggingBrokerPlugin.setPerDestinationLogger(true);
            loggingBrokerPlugin.setLogAll(true);
            broker.setPlugins(new LoggingBrokerPlugin[]{loggingBrokerPlugin});
            broker.start();


            Connection connection = new ActiveMQConnectionFactory(broker.getVmConnectorURI()).createConnection();
            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            MessageProducer messageProducer = session.createProducer(session.createQueue(qName));
            messageProducer.setDeliveryMode(DeliveryMode.PERSISTENT);
            connection.start();

            messageProducer.send(session.createTextMessage("Hi"));
            connection.close();

            assertTrue("got expected log message", ok.get());
        } finally {
            logger.removeAppender(appender);
        }
    }
}
