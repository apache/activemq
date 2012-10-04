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
package org.apache.activemq.transport.amqp;

import org.apache.qpid.amqp_1_0.jms.impl.ConnectionFactoryImpl;
import org.apache.qpid.amqp_1_0.jms.impl.QueueImpl;
import org.junit.Test;

import javax.jms.*;

import static org.junit.Assert.assertEquals;

/**
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
public class JMSClientTest extends AmqpTestSupport {

    @Test
    public void testSendReceive() throws Exception {

        QueueImpl queue = new QueueImpl("queue://testqueue");
        int nMsgs = 100;
        final String dataFormat = "%01024d";

        final ConnectionFactoryImpl factory = new ConnectionFactoryImpl("localhost", port, null, null);

        try {
            final Connection connection = factory.createConnection();
            connection.setExceptionListener(new ExceptionListener() {
                @Override
                public void onException(JMSException exception) {
                    exception.printStackTrace();
                }
            });
            connection.start();
            {
                Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
                MessageProducer p = session.createProducer(queue);
                for (int i = 0; i < nMsgs; i++) {
                    System.out.println("Sending " + i);
                    p.send(session.createTextMessage(String.format(dataFormat, i)));
                }
                p.close();
                session.close();
            }
            System.out.println("=======================================================================================");
            System.out.println(" receiving ");
            System.out.println("=======================================================================================");
            {
                Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
                MessageConsumer c = session.createConsumer(queue);

                // Receive messages non-transacted
                int i = 0;
                while ( i < nMsgs) {
                    TextMessage msg = (TextMessage) c.receive();
                    if( msg!=null ) {
                        String s = msg.getText();
                        assertEquals(String.format(dataFormat, i), s);
                        System.out.println("Received: " + i);
                        i++;
                    }
                }
                c.close();
                session.close();
            }
            connection.close();
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

}
