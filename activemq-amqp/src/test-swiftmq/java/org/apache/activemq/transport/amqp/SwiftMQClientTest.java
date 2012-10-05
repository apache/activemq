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

import com.swiftmq.amqp.AMQPContext;
import com.swiftmq.amqp.v100.client.*;
import com.swiftmq.amqp.v100.generated.messaging.message_format.AmqpValue;
import com.swiftmq.amqp.v100.messaging.AMQPMessage;
import com.swiftmq.amqp.v100.types.AMQPString;
import com.swiftmq.amqp.v100.types.AMQPType;
import org.junit.Test;
import static org.junit.Assert.*;

/**
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
public class SwiftMQClientTest extends AmqpTestSupport {

    @Test
    public void testSendReceive() throws Exception {

        String queue = "testqueue";
        int nMsgs = 100;
        final String dataFormat = "%01024d";

        int qos = QoS.AT_MOST_ONCE;
        AMQPContext ctx = new AMQPContext(AMQPContext.CLIENT);

        try {

            Connection connection = new Connection(ctx, "127.0.0.1", port, false);
            connection.setContainerId("client");
            connection.setIdleTimeout(-1);
            connection.setMaxFrameSize(1024 * 4);
            connection.setExceptionListener(new ExceptionListener() {
                public void onException(Exception e) {
                    e.printStackTrace();
                }
            });
            connection.connect();
            {

                Session session = connection.createSession(10, 10);
                Producer p = session.createProducer(queue, qos);
                for (int i = 0; i < nMsgs; i++) {
                    AMQPMessage msg = new AMQPMessage();
                    System.out.println("Sending " + i);
                    msg.setAmqpValue(new AmqpValue(new AMQPString(String.format(dataFormat, i))));
                    p.send(msg);
                }
                p.close();
                session.close();
            }
            System.out.println("=======================================================================================");
            System.out.println(" receiving ");
            System.out.println("=======================================================================================");
            {
                Session session = connection.createSession(10, 10);
                Consumer c = session.createConsumer(queue, 100, qos, true, null);

                // Receive messages non-transacted
                int i = 0;
                while ( i < nMsgs) {
                    AMQPMessage msg = c.receive();
                    if( msg!=null ) {
                        final AMQPType value = msg.getAmqpValue().getValue();
                        if (value instanceof AMQPString) {
                            String s = ((AMQPString) value).getValue();
                            assertEquals(String.format(dataFormat, i), s);
                            System.out.println("Received: " + i);
                        }
                        if (!msg.isSettled())
                            msg.accept();
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
