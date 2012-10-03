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

/**
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
public class SwiftMQClientTest extends AmqpTestSupport {

    @Test
    public void testSendReceive() throws Exception {

        String queue = "testqueue";
        int nMsgs = 1;
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
                String data = String.format("%010d", 0);

                Session session = connection.createSession(10, 10);
                Producer p = session.createProducer(queue, qos);
                for (int i = 0; i < nMsgs; i++) {
                    AMQPMessage msg = new AMQPMessage();
                    String s = "Message #" + (i + 1);
                    System.out.println("Sending " + s);
                    msg.setAmqpValue(new AmqpValue(new AMQPString(s + ", data: " + data)));
                    p.send(msg);
                }
                p.close();
                session.close();
            }

//            {
//                Session session = connection.createSession(10, 10);
//                Consumer c = session.createConsumer(queue, 100, qos, true, null);
//
//                // Receive messages non-transacted
//                for (int i = 0; i < nMsgs; i++) {
//                    AMQPMessage msg = c.receive();
//                    final AMQPType value = msg.getAmqpValue().getValue();
//                    if (value instanceof AMQPString) {
//                        AMQPString s = (AMQPString) value;
//                        System.out.println("Received: " + s.getValue());
//                    }
//                    if (!msg.isSettled())
//                        msg.accept();
//                }
//                c.close();
//                session.close();
//            }
            connection.close();
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

}
