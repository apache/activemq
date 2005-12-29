/**
 *
 * Copyright 2005-2006 The Apache Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.activemq.usecases;
import java.util.HashMap;
import javax.jms.Connection;
import javax.jms.Destination;
import javax.jms.IllegalStateException;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.MessageProducer;
import javax.jms.ObjectMessage;
import javax.jms.Session;

import org.apache.activemq.test.TestSupport;

/**
 * @version $Revision: 1.1.1.1 $
 */
public class ChangeSessionDeliveryModeTest extends TestSupport implements MessageListener {
    private static final int COUNT = 200;
    private static final String VALUE_NAME = "value";

    /**
     * test following condition- which are defined by JMS Spec 1.1: MessageConsumers cannot use a MessageListener and
     * receive() from the same session
     * 
     * @throws Exception
     */
    public void testDoChangeSessionDeliveryMode() throws Exception {
        Destination destination = createDestination("foo.bar");
        Connection connection = createConnection();
        connection.start();
        Session consumerSession = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        MessageConsumer consumer1 = consumerSession.createConsumer(destination);
        consumer1.setMessageListener(this);
        JMSException jmsEx = null;
        MessageConsumer consumer2 = consumerSession.createConsumer(destination);
        try {
            consumer2.receive(10);
        }
        catch (JMSException e) {
            jmsEx = e;
        }
        assertTrue(jmsEx != null && jmsEx instanceof IllegalStateException);
    }

    public void onMessage(Message msg) {
    }
}