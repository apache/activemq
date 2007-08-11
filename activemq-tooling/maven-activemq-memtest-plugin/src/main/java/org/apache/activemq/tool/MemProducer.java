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
package org.apache.activemq.tool;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.DeliveryMode;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageProducer;
import javax.jms.Session;

/**
 * @version $Revision: 1.3 $
 */
public class MemProducer {
    protected Connection connection;
    protected MessageProducer producer;

    public MemProducer(ConnectionFactory fac, Destination dest) throws JMSException {
        connection = fac.createConnection();
        Session s = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        producer = s.createProducer(dest);
    }

    public void setDeliveryMode(int mode) throws JMSException {
        producer.setDeliveryMode(mode);
    }

    public void start() throws JMSException {
        connection.start();
    }

    public void stop() throws JMSException {
        connection.stop();
    }

    public void shutDown() throws JMSException {
        connection.close();
    }

    public void sendMessage(Message msg) throws JMSException {
        sendMessage(msg, null, 0);
    }

    /*
    *   allow producer to attach message counter on its header. This will be used to verify message order
    *
    */
    public void sendMessage(Message msg, String headerName, long headerValue) throws JMSException {
        if (headerName != null) {
            msg.setLongProperty(headerName, headerValue);
        }

        producer.send(msg);

    }

}
