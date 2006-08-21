/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package blah;

import java.sql.Timestamp;
import java.util.Date;
import java.util.Properties;

import javax.jms.*;
import javax.naming.*;

public class ActiveMQQueueReceiver {
    public static void main(String[] args) {
        Queue queue = null;
        QueueConnectionFactory queueConnectionFactory = null;
        QueueConnection queueConnection = null;

        try {
            Properties props = new Properties();
            //props.setProperty(Context.INITIAL_CONTEXT_FACTORY, "com.evermind.server.rmi.RMIInitialContextFactory");
            //props.setProperty(Context.PROVIDER_URL, "ormi://10.1.0.99:3202/default");
            //props.setProperty(Context.SECURITY_PRINCIPAL, "dan");
            //props.setProperty(Context.SECURITY_CREDENTIALS, "abc123");

            props.setProperty(Context.INITIAL_CONTEXT_FACTORY, "org.activemq.jndi.ActiveMQInitialContextFactory");
            props.setProperty(Context.PROVIDER_URL, "tcp://hostname:61616");
            props.setProperty("queue.BlahQueue", "example.BlahQueue");

            // use the following if you with to make the receiver a broker
            //props.setProperty("useEmbeddedBroker", "true");

            Context jndiContext = new InitialContext(props);

            //queueConnectionFactory = (QueueConnectionFactory) jndiContext.lookup("jms/QueueConnectionFactory");
            //queue = (Queue) jndiContext.lookup("jms/demoQueue");
            queueConnectionFactory = (QueueConnectionFactory) jndiContext.lookup("QueueConnectionFactory");
            queue = (Queue) jndiContext.lookup("BlahQueue");
        }
        catch (NamingException e) {
            System.out.println("---------------------------ERROR-----------------------------");
            e.printStackTrace();
            System.exit(-1);
        }

        try {
            queueConnection = queueConnectionFactory.createQueueConnection();
            QueueSession queueSession = queueConnection.createQueueSession(false, Session.AUTO_ACKNOWLEDGE);
            QueueReceiver queueReceiver = queueSession.createReceiver(queue);
            queueConnection.start();

            //while (true)
            //{
            System.out.println("Starting to receive");

            TextMessage message = (TextMessage) queueReceiver.receive(10000);

            if (message != null) {
                Date timestamp = new Date(message.getJMSTimestamp());
                System.out.println("Blah:       " + message.getStringProperty("Blah"));
                System.out.println("Timestamp:  " + timestamp);
                System.out.println("Payload:    " + message.getText());
            }
            else {
                System.out.println("NO MESSAGES");
            }
            System.out.println();

            //Thread.sleep(10000);
            //}
        }
        catch (Exception e) {
            System.out.println("SOMETHING WENT WRONG WHILE CONSUMING");
            e.printStackTrace();
        }
        finally {
            if (queueConnection != null) {
                try {
                    queueConnection.close();
                }
                catch (Exception ignored) {
                }
            }
        }
    }
}
