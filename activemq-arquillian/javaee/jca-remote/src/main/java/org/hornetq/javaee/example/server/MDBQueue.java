/*
 * Copyright 2009 Red Hat, Inc.
 *  Red Hat licenses this file to you under the Apache License, version
 *  2.0 (the "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *     http://www.apache.org/licenses/LICENSE-2.0
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 *  implied.  See the License for the specific language governing
 *  permissions and limitations under the License.
 */
package org.hornetq.javaee.example.server;

import org.jboss.ejb3.annotation.ResourceAdapter;

import javax.annotation.Resource;
import javax.ejb.ActivationConfigProperty;
import javax.ejb.MessageDriven;
import javax.jms.*;

/**
 * @author <a href="mailto:andy.taylor@jboss.org">Andy Taylor</a>
 *         Created May 24, 2010
 */

/**
 * MDB that is connected to the remote queue.
 * @author <a href="mailto:clebert.suconic@jboss.org">Clebert Suconic</a>
 */

//Step 10. The message is received on the MDB, using a remote queue.
@MessageDriven(name = "MDB_Queue",
               activationConfig =
                     {
                        @ActivationConfigProperty(propertyName = "destinationType", propertyValue = "javax.jms.Queue"),
                        @ActivationConfigProperty(propertyName = "destination", propertyValue = "queues/mdbQueue"),
                        @ActivationConfigProperty(propertyName = "acknowledgeMode", propertyValue = "Auto-acknowledge"),
                        // amq defaults to not look in jndi
                        @ActivationConfigProperty(propertyName = "useJndi", propertyValue = "true")
                     })
@ResourceAdapter("activemq-remote.rar")
public class MDBQueue implements MessageListener
{
   /**
    *  Resource to be deployed by jms-remote-ds.xml
    *  */
   @Resource(mappedName="java:/RemoteJmsXA")
   private ConnectionFactory connectionFactory;

   public void onMessage(Message message)
   {
      try
      {
         // Step 8. Receive the text message
         TextMessage tm = (TextMessage)message;

         String text = tm.getText();

         System.out.println("Step 11: (MDBQueue.java) Message received using the remote adapter. Message = \"" + text + "\"" );

         // Step 9. look up the reply queue
         //Queue destQueue = HornetQJMSClient.createQueue("mdbReplyQueue");

         // Step 10. Create a connection
         Connection connection = connectionFactory.createConnection();

         // Step 11. Create a session
         Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

         Queue destQueue = session.createQueue("mdbReplyQueue");

         // Step 12. Create a message producer to send the message
         MessageProducer producer = session.createProducer(destQueue);

         System.out.println("sending a reply message");

         // Step 13. Create and send a reply text message
         producer.send(session.createTextMessage("A reply message"));

         // Step 14. Return the connection back to the pool
         connection.close();

      }
      catch (Exception e)
      {
         e.printStackTrace();
      }
   }
}
