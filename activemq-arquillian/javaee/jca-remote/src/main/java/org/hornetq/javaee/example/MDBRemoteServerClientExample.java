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

package org.hornetq.javaee.example;



import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.naming.Context;
import javax.naming.InitialContext;
import java.util.Properties;

/**
 *
 * MDB Remote & JCA Configuration Example.
 *
 * @author <a href="mailto:clebert.suconic@jboss.org">Clebert Suconic</a>
 */
public class MDBRemoteServerClientExample
{
   public static void main(String[] args) throws Exception
   {
      InitialContext initialContext = null;
      Connection connection = null;
      try
      {
         //Step 1. Create an initial context to perform the JNDI lookup.
         final Properties env = new Properties();

         env.put(Context.URL_PKG_PREFIXES, "org.jboss.ejb.client.naming");

         env.put(Context.INITIAL_CONTEXT_FACTORY, "org.jboss.naming.remote.client.InitialContextFactory");

         env.put(Context.PROVIDER_URL, "remote://localhost:4547");

         env.put(Context.SECURITY_PRINCIPAL, "guest");

         env.put(Context.SECURITY_CREDENTIALS, "password");

         initialContext = new InitialContext(env);

         // Step 2. Look up the MDB's queue
         Queue queue = (Queue) initialContext.lookup("/queues/mdbQueue");

         // Step 3. Look up a Connection Factory
         ConnectionFactory cf = (ConnectionFactory) initialContext.lookup("jms/RemoteConnectionFactory");

         //Step 4. Create a connection
         connection = cf.createConnection("guest", "password");

         System.out.println("new connection: " + connection);

         //Step 5. Create a Session
         Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

         // Step 6. Create a message producer to send the message
         MessageProducer producer = session.createProducer(queue);

         // Step 7. Create and send a message
         producer.send(session.createTextMessage("a message"));

         // Step 15. Look up the reply queue
         Queue replyQueue = (Queue) initialContext.lookup("/queues/mdbReplyQueue");

         // Step 16. Create a message consumer to receive the message
         MessageConsumer consumer = session.createConsumer(replyQueue);

         // Step 17. Start the connection so delivery starts
         connection.start();

         // Step 18. Receive the text message
         TextMessage textMessage = (TextMessage) consumer.receive(5000);

         System.out.println("Message received from reply queue. Message = \"" + textMessage.getText() + "\"" );

      }
      finally
      {
         // Step 19. Be sure to close our JMS resources!
         if (initialContext != null)
         {
            initialContext.close();
         }
         if (connection != null)
         {
            connection.close();
         }
      }
   }
}