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
package org.objectweb.jtests.jms.conform.connection;

import jakarta.jms.JMSException;
import jakarta.jms.Message;
import jakarta.jms.MessageListener;
import jakarta.jms.Session;
import jakarta.jms.TextMessage;

import org.junit.Assert;
import junit.framework.Test;
import junit.framework.TestSuite;

import org.objectweb.jtests.jms.framework.PTPTestCase;
import org.objectweb.jtests.jms.framework.TestConfig;

/**
 * Test connections.
 * 
 * See JMS specifications, sec. 4.3.5 Closing a Connection
 *
 * @author Jeff Mesnil (jmesnil@gmail.com)
 * @version $Id: ConnectionTest.java,v 1.2 2007/06/15 20:55:20 csuconic Exp $
 */
public class ConnectionTest extends PTPTestCase
{

   /**
    * Test that invoking the <code>acknowledge()</code> method of a received message 
    * from a closed connection's session must throw an <code>IllegalStateException</code>.
    */
   public void testAcknowledge()
   {
      try
      {
         receiverConnection.stop();
         receiverSession = receiverConnection.createQueueSession(false, Session.CLIENT_ACKNOWLEDGE);
         receiver.close(); // Before assigning a new receiver, we need to close the old one...
         // Not closing it could cause load balancing between receivers.
         // Not having this close might be valid for JORAM or JBossMQ, but it's not valid for HornetQ (and still legal)

         receiver = receiverSession.createReceiver(receiverQueue);
         receiverConnection.start();

         Message message = senderSession.createMessage();
         sender.send(message);

         Message m = receiver.receive(TestConfig.TIMEOUT);
         receiverConnection.close();
         m.acknowledge();
         Assert.fail("sec. 4.3.5 Invoking the acknowledge method of a received message from a closed " + "connection's session must throw a [jakarta.jms.]IllegalStateException.\n");
      }
      catch (jakarta.jms.IllegalStateException e)
      {
      }
      catch (JMSException e)
      {
         Assert.fail("sec. 4.3.5 Invoking the acknowledge method of a received message from a closed " + "connection's session must throw a [jakarta.jms.]IllegalStateException, not a " +
                     e);
      }
      catch (java.lang.IllegalStateException e)
      {
         Assert.fail("sec. 4.3.5 Invoking the acknowledge method of a received message from a closed " + "connection's session must throw an [jakarta.jms.]IllegalStateException "
                     + "[not a java.lang.IllegalStateException]");
      }
   }

   /**
    * Test that an attempt to use a <code>Connection</code> which has been closed 
    * throws a <code>jakarta.jms.IllegalStateException</code>.
    */
   public void testUseClosedConnection()
   {
      try
      {
         senderConnection.close();
         senderConnection.createQueueSession(false, Session.AUTO_ACKNOWLEDGE);
         Assert.fail("Should raise a jakarta.jms.IllegalStateException");
      }
      catch (jakarta.jms.IllegalStateException e)
      {
      }
      catch (JMSException e)
      {
         Assert.fail("Should raise a jakarta.jms.IllegalStateException, not a " + e);
      }
      catch (java.lang.IllegalStateException e)
      {
         Assert.fail("Should raise a jakarta.jms.IllegalStateException, not a java.lang.IllegalStateException");
      }
   }

   /**
    * Test that a <code>MessageProducer</code> can send messages while a 
    * <code>Connection</code> is stopped.
    */
   public void testMessageSentWhenConnectionClosed()
   {
      try
      {
         senderConnection.stop();
         Message message = senderSession.createTextMessage();
         sender.send(message);

         receiver.receive(TestConfig.TIMEOUT);
      }
      catch (JMSException e)
      {
         fail(e);
      }
   }

   /**
    * Test that closing a closed connectiondoes not throw an exception.
    */
   public void testCloseClosedConnection()
   {
      try
      {
         // senderConnection is already started
         // we close it once
         senderConnection.close();
         // we close it a second time
         senderConnection.close();
      }
      catch (Exception e)
      {
         Assert.fail("sec. 4.3.5 Closing a closed connection must not throw an exception.\n");
      }
   }

   /**
    * Test that starting a started connection is ignored
    */
   public void testStartStartedConnection()
   {
      try
      {
         // senderConnection is already started
         // start it again should be ignored
         senderConnection.start();
      }
      catch (JMSException e)
      {
         fail(e);
      }
   }

   /**
    * Test that stopping a stopped connection is ignored
    */
   public void testStopStoppedConnection()
   {
      try
      {
         // senderConnection is started
         // we stop it once
         senderConnection.stop();
         // stopping it a second time is ignored
         senderConnection.stop();
      }
      catch (JMSException e)
      {
         fail(e);
      }
   }

   /**
    * Test that delivery of message is stopped if the message consumer connection is stopped
    */
   public void testStopConsumerConnection()
   {
      try
      {
         receiverConnection.stop();

         receiver.setMessageListener(new MessageListener()
         {
            public void onMessage(final Message m)
            {
               try
               {
                  Assert.fail("The message must not be received, the consumer connection is stopped");
                  Assert.assertEquals("test", ((TextMessage)m).getText());
               }
               catch (JMSException e)
               {
                  fail(e);
               }
            }
         });

         TextMessage message = senderSession.createTextMessage();
         message.setText("test");
         sender.send(message);
         synchronized (this)
         {
            try
            {
               wait(1000);
            }
            catch (Exception e)
            {
               fail(e);
            }
         }
      }
      catch (JMSException e)
      {
         fail(e);
      }
   }

   /** 
    * Method to use this class in a Test suite
    */
   public static Test suite()
   {
      return new TestSuite(ConnectionTest.class);
   }

   public ConnectionTest(final String name)
   {
      super(name);
   }
}
