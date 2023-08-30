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
package org.objectweb.jtests.jms.conform.session;

import jakarta.jms.InvalidDestinationException;
import jakarta.jms.InvalidSelectorException;
import jakarta.jms.JMSException;
import jakarta.jms.Message;
import jakarta.jms.Queue;
import jakarta.jms.TextMessage;

import org.junit.Assert;
import junit.framework.Test;
import junit.framework.TestSuite;

import org.objectweb.jtests.jms.framework.PTPTestCase;
import org.objectweb.jtests.jms.framework.TestConfig;

/**
 * Test queue sessions
 * <br />
 * See JMS specifications, sec. 4.4 Session
 * 
 * @author Jeff Mesnil (jmesnil@gmail.com)
 * @version $Id: QueueSessionTest.java,v 1.2 2007/06/19 23:32:35 csuconic Exp $
 */
public class QueueSessionTest extends PTPTestCase
{

   /**
    * Test that if we rollback a transaction which has consumed a message,
    * the message is effectively redelivered.
    */
   public void testRollbackRececeivedMessage()
   {
      try
      {
         senderConnection.stop();
         // senderSession has been created as non transacted
         // we create it again but as a transacted session
         senderSession = senderConnection.createQueueSession(true, 0);
         Assert.assertEquals(true, senderSession.getTransacted());
         // we create again the sender
         sender = senderSession.createSender(senderQueue);
         senderConnection.start();

         receiverConnection.stop();
         // receiverSession has been created as non transacted
         // we create it again but as a transacted session
         receiverSession = receiverConnection.createQueueSession(true, 0);
         Assert.assertEquals(true, receiverSession.getTransacted());

         if (receiver != null)
         {
            receiver.close();
         }
         // we create again the receiver
         receiver = receiverSession.createReceiver(receiverQueue);
         receiverConnection.start();

         // we send a message...
         TextMessage message = senderSession.createTextMessage();
         message.setText("testRollbackRececeivedMessage");
         sender.send(message);
         // ... and commit the *producer* transaction
         senderSession.commit();

         // we receive a message...
         Message m = receiver.receive(TestConfig.TIMEOUT);
         Assert.assertTrue(m != null);
         Assert.assertTrue(m instanceof TextMessage);
         TextMessage msg = (TextMessage)m;
         // ... which is the one which was sent...
         Assert.assertEquals("testRollbackRececeivedMessage", msg.getText());
         // ...and has not been redelivered
         Assert.assertEquals(false, msg.getJMSRedelivered());

         // we rollback the *consumer* transaction
         receiverSession.rollback();

         // we receive again a message
         m = receiver.receive(TestConfig.TIMEOUT);
         Assert.assertTrue(m != null);
         Assert.assertTrue(m instanceof TextMessage);
         msg = (TextMessage)m;
         // ... which is still the one which was sent...
         Assert.assertEquals("testRollbackRececeivedMessage", msg.getText());
         // .. but this time, it has been redelivered
         Assert.assertEquals(true, msg.getJMSRedelivered());

      }
      catch (Exception e)
      {
         fail(e);
      }
   }

   /**
    * Test that a call to the <code>createBrowser()</code> method with an invalid
    * messaeg session throws a <code>jakarta.jms.InvalidSelectorException</code>.
    */
   public void testCreateBrowser_2()
   {
      try
      {
         senderSession.createBrowser(senderQueue, "definitely not a message selector!");
         Assert.fail("Should throw a jakarta.jms.InvalidSelectorException.\n");
      }
      catch (InvalidSelectorException e)
      {
      }
      catch (JMSException e)
      {
         Assert.fail("Should throw a jakarta.jms.InvalidSelectorException, not a " + e);
      }
   }

   /**
    * Test that a call to the <code>createBrowser()</code> method with an invalid
    * <code>Queue</code> throws a <code>jakarta.jms.InvalidDestinationException</code>.
    */
   public void testCreateBrowser_1()
   {
      try
      {
         senderSession.createBrowser((Queue)null);
         Assert.fail("Should throw a jakarta.jms.InvalidDestinationException.\n");
      }
      catch (InvalidDestinationException e)
      {
      }
      catch (JMSException e)
      {
         Assert.fail("Should throw a jakarta.jms.InvalidDestinationException, not a " + e);
      }
   }

   /**
    * Test that a call to the <code>createReceiver()</code> method with an invalid
    * message selector throws a <code>jakarta.jms.InvalidSelectorException</code>.
    */
   public void testCreateReceiver_2()
   {
      try
      {
         receiver = senderSession.createReceiver(senderQueue, "definitely not a message selector!");
         Assert.fail("Should throw a jakarta.jms.InvalidSelectorException.\n");
      }
      catch (InvalidSelectorException e)
      {
      }
      catch (JMSException e)
      {
         Assert.fail("Should throw a jakarta.jms.InvalidSelectorException, not a " + e);
      }
   }

   /**
    * Test that a call to the <code>createReceiver()</code> method with an invalid
    * <code>Queue</code> throws a <code>jakarta.jms.InvalidDestinationException</code>>
    */
   public void testCreateReceiver_1()
   {
      try
      {
         receiver = senderSession.createReceiver((Queue)null);
         Assert.fail("Should throw a jakarta.jms.InvalidDestinationException.\n");
      }
      catch (InvalidDestinationException e)
      {
      }
      catch (JMSException e)
      {
         Assert.fail("Should throw a jakarta.jms.InvalidDestinationException, not a " + e);
      }
   }

   /** 
    * Method to use this class in a Test suite
    */
   public static Test suite()
   {
      return new TestSuite(QueueSessionTest.class);
   }

   public QueueSessionTest(final String name)
   {
      super(name);
   }
}
