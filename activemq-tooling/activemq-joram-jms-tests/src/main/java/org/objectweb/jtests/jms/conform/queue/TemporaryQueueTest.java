/*
 * Copyright 2009 Red Hat, Inc.
 * Red Hat licenses this file to you under the Apache License, version
 * 2.0 (the "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *    http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 */

package org.objectweb.jtests.jms.conform.queue;

import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.QueueReceiver;
import javax.jms.TemporaryQueue;
import javax.jms.TextMessage;

import junit.framework.Assert;
import junit.framework.Test;
import junit.framework.TestSuite;

import org.objectweb.jtests.jms.framework.PTPTestCase;
import org.objectweb.jtests.jms.framework.TestConfig;

/**
 * Test the <code>javax.jms.TemporaryQueue</code> features.
 *
 * @author Jeff Mesnil (jmesnil@gmail.com)
 * @version $Id: TemporaryQueueTest.java,v 1.1 2007/03/29 04:28:37 starksm Exp $
 */
public class TemporaryQueueTest extends PTPTestCase
{

   private TemporaryQueue tempQueue;

   private QueueReceiver tempReceiver;

   /**
    * Test a TemporaryQueue
    */
   public void testTemporaryQueue()
   {
      try
      {
         // we stop both sender and receiver connections
         senderConnection.stop();
         receiverConnection.stop();
         // we create a temporary queue to receive messages
         tempQueue = receiverSession.createTemporaryQueue();
         // we recreate the sender because it has been
         // already created with a Destination as parameter
         sender = senderSession.createSender(null);
         // we create a receiver on the temporary queue
         tempReceiver = receiverSession.createReceiver(tempQueue);
         receiverConnection.start();
         senderConnection.start();

         TextMessage message = senderSession.createTextMessage();
         message.setText("testTemporaryQueue");
         sender.send(tempQueue, message);

         Message m = tempReceiver.receive(TestConfig.TIMEOUT);
         Assert.assertTrue(m instanceof TextMessage);
         TextMessage msg = (TextMessage)m;
         Assert.assertEquals("testTemporaryQueue", msg.getText());
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
      return new TestSuite(TemporaryQueueTest.class);
   }

   public TemporaryQueueTest(final String name)
   {
      super(name);
   }
}
