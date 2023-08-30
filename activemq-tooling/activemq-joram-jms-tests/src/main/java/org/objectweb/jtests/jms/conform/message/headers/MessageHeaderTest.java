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
package org.objectweb.jtests.jms.conform.message.headers;

import jakarta.jms.DeliveryMode;
import jakarta.jms.Destination;
import jakarta.jms.JMSException;
import jakarta.jms.Message;
import jakarta.jms.Queue;
import jakarta.jms.TemporaryQueue;
import javax.naming.Context;
import javax.naming.NamingException;

import org.junit.Assert;
import junit.framework.Test;
import junit.framework.TestSuite;

import org.objectweb.jtests.jms.framework.PTPTestCase;
import org.objectweb.jtests.jms.framework.TestConfig;

/**
 * Test the headers of a message
 *
 * @author Jeff Mesnil (jmesnil@gmail.com)
 * @version $Id: MessageHeaderTest.java,v 1.1 2007/03/29 04:28:36 starksm Exp $
 */
public class MessageHeaderTest extends PTPTestCase
{

   /**
    * Test that the <code>MessageProducer.setPriority()</code> changes effectively
    * priority of the message.
    */
   public void testJMSPriority_2()
   {
      try
      {
         Message message = senderSession.createMessage();
         sender.send(message);
         sender.setPriority(9);
         sender.send(message);
         Assert.assertEquals("sec. 3.4.9 After completion of the send it holds the value specified by the " + "method sending the message.\n",
                             9,
                             message.getJMSPriority());

         receiver.receive(TestConfig.TIMEOUT);
      }
      catch (JMSException e)
      {
         fail(e);
      }
   }

   /**
    * Test that the priority set by <code>Message.setJMSPriority()</code> is ignored when a 
    * message is sent and that it holds the value specified when sending the message (i.e. 
    * <code>Message.DEFAULT_PRIORITY</code> in this test).
    */
   public void testJMSPriority_1()
   {
      try
      {
         Message message = senderSession.createMessage();
         message.setJMSPriority(0);
         sender.send(message);
         Assert.assertTrue("sec. 3.4.9 When a message is sent this value is ignored.\n", message.getJMSPriority() != 0);
         Assert.assertEquals("sec. 3.4.9 After completion of the send it holds the value specified by the " + "method sending the message.\n",
                             Message.DEFAULT_PRIORITY,
                             message.getJMSPriority());

         receiver.receive(TestConfig.TIMEOUT);
      }
      catch (JMSException e)
      {
         fail(e);
      }
   }

   /**
    * Test that the value of the <code>JMSExpiration<code> header field is the same
    * for the sent message and the received one.
    */
   public void testJMSExpiration()
   {
      try
      {
         Message message = senderSession.createMessage();
         sender.send(message);

         Message msg = receiver.receive(TestConfig.TIMEOUT);
         Assert.assertEquals("sec. 3.4.9 When a message is received its JMSExpiration header field contains this same " + "value [i.e. set on return of the send method].\n",
                             message.getJMSExpiration(),
                             msg.getJMSExpiration());
      }
      catch (JMSException e)
      {
         fail(e);
      }
   }

   /**
    * Test that the <code>JMSMessageID</code> is set by the provider when the <code>send</code> method returns 
    * and that it starts with <code>"ID:"</code>.
    */
   public void testJMSMessageID_2()
   {
      try
      {
         Message message = senderSession.createMessage();
         sender.send(message);
         Assert.assertTrue("sec. 3.4.3 When the send method returns it contains a provider-assigned value.\n",
                           message.getJMSMessageID() != null);
         Assert.assertTrue("sec. 3.4.3 All JMSMessageID values must start with the prefix 'ID:'.\n",
                           message.getJMSMessageID().startsWith("ID:"));

         Message msg = receiver.receive(TestConfig.TIMEOUT);
         Assert.assertTrue("sec. 3.4.3 All JMSMessageID values must start with the prefix 'ID:'.\n",
                           msg.getJMSMessageID().startsWith("ID:"));
      }
      catch (JMSException e)
      {
         fail(e);
      }
   }

   /**
    * Test that the <code>JMSMessageID</code> header field value is 
    * ignored when the message is sent.
    */
   public void testJMSMessageID_1()
   {
      try
      {
         Message message = senderSession.createMessage();
         message.setJMSMessageID("ID:foo");
         sender.send(message);
         Assert.assertTrue("sec. 3.4.3 When a message is sent this value is ignored.\n",
                           message.getJMSMessageID() != "ID:foo");
         receiver.receive(TestConfig.TIMEOUT);
      }
      catch (JMSException e)
      {
         fail(e);
      }
   }

   /**
    * Test that the <code>JMSDeliveryMode</code> header field value is ignored
    * when the message is sent and that it holds the value specified by the sending 
    * method (i.e. <code>Message.DEFAULT_DELIVERY_MODE</code> in this test when the message is received.
    */
   public void testJMSDeliveryMode()
   {
      try
      {
         // sender has been created with the DEFAULT_DELIVERY_MODE which is PERSISTENT
         Assert.assertEquals(DeliveryMode.PERSISTENT, sender.getDeliveryMode());
         Message message = senderSession.createMessage();
         // send a message specfiying NON_PERSISTENT for the JMSDeliveryMode header field
         message.setJMSDeliveryMode(DeliveryMode.NON_PERSISTENT);
         sender.send(message);
         Assert.assertTrue("sec. 3.4.2 When a message is sent this value is ignored",
                           message.getJMSDeliveryMode() != DeliveryMode.NON_PERSISTENT);
         Assert.assertEquals("sec. 3.4.2 After completion of the send it holds the delivery mode specified " + "by the sending method (persistent by default).\n",
                             Message.DEFAULT_DELIVERY_MODE,
                             message.getJMSDeliveryMode());

         receiver.receive(TestConfig.TIMEOUT);
      }
      catch (JMSException e)
      {
         fail(e);
      }
   }

   /**
    * Test that the <code>JMSDestination</code> header field value is ignored when the message
    * is sent and that after completion of the sending method, it holds the <code>Destination</code>
    * specified by the sending method.
    * Also test that the value of the header on the received message is the same that on the sent message.
    */
   public void testJMSDestination()
   {
      try
      {
         admin.createQueue("anotherQueue");
         Context ctx = admin.createContext();
         Queue anotherQueue = (Queue)ctx.lookup("anotherQueue");
         Assert.assertTrue(anotherQueue != senderQueue);

         // set the JMSDestination header field to the anotherQueue Destination
         Message message = senderSession.createMessage();
         message.setJMSDestination(anotherQueue);
         sender.send(message);
         Assert.assertTrue("sec. 3.4.1 When a message is sent this value is ignored.\n",
                           message.getJMSDestination() != anotherQueue);
         Assert.assertEquals("sec. 3.4.1 After completion of the send it holds the destination object specified " + "by the sending method.\n",
                             senderQueue,
                             message.getJMSDestination());

         Message msg = receiver.receive(TestConfig.TIMEOUT);
         Assert.assertEquals("sec. 3.4.1 When a message is received, its destination value must be equivalent  " + " to the value assigned when it was sent.\n",
                             ((Queue)message.getJMSDestination()).getQueueName(),
                             ((Queue)msg.getJMSDestination()).getQueueName());

         admin.deleteQueue("anotherQueue");
      }
      catch (JMSException e)
      {
         fail(e);
      }
      catch (NamingException e)
      {
         Assert.fail(e.getMessage());
      }
   }

   /**
    * Test that a <code>Destination</code> set by the <code>setJMSReplyTo()</code>
    * method on a sended message corresponds to the <code>Destination</code> get by 
    * the </code>getJMSReplyTo()</code> method.
    */
   public void testJMSReplyTo_1()
   {
      try
      {
         Message message = senderSession.createMessage();
         message.setJMSReplyTo(senderQueue);
         sender.send(message);

         Message msg = receiver.receive(TestConfig.TIMEOUT);
         Destination dest = msg.getJMSReplyTo();
         Assert.assertTrue("JMS ReplyTo header field should be a Queue", dest instanceof Queue);
         Queue replyTo = (Queue)dest;
         Assert.assertEquals("JMS ReplyTo header field should be equals to the sender queue",
                             replyTo.getQueueName(),
                             senderQueue.getQueueName());
      }
      catch (JMSException e)
      {
         fail(e);
      }
   }

   /** 
    * Test that if the JMS ReplyTo header field has been set as a <code>TemporaryQueue</code>,
    * it will be rightly get also as a <code>TemporaryQueue</code> 
    * (and not only as a <code>Queue</code>).
    */
   public void testJMSReplyTo_2()
   {
      try
      {
         TemporaryQueue tempQueue = senderSession.createTemporaryQueue();
         Message message = senderSession.createMessage();
         message.setJMSReplyTo(tempQueue);
         sender.send(message);

         Message msg = receiver.receive(TestConfig.TIMEOUT);
         Destination dest = msg.getJMSReplyTo();
         Assert.assertTrue("JMS ReplyTo header field should be a TemporaryQueue", dest instanceof TemporaryQueue);
         Queue replyTo = (Queue)dest;
         Assert.assertEquals("JMS ReplyTo header field should be equals to the temporary queue",
                             replyTo.getQueueName(),
                             tempQueue.getQueueName());
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
      return new TestSuite(MessageHeaderTest.class);
   }

   public MessageHeaderTest(final String name)
   {
      super(name);
   }
}
