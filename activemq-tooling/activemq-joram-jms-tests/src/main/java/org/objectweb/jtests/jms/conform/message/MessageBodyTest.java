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

package org.objectweb.jtests.jms.conform.message;

import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageNotWriteableException;
import javax.jms.TextMessage;

import junit.framework.Assert;
import junit.framework.Test;
import junit.framework.TestSuite;

import org.objectweb.jtests.jms.framework.PTPTestCase;
import org.objectweb.jtests.jms.framework.TestConfig;

/**
 * Tests on message body.
 *
 * @author Jeff Mesnil (jmesnil@gmail.com)
 * @version $Id: MessageBodyTest.java,v 1.1 2007/03/29 04:28:37 starksm Exp $
 */
public class MessageBodyTest extends PTPTestCase
{
   
   /** 
    * Method to use this class in a Test suite
    */
   public static Test suite()
   {
      return new TestSuite(MessageBodyTest.class);
   }


   /**
    * Test that the <code>TextMessage.clearBody()</code> method does nto clear the 
    * message properties.
    */
   public void testClearBody_2()
   {
      try
      {
         TextMessage message = senderSession.createTextMessage();
         message.setStringProperty("prop", "foo");
         message.clearBody();
         Assert.assertEquals("sec. 3.11.1 Clearing a message's body does not clear its property entries.\n",
                             "foo",
                             message.getStringProperty("prop"));
      }
      catch (JMSException e)
      {
         fail(e);
      }
   }

   /**
    * Test that the <code>TextMessage.clearBody()</code> effectively clear the body of the message
    */
   public void testClearBody_1()
   {
      try
      {
         TextMessage message = senderSession.createTextMessage();
         message.setText("bar");
         message.clearBody();
         Assert.assertEquals("sec. 3 .11.1 the clearBody method of Message resets the value of the message body " + "to the 'empty' initial message value as set by the message type's create "
                                      + "method provided by Session.\n",
                             null,
                             message.getText());
      }
      catch (JMSException e)
      {
         fail(e);
      }
   }

   /**
    * Test that a call to the <code>TextMessage.setText()</code> method on a 
    * received message raises a <code>javax.jms.MessageNotWriteableException</code>.
    */
   public void testWriteOnReceivedBody()
   {
      try
      {
         TextMessage message = senderSession.createTextMessage();
         message.setText("foo");
         sender.send(message);

         Message m = receiver.receive(TestConfig.TIMEOUT);
         Assert.assertTrue("The message should be an instance of TextMessage.\n", m instanceof TextMessage);
         TextMessage msg = (TextMessage)m;
         msg.setText("bar");
         Assert.fail("should raise a MessageNotWriteableException (sec. 3.11.2)");
      }
      catch (MessageNotWriteableException e)
      {
      }
      catch (JMSException e)
      {
         fail(e);
      }
   }

   public MessageBodyTest(final String name)
   {
      super(name);
   }

   @Override
   protected void setUp() throws Exception
   {

      super.setUp();
   }

   @Override
   protected void tearDown() throws Exception
   {
      super.tearDown();

   }
}
