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
package org.objectweb.jtests.jms.conform.message.properties;

import java.util.Enumeration;
import java.util.Vector;

import jakarta.jms.JMSException;
import jakarta.jms.Message;
import jakarta.jms.MessageFormatException;
import jakarta.jms.TextMessage;

import org.junit.Assert;
import junit.framework.Test;
import junit.framework.TestSuite;

import org.objectweb.jtests.jms.framework.PTPTestCase;

/**
 * Test the <code>jakarta.jms.Message</code> properties.
 * <br />
 *  See JMS Specification, sec. 3.5 Message Properties (p.32-37)
 *
 * @author Jeff Mesnil (jmesnil@gmail.com)
 * @version $Id: MessagePropertyTest.java,v 1.1 2007/03/29 04:28:34 starksm Exp $
 */
public class MessagePropertyTest extends PTPTestCase
{

   /**
    * Test that any other class than <code>Boolean, Byte, Short, Integer, Long,
    * Float, Double</code> and <code>String</code> used in the <code>Message.setObjectProperty()</code>
    * method throws a <code>jakarta.jms.MessageFormatException</code>.
    */
   public void testSetObjectProperty_2()
   {
      try
      {
         Message message = senderSession.createMessage();
         message.setObjectProperty("prop", new Vector());
         Assert.fail("sec. 3.5.5 An attempt to use any other class [than Boolean, Byte,...,String] must throw " + "a JMS MessageFormatException.\n");
      }
      catch (MessageFormatException e)
      {
      }
      catch (JMSException e)
      {
         Assert.fail("Should throw a jakarta.jms.MessageFormatException, not a " + e);
      }
   }

   /**
    * if a property is set as a <code>Float</code> with the <code>Message.setObjectProperty()</code>
    * method, it can be retrieve directly as a <code>double</code> by <code>Message.getFloatProperty()</code>
    */
   public void testSetObjectProperty_1()
   {
      try
      {
         Message message = senderSession.createMessage();
         message.setObjectProperty("pi", Float.valueOf(3.14159f));
         Assert.assertEquals(3.14159f, message.getFloatProperty("pi"), 0);
      }
      catch (JMSException e)
      {
         fail(e);
      }
   }

   /**
    * Test that a <code>null</code> value is returned by the <code>Message.getObjectProperty()</code> method
    * if a property by the specified name does not exits.
    */
   public void testGetObjectProperty()
   {
      try
      {
         Message message = senderSession.createMessage();
         Assert.assertEquals("sec. 3.5.5 A null value is returned [by the getObjectProperty method] if a property by the specified " + "name does not exits.\n",
                             null,
                             message.getObjectProperty("prop"));
      }
      catch (JMSException e)
      {
         fail(e);
      }
   }

   /**
    * Test that a <code>null</code> value is returned by the <code>Message.getStringProperty()</code> method
    * if a property by the specified name does not exits.
    */
   public void testGetStringProperty()
   {
      try
      {
         Message message = senderSession.createMessage();
         Assert.assertEquals("sec. 3.5.5 A null value is returned [by the getStringProperty method] if a property by the specified " + "name does not exits.\n",
                             null,
                             message.getStringProperty("prop"));
      }
      catch (JMSException e)
      {
         fail(e);
      }
   }

   /** 
    * Test that an attempt to get a <code>double</code> property which does not exist throw
    * a <code>java.lang.NullPointerException</code>
    */
   public void testGetDoubleProperty()
   {
      try
      {
         Message message = senderSession.createMessage();
         message.getDoubleProperty("prop");
         Assert.fail("Should raise a NullPointerException.\n");
      }
      catch (NullPointerException e)
      {
      }
      catch (JMSException e)
      {
         fail(e);
      }
   }

   /** 
    * Test that an attempt to get a <code>float</code> property which does not exist throw
    * a <code>java.lang.NullPointerException</code>
    */
   public void testGetFloatProperty()
   {
      try
      {
         Message message = senderSession.createMessage();
         message.getFloatProperty("prop");
         Assert.fail("Should raise a NullPointerException.\n");
      }
      catch (NullPointerException e)
      {
      }
      catch (JMSException e)
      {
         fail(e);
      }
   }

   /** 
    * Test that an attempt to get a <code>long</code> property which does not exist throw
    * a <code>java.lang.NumberFormatException</code>
    */
   public void testGetLongProperty()
   {
      try
      {
         Message message = senderSession.createMessage();
         message.getLongProperty("prop");
         Assert.fail("Should raise a NumberFormatException.\n");
      }
      catch (NumberFormatException e)
      {
      }
      catch (JMSException e)
      {
         fail(e);
      }
   }

   /** 
    * Test that an attempt to get a <code>int</code> property which does not exist throw
    * a <code>java.lang.NumberFormatException</code>
    */
   public void testGetIntProperty()
   {
      try
      {
         Message message = senderSession.createMessage();
         message.getIntProperty("prop");
         Assert.fail("Should raise a NumberFormatException.\n");
      }
      catch (NumberFormatException e)
      {
      }
      catch (JMSException e)
      {
         fail(e);
      }
   }

   /** 
    * Test that an attempt to get a <code>short</code> property which does not exist throw
    * a <code>java.lang.NumberFormatException</code>
    */
   public void testGetShortProperty()
   {
      try
      {
         Message message = senderSession.createMessage();
         message.getShortProperty("prop");
         Assert.fail("Should raise a NumberFormatException.\n");
      }
      catch (NumberFormatException e)
      {
      }
      catch (JMSException e)
      {
         fail(e);
      }
   }

   /** 
    * Test that an attempt to get a <code>byte</code> property which does not exist throw
    * a <code>java.lang.NumberFormatException</code>
    */
   public void testGetByteProperty()
   {
      try
      {
         Message message = senderSession.createMessage();
         message.getByteProperty("prop");
         Assert.fail("Should raise a NumberFormatException.\n");
      }
      catch (NumberFormatException e)
      {
      }
      catch (JMSException e)
      {
         fail(e);
      }
   }

   /** 
    * Test that an attempt to get a <code>boolean</code> property which does not exist 
    * returns <code>false</code>
    */
   public void testGetBooleanProperty()
   {
      try
      {
         Message message = senderSession.createMessage();
         Assert.assertEquals(false, message.getBooleanProperty("prop"));
      }
      catch (JMSException e)
      {
         fail(e);
      }
   }

   /**
    * Test that the <code>Message.getPropertyNames()</code> method does not return
    * the name of the JMS standard header fields (e.g. <code>JMSCorrelationID</code>).
    */
   public void testGetPropertyNames()
   {
      try
      {
         Message message = senderSession.createMessage();
         message.setJMSCorrelationID("foo");
         Enumeration enumeration = message.getPropertyNames();
         while (enumeration.hasMoreElements())
         {
            String propName = (String)enumeration.nextElement();
            boolean valid = !propName.startsWith("JMS") || propName.startsWith("JMSX");
            Assert.assertTrue("sec. 3.5.6 The getPropertyNames method does not return the names of " + "the JMS standard header field [e.g. JMSCorrelationID]: " +
                                       propName,
                              valid);
         }
      }
      catch (JMSException e)
      {
         fail(e);
      }
   }

   /**
    * Test that the <code>Message.getPropertyNames()</code> methods.
    */
   public void testPropertyIteration()
   {
      try
      {
         Message message = senderSession.createMessage();
         Enumeration enumeration = message.getPropertyNames();
         // there can be some properties already defined (e.g. JMSXDeliveryCount)
         int originalCount = 0;
         while (enumeration.hasMoreElements())
         {
            enumeration.nextElement();
            originalCount++;
         }
         message.setDoubleProperty("pi", 3.14159);
         enumeration = message.getPropertyNames();
         boolean foundPiProperty = false;
         int newCount = 0;
         while (enumeration.hasMoreElements())
         {
            String propName = (String)enumeration.nextElement();
            newCount++;
            if ("pi".equals(propName))
            {
               foundPiProperty = true;
            }
         }
         Assert.assertEquals(originalCount + 1, newCount);
         Assert.assertTrue(foundPiProperty);
      }
      catch (JMSException e)
      {
         fail(e);
      }
   }

   /**
    * Test that the <code>Message.clearProperties()</code> method does not clear the
    * value of the Message's body.
    */
   public void testClearProperties_2()
   {
      try
      {
         TextMessage message = senderSession.createTextMessage();
         message.setText("foo");
         message.clearProperties();
         Assert.assertEquals("sec. 3.5.7 Clearing a message's  property entries does not clear the value of its body.\n",
                             "foo",
                             message.getText());
      }
      catch (JMSException e)
      {
         fail(e);
      }
   }

   /**
    * Test that the <code>Message.clearProperties()</code> method deletes all the 
    * properties of the Message.
    */
   public void testClearProperties_1()
   {
      try
      {
         TextMessage message = senderSession.createTextMessage();
         message.setStringProperty("prop", "foo");
         message.clearProperties();
         Assert.assertEquals("sec. 3.5.7 A message's properties are deleted by the clearProperties method.\n",
                             null,
                             message.getStringProperty("prop"));
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
      return new TestSuite(MessagePropertyTest.class);
   }

   public MessagePropertyTest(final String name)
   {
      super(name);
   }
}
