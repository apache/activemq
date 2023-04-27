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
package org.objectweb.jtests.jms.conform.message;

import jakarta.jms.DeliveryMode;
import jakarta.jms.Message;

import org.junit.Assert;
import junit.framework.Test;
import junit.framework.TestSuite;

import org.objectweb.jtests.jms.framework.JMSTestCase;

/**
 * Test the default constants of the <code>jakarta.jms.Message</code> interface.
 *
 * @author Jeff Mesnil (jmesnil@gmail.com)
 * @version $Id: MessageDefaultTest.java,v 1.1 2007/03/29 04:28:37 starksm Exp $
 */
public class MessageDefaultTest extends JMSTestCase
{

   /**
    * test that the <code>DEFAULT_DELIVERY_MODE</code> of <code>jakarta.jms.Message</code>
    * corresponds to <code>jakarta.jms.Delivery.PERSISTENT</code>.
    */
   public void testDEFAULT_DELIVERY_MODE()
   {
      Assert.assertEquals("The delivery mode is persistent by default.\n",
                          DeliveryMode.PERSISTENT,
                          Message.DEFAULT_DELIVERY_MODE);
   }

   /**
    * test that the <code>DEFAULT_PRIORITY</code> of <code>jakarta.jms.Message</code>
    * corresponds to 4.
    */
   public void testDEFAULT_PRIORITY()
   {
      Assert.assertEquals("The default priority is 4.\n", 4, Message.DEFAULT_PRIORITY);
   }

   /** 
    * Method to use this class in a Test suite
    */
   public static Test suite()
   {
      return new TestSuite(MessageDefaultTest.class);
   }

   public MessageDefaultTest(final String name)
   {
      super(name);
   }
}
