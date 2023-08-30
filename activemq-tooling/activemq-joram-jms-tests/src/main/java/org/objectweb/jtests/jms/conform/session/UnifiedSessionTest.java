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

import jakarta.jms.JMSException;
import jakarta.jms.QueueConnection;
import jakarta.jms.QueueSession;
import jakarta.jms.ServerSessionPool;
import jakarta.jms.Session;
import jakarta.jms.TopicConnection;
import jakarta.jms.TopicSession;

import org.junit.Assert;
import junit.framework.Test;
import junit.framework.TestSuite;

import org.objectweb.jtests.jms.framework.UnifiedTestCase;

/**
 * Test unified JMS 1.1 sessions.
 * <br />
 * See JMS 1.1 specifications
 * 
 * @author Jeff Mesnil (jmesnil@gmail.com)
 * @version $Id: UnifiedSessionTest.java,v 1.1 2007/03/29 04:28:37 starksm Exp $
 * @since JMS 1.1
 */
public class UnifiedSessionTest extends UnifiedTestCase
{

   /**
    * QueueConnection
    */
   protected QueueConnection queueConnection;

   /**
    * QueueSession (non transacted, AUTO_ACKNOWLEDGE)
    */
   protected QueueSession queueSession;

   /**
    * TopicConnection
    */
   protected TopicConnection topicConnection;

   /**
    * TopicSession (non transacted, AUTO_ACKNOWLEDGE)
    */
   protected TopicSession topicSession;

   /**
    * Test that a call to <code>createDurableConnectionConsumer()</code> method 
    * on a <code>QueueConnection</code> throws a 
    * <code>jakarta.jms.IllegalStateException</code>.
    * (see JMS 1.1 specs, table 4-1).
    * 
    * @since JMS 1.1
    */
   public void testCreateDurableConnectionConsumerOnQueueConnection()
   {
      try
      {
         queueConnection.createDurableConnectionConsumer(topic, "subscriptionName", "", (ServerSessionPool)null, 1);
         Assert.fail("Should throw a jakarta.jms.IllegalStateException");
      }
      catch (jakarta.jms.IllegalStateException e)
      {
      }
      catch (JMSException e)
      {
         Assert.fail("Should throw a jakarta.jms.IllegalStateException, not a " + e);
      }
   }

   /**
    * Test that a call to <code>createDurableSubscriber()</code> method 
    * on a <code>QueueSession</code> throws a 
    * <code>jakarta.jms.IllegalStateException</code>.
    * (see JMS 1.1 specs, table 4-1).
    * 
    * @since JMS 1.1
    */
   public void testCreateDurableSubscriberOnQueueSession()
   {
      try
      {
         queueSession.createDurableSubscriber(topic, "subscriptionName");
         Assert.fail("Should throw a jakarta.jms.IllegalStateException");
      }
      catch (jakarta.jms.IllegalStateException e)
      {
      }
      catch (JMSException e)
      {
         Assert.fail("Should throw a jakarta.jms.IllegalStateException, not a " + e);
      }
   }

   /**
    * Test that a call to <code>createTemporaryTopic()</code> method 
    * on a <code>QueueSession</code> throws a 
    * <code>jakarta.jms.IllegalStateException</code>.
    * (see JMS 1.1 specs, table 4-1).
    * 
    * @since JMS 1.1
    */
   public void testCreateTemporaryTopicOnQueueSession()
   {
      try
      {
         queueSession.createTemporaryTopic();
         Assert.fail("Should throw a jakarta.jms.IllegalStateException");
      }
      catch (jakarta.jms.IllegalStateException e)
      {
      }
      catch (JMSException e)
      {
         Assert.fail("Should throw a jakarta.jms.IllegalStateException, not a " + e);
      }
   }

   /**
    * Test that a call to <code>createTopic()</code> method 
    * on a <code>QueueSession</code> throws a 
    * <code>jakarta.jms.IllegalStateException</code>.
    * (see JMS 1.1 specs, table 4-1).
    * 
    * @since JMS 1.1
    */
   public void testCreateTopicOnQueueSession()
   {
      try
      {
         queueSession.createTopic("topic_name");
         Assert.fail("Should throw a jakarta.jms.IllegalStateException");
      }
      catch (jakarta.jms.IllegalStateException e)
      {
      }
      catch (JMSException e)
      {
         Assert.fail("Should throw a jakarta.jms.IllegalStateException, not a " + e);
      }
   }

   /**
    * Test that a call to <code>unsubscribe()</code> method 
    * on a <code>QueueSession</code> throws a 
    * <code>jakarta.jms.IllegalStateException</code>.
    * (see JMS 1.1 specs, table 4-1).
    * 
    * @since JMS 1.1
    */
   public void testUnsubscribeOnQueueSession()
   {
      try
      {
         queueSession.unsubscribe("subscriptionName");
         Assert.fail("Should throw a jakarta.jms.IllegalStateException");
      }
      catch (jakarta.jms.IllegalStateException e)
      {
      }
      catch (JMSException e)
      {
         Assert.fail("Should throw a jakarta.jms.IllegalStateException, not a " + e);
      }
   }

   /**
    * Test that a call to <code>createBrowser()</code> method 
    * on a <code>TopicSession</code> throws a 
    * <code>jakarta.jms.IllegalStateException</code>.
    * (see JMS 1.1 specs, table 4-1).
    * 
    * @since JMS 1.1
    */
   public void testCreateBrowserOnTopicSession()
   {
      try
      {
         topicSession.createBrowser(queue);
         Assert.fail("Should throw a jakarta.jms.IllegalStateException");
      }
      catch (jakarta.jms.IllegalStateException e)
      {
      }
      catch (JMSException e)
      {
         Assert.fail("Should throw a jakarta.jms.IllegalStateException, not a " + e);
      }
   }

   /**
    * Test that a call to <code>createQueue()</code> method 
    * on a <code>TopicSession</code> throws a 
    * <code>jakarta.jms.IllegalStateException</code>.
    * (see JMS 1.1 specs, table 4-1).
    * 
    * @since JMS 1.1
    */
   public void testCreateQueueOnTopicSession()
   {
      try
      {
         topicSession.createQueue("queue_name");
         Assert.fail("Should throw a jakarta.jms.IllegalStateException");
      }
      catch (jakarta.jms.IllegalStateException e)
      {
      }
      catch (JMSException e)
      {
         Assert.fail("Should throw a jakarta.jms.IllegalStateException, not a " + e);
      }
   }

   /**
    * Test that a call to <code>createTemporaryQueue()</code> method 
    * on a <code>TopicSession</code> throws a 
    * <code>jakarta.jms.IllegalStateException</code>.
    * (see JMS 1.1 specs, table 4-1).
    * 
    * @since JMS 1.1
    */
   public void testCreateTemporaryQueueOnTopicSession()
   {
      try
      {
         topicSession.createTemporaryQueue();
         Assert.fail("Should throw a jakarta.jms.IllegalStateException");
      }
      catch (jakarta.jms.IllegalStateException e)
      {
      }
      catch (JMSException e)
      {
         Assert.fail("Should throw a jakarta.jms.IllegalStateException, not a " + e);
      }
   }

   @Override
   public void setUp() throws Exception
   {
      super.setUp();
      try
      {
         queueConnection = queueConnectionFactory.createQueueConnection();
         queueSession = queueConnection.createQueueSession(false, Session.AUTO_ACKNOWLEDGE);
         topicConnection = topicConnectionFactory.createTopicConnection();
         topicSession = topicConnection.createTopicSession(false, Session.AUTO_ACKNOWLEDGE);

         queueConnection.start();
         topicConnection.start();
      }
      catch (Exception e)
      {
         throw new RuntimeException(e);
      }
   }

   @Override
   public void tearDown() throws Exception
   {
      try
      {
         queueConnection.close();
         topicConnection.close();
      }
      catch (Exception ignored)
      {
      }
      finally
      {
         queueConnection = null;
         queueSession = null;
         topicConnection = null;
         topicSession = null;
         super.tearDown();
      }
   }

   /** 
    * Method to use this class in a Test suite
    */
   public static Test suite()
   {
      return new TestSuite(UnifiedSessionTest.class);
   }

   public UnifiedSessionTest(final String name)
   {
      super(name);
   }
}
