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

package org.objectweb.jtests.jms.conform.topic;

import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.TemporaryTopic;
import javax.jms.TextMessage;
import javax.jms.TopicSubscriber;

import junit.framework.Assert;
import junit.framework.Test;
import junit.framework.TestSuite;

import org.objectweb.jtests.jms.framework.PubSubTestCase;
import org.objectweb.jtests.jms.framework.TestConfig;

/**
 * Test the <code>javax.jms.TemporaryTopic</code> features.
 *
 * @author Jeff Mesnil (jmesnil@gmail.com)
 * @version $Id: TemporaryTopicTest.java,v 1.1 2007/03/29 04:28:34 starksm Exp $
 */
public class TemporaryTopicTest extends PubSubTestCase
{

   private TemporaryTopic tempTopic;

   private TopicSubscriber tempSubscriber;

   /**
    * Test a TemporaryTopic
    */
   public void testTemporaryTopic()
   {
      try
      {
         // we stop both publisher and subscriber connections
         publisherConnection.stop();
         subscriberConnection.stop();
         // we create a temporary topic to receive messages
         tempTopic = subscriberSession.createTemporaryTopic();
         // we recreate the sender because it has been
         // already created with another Destination as parameter
         publisher = publisherSession.createPublisher(tempTopic);
         // we create a temporary subscriber on the temporary topic
         tempSubscriber = subscriberSession.createSubscriber(tempTopic);
         subscriberConnection.start();
         publisherConnection.start();

         TextMessage message = publisherSession.createTextMessage();
         message.setText("testTemporaryTopic");
         publisher.publish(message);

         Message m = tempSubscriber.receive(TestConfig.TIMEOUT);
         Assert.assertTrue(m instanceof TextMessage);
         TextMessage msg = (TextMessage)m;
         Assert.assertEquals("testTemporaryTopic", msg.getText());
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
      return new TestSuite(TemporaryTopicTest.class);
   }

   public TemporaryTopicTest(final String name)
   {
      super(name);
   }
}
