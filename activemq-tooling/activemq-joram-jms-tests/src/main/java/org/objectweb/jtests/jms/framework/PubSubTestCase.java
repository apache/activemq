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
package org.objectweb.jtests.jms.framework;

import jakarta.jms.Session;
import jakarta.jms.Topic;
import jakarta.jms.TopicConnection;
import jakarta.jms.TopicConnectionFactory;
import jakarta.jms.TopicPublisher;
import jakarta.jms.TopicSession;
import jakarta.jms.TopicSubscriber;
import javax.naming.Context;

/**
 * Creates convenient JMS Publish/Subscribe objects which can be needed for tests.
 * <br />
 * This class defines the setUp and tearDown methods so
 * that JMS administrated objects and  other "ready to use" Pub/Sub objects (that is to say topics,
 * sessions, publishers and subscribers) are available conveniently for the test cases.
 * <br />
 * Classes which want that convenience should extend <code>PubSubTestCase</code> instead of 
 * <code>JMSTestCase</code>.
 *
 * @author Jeff Mesnil (jmesnil@gmail.com)
 * @version $Id: PubSubTestCase.java,v 1.2 2007/06/19 23:32:35 csuconic Exp $
 */
public abstract class PubSubTestCase extends JMSTestCase
{

   private Context ctx;

   private static final String TCF_NAME = "testTCF";

   private static final String TOPIC_NAME = "testJoramTopic";

   /**
    * Topic used by a publisher
    */
   protected Topic publisherTopic;

   /**
    * Publisher on queue
    */
   protected TopicPublisher publisher;

   /**
    * TopicConnectionFactory of the publisher
    */
   protected TopicConnectionFactory publisherTCF;

   /**
    * TopicConnection of the publisher
    */
   protected TopicConnection publisherConnection;

   /**
    * TopicSession of the publisher (non transacted, AUTO_ACKNOWLEDGE)
    */
   protected TopicSession publisherSession;

   /**
    * Topic used by a subscriber
    */
   protected Topic subscriberTopic;

   /**
    * Subscriber on queue
    */
   protected TopicSubscriber subscriber;

   /**
    * TopicConnectionFactory of the subscriber
    */
   protected TopicConnectionFactory subscriberTCF;

   /**
    * TopicConnection of the subscriber
    */
   protected TopicConnection subscriberConnection;

   /**
    * TopicSession of the subscriber (non transacted, AUTO_ACKNOWLEDGE)
    */
   protected TopicSession subscriberSession;

   /**
    * Create all administrated objects connections and sessions ready to use for tests.
    * <br />
    * Start connections.
    */
   @Override
   protected void setUp() throws Exception
   {
      super.setUp();

      try
      {
         // ...and creates administrated objects and binds them
         admin.createTopicConnectionFactory(PubSubTestCase.TCF_NAME);
         admin.createTopic(PubSubTestCase.TOPIC_NAME);

         // end of admin step, start of JMS client step
         ctx = admin.createContext();

         publisherTCF = (TopicConnectionFactory)ctx.lookup(PubSubTestCase.TCF_NAME);
         publisherTopic = (Topic)ctx.lookup(PubSubTestCase.TOPIC_NAME);
         publisherConnection = publisherTCF.createTopicConnection();
         publisherConnection.setClientID("publisherConnection");
         publisherSession = publisherConnection.createTopicSession(false, Session.AUTO_ACKNOWLEDGE);
         publisher = publisherSession.createPublisher(publisherTopic);

         subscriberTCF = (TopicConnectionFactory)ctx.lookup(PubSubTestCase.TCF_NAME);
         subscriberTopic = (Topic)ctx.lookup(PubSubTestCase.TOPIC_NAME);
         subscriberConnection = subscriberTCF.createTopicConnection();
         subscriberConnection.setClientID("subscriberConnection");
         subscriberSession = subscriberConnection.createTopicSession(false, Session.AUTO_ACKNOWLEDGE);
         subscriber = subscriberSession.createSubscriber(subscriberTopic);

         publisherConnection.start();
         subscriberConnection.start();
         // end of client step
      }
      catch (Exception e)
      {
         throw new RuntimeException(e);
      }
   }

   /**
    *  Close connections and delete administrated objects
    */
   @Override
   protected void tearDown() throws Exception
   {
      try
      {
         publisherConnection.close();
         subscriberConnection.close();

         admin.deleteTopicConnectionFactory(PubSubTestCase.TCF_NAME);
         admin.deleteTopic(PubSubTestCase.TOPIC_NAME);
      }
      catch (Exception ignored)
      {
        ignored.printStackTrace();
      }
      finally
      {
         publisherTopic = null;
         publisher = null;
         publisherTCF = null;
         publisherSession = null;
         publisherConnection = null;

         subscriberTopic = null;
         subscriber = null;
         subscriberTCF = null;
         subscriberSession = null;
         subscriberConnection = null;
      }

      super.tearDown();
   }

   public PubSubTestCase(final String name)
   {
      super(name);
   }
}
