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

package org.objectweb.jtests.jms.framework;

import javax.jms.Queue;
import javax.jms.QueueConnection;
import javax.jms.QueueConnectionFactory;
import javax.jms.QueueReceiver;
import javax.jms.QueueSender;
import javax.jms.QueueSession;
import javax.jms.Session;
import javax.naming.Context;

/**
 * Creates convenient Point to Point JMS objects which can be needed for tests.
 * <br />
 * This class defines the setUp and tearDown methods so
 * that JMS administrated objects and  other "ready to use" PTP objects (that is to say queues,
 * sessions, senders and receviers) are available conveniently for the test cases.
 * <br />
 * Classes which want that convenience should extend <code>PTPTestCase</code> instead of 
 * <code>JMSTestCase</code>.
 *
 * @author Jeff Mesnil (jmesnil@gmail.com)
 * @version $Id: PTPTestCase.java,v 1.1 2007/03/29 04:28:35 starksm Exp $
 */
public abstract class PTPTestCase extends JMSTestCase
{

   protected Context ctx;

   private static final String QCF_NAME = "testQCF";

   private static final String QUEUE_NAME = "testJoramQueue";

   /**
    * Queue used by a sender
    */
   protected Queue senderQueue;

   /**
    * Sender on queue
    */
   protected QueueSender sender;

   /**
    * QueueConnectionFactory of the sender
    */
   protected QueueConnectionFactory senderQCF;

   /**
    * QueueConnection of the sender
    */
   protected QueueConnection senderConnection;

   /**
    * QueueSession of the sender (non transacted, AUTO_ACKNOWLEDGE)
    */
   protected QueueSession senderSession;

   /**
    * Queue used by a receiver
    */
   protected Queue receiverQueue;

   /**
    * Receiver on queue
    */
   protected QueueReceiver receiver;

   /**
    * QueueConnectionFactory of the receiver
    */
   protected QueueConnectionFactory receiverQCF;

   /**
    * QueueConnection of the receiver
    */
   protected QueueConnection receiverConnection;

   /**
    * QueueSession of the receiver (non transacted, AUTO_ACKNOWLEDGE)
    */
   protected QueueSession receiverSession;

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
         admin.createQueueConnectionFactory(PTPTestCase.QCF_NAME);
         admin.createQueue(PTPTestCase.QUEUE_NAME);

         // end of admin step, start of JMS client step
         ctx = admin.createContext();

         senderQCF = (QueueConnectionFactory)ctx.lookup(PTPTestCase.QCF_NAME);
         senderQueue = (Queue)ctx.lookup(PTPTestCase.QUEUE_NAME);
         senderConnection = senderQCF.createQueueConnection();
         senderSession = senderConnection.createQueueSession(false, Session.AUTO_ACKNOWLEDGE);
         sender = senderSession.createSender(senderQueue);

         receiverQCF = (QueueConnectionFactory)ctx.lookup(PTPTestCase.QCF_NAME);
         receiverQueue = (Queue)ctx.lookup(PTPTestCase.QUEUE_NAME);
         receiverConnection = receiverQCF.createQueueConnection();
         receiverSession = receiverConnection.createQueueSession(false, Session.AUTO_ACKNOWLEDGE);
         receiver = receiverSession.createReceiver(receiverQueue);

         senderConnection.start();
         receiverConnection.start();
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
         senderConnection.close();
         receiverConnection.close();

         admin.deleteQueueConnectionFactory(PTPTestCase.QCF_NAME);
         admin.deleteQueue(PTPTestCase.QUEUE_NAME);
      }
      catch (Exception ignored)
      {
         ignored.printStackTrace();
      }
      finally
      {
         senderQueue = null;
         sender = null;
         senderQCF = null;
         senderSession = null;
         senderConnection = null;

         receiverQueue = null;
         receiver = null;
         receiverQCF = null;
         receiverSession = null;
         receiverConnection = null;
      }

      super.tearDown();
   }

   public PTPTestCase(final String name)
   {
      super(name);
   }
}
