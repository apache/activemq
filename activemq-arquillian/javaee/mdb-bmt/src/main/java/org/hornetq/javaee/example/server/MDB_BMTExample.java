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
package org.hornetq.javaee.example.server;

import javax.annotation.Resource;
import javax.ejb.ActivationConfigProperty;
import javax.ejb.MessageDriven;
import javax.ejb.MessageDrivenContext;
import javax.ejb.TransactionManagement;
import javax.ejb.TransactionManagementType;
import javax.jms.Message;
import javax.jms.MessageListener;
import javax.jms.TextMessage;
import javax.transaction.UserTransaction;

/**
 * @author <a href="mailto:andy.taylor@jboss.org">Andy Taylor</a>
 */
@MessageDriven(name = "MDB_BMTExample", activationConfig = { @ActivationConfigProperty(propertyName = "destinationType", propertyValue = "javax.jms.Queue"),
                                                            @ActivationConfigProperty(propertyName = "destination", propertyValue = "queue/testQueue"),
                                                            // amq defaults to not look in jndi
                                                            @ActivationConfigProperty(propertyName = "useJndi", propertyValue = "true"),
                                                            @ActivationConfigProperty(propertyName = "acknowledgeMode", propertyValue = "Dups-ok-acknowledge") })
@TransactionManagement(value = TransactionManagementType.BEAN)
public class MDB_BMTExample implements MessageListener
{
   @Resource
   MessageDrivenContext ctx;

   public void onMessage(final Message message)
   {
      try
      {
         // Step 9. We know the client is sending a text message so we cast
         TextMessage textMessage = (TextMessage)message;

         // Step 10. get the text from the message.
         String text = textMessage.getText();

         System.out.println("message " + text + " received");

         // Step 11. lets look at the user transaction to make sure there isn't one.
         UserTransaction tx = ctx.getUserTransaction();

         if (tx != null)
         {
            tx.begin();
            System.out.println("we're in the middle of a transaction: " + tx);
            tx.commit();
         }
         else
         {
            System.out.println("something is wrong, I was expecting a transaction");
         }
      }
      catch (Exception e)
      {
         e.printStackTrace();
      }
   }
}