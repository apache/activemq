/*
 * Copyright 2009 Red Hat, Inc.
 *  Red Hat licenses this file to you under the Apache License, version
 *  2.0 (the "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *     http://www.apache.org/licenses/LICENSE-2.0
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 *  implied.  See the License for the specific language governing
 *  permissions and limitations under the License.
 */
package org.hornetq.javaee.example.server;

import javax.ejb.ActivationConfigProperty;
import javax.ejb.MessageDriven;
import javax.jms.Message;
import javax.jms.MessageListener;
import javax.jms.TextMessage;

/**
 * MDB that is connected to the remote queue.
 * @author <a href="mailto:clebert.suconic@jboss.org">Clebert Suconic</a>
 */

//Step 9. The message is received on the MDB, using a local queue.
@MessageDriven(name = "MDB_QueueA",
               activationConfig =
                     {
                        @ActivationConfigProperty(propertyName = "destinationType", propertyValue = "javax.jms.Queue"),
                        @ActivationConfigProperty(propertyName = "destination", propertyValue = "queue/A"),
                        @ActivationConfigProperty(propertyName = "acknowledgeMode", propertyValue = "Auto-acknowledge")
                     })
public class MDBQueueA implements MessageListener
{
   public void onMessage(Message message)
   {
      try
      {
         TextMessage tm = (TextMessage)message;

         String text = tm.getText();

         System.out.println("Step 10: (MDBQueueA.java) Message received using the default adapter. Message = \"" + text + "\"" );

      }
      catch (Exception e)
      {
         e.printStackTrace();
      }
   }
}
