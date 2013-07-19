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
package org.hornetq.javaee.example.server2;

import org.jboss.ejb3.annotation.ResourceAdapter;

import javax.ejb.ActivationConfigProperty;
import javax.ejb.MessageDriven;
import javax.jms.Message;
import javax.jms.MessageListener;
import javax.jms.TextMessage;

/**
 * MDB that is connected to the remote queue.
 * @author <a href="mailto:clebert.suconic@jboss.org">Clebert Suconic</a>
 */

//Step 10. The message is received on the MDB, using a remote queue.
@MessageDriven(name = "MDB_QueueB",
               activationConfig =
                     {
                        @ActivationConfigProperty(propertyName = "destinationType", propertyValue = "javax.jms.Queue"),
                        @ActivationConfigProperty(propertyName = "destination", propertyValue = "queue/B"),
                        @ActivationConfigProperty(propertyName = "acknowledgeMode", propertyValue = "Auto-acknowledge")
                     })
@ResourceAdapter("activemq-remote.rar")
public class MDBQueueB implements MessageListener
{
   public void onMessage(Message message)
   {
      try
      {
         TextMessage tm = (TextMessage)message;

         String text = tm.getText();

         System.out.println("Step 11: (MDBQueueB.java) Message received using the remote adapter. Message = \"" + text + "\"" );

      }
      catch (Exception e)
      {
         e.printStackTrace();
      }
   }
}
