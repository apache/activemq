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

package org.apache.activemq.soaktest;


public class PublishThenConsumeSoakTest extends SoakTestSupport{


    public void testPublishThenReceive() throws Exception {
          messageCount = 5000000;

          createProducers();
          int counter = 0;
          for (int i = 0; i < messageCount; i++) {

                for (int k = 0; k < producers.length; k++) {
                    producers[k].sendMessage(payload,"counter",counter);
                    counter++;
                }
          }

          allMessagesList.setAsParent(true);

          createConsumers();
          allMessagesList.waitForMessagesToArrive(messageCount*producers.length);
          allMessagesList.assertMessagesReceived(messageCount*producers.length);
          allMessagesList.assertMessagesReceivedAreInOrder(messageCount*producers.length);

    }


}
