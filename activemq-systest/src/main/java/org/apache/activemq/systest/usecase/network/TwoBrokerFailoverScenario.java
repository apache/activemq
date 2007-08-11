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
package org.apache.activemq.systest.usecase.network;

import org.apache.activemq.systest.BrokerAgent;
import org.apache.activemq.systest.ConsumerAgent;
import org.apache.activemq.systest.MessageList;
import org.apache.activemq.systest.ProducerAgent;
import org.apache.activemq.systest.QueueOnlyScenario;

/**
 * 
 * @version $Revision: 1.1 $
 */
public class TwoBrokerFailoverScenario extends TwoBrokerNetworkScenario implements QueueOnlyScenario {

    public TwoBrokerFailoverScenario(BrokerAgent brokera, BrokerAgent brokerb, ProducerAgent producer, ConsumerAgent consumer, MessageList list) {
        super(brokera, brokerb, producer, consumer, list);
    }

    public void run() throws Exception {
        producer.sendMessages(messageList, 30);
        consumer.waitUntilConsumed(messageList, 20);

        consumer.stop();
        brokerB.kill();
        consumer.connectTo(brokerA);
        consumer.start();

        producer.sendMessages(messageList);
        
        consumer.assertConsumed(messageList);
    }

}
