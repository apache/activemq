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
package org.apache.activemq.broker;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class ProducerBrokerExchangeTest {

    @Test
    public void testGetPercentageBlockedHandlesDivideByZero(){
        ProducerBrokerExchange producerBrokerExchange = new ProducerBrokerExchange();
        producerBrokerExchange.getPercentageBlocked();
    }

    @Test
    public void testGetPercentageBlockedNonZero(){
        ProducerBrokerExchange producerBrokerExchange = new ProducerBrokerExchange();
        producerBrokerExchange.blockingOnFlowControl(true);
        producerBrokerExchange.incrementSend();
        assertEquals(100.0, producerBrokerExchange.getPercentageBlocked(), 0);
    }
}
