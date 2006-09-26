/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.activemq.usecases.rest;

import junit.framework.*;

public class RESTLoadTest extends TestCase {
    public volatile int counter = 0;

    public void testREST() {
        int HowManyMessages = 60000;
        TestConsumerThread consumer = new TestConsumerThread(this, HowManyMessages);
        TestProducerThread producer = new TestProducerThread(this, HowManyMessages);
        consumer.start();
        producer.start();
        while (counter > 0) {
        }
        System.out.println("Produced:" + producer.success + " Consumed:" + consumer.success);
    }

    public static void main(String args[]) {
        junit.textui.TestRunner.run(new TestSuite(RESTLoadTest.class));
    }
}
 
