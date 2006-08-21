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
package org.activemq.usecases;

public class QueueSystemTest extends SystemTestSupport {

    /**
     * Unit test for persistent queue messages with the following settings:
     * 1 Producer, 1 Consumer, 1 Subject, 10 Messages
     *
     * @throws Exception
     */
    ///*
    public void testPersistentQueueMessageA() throws Exception{
        SystemTestSupport st = new SystemTestSupport(false,
                                                     true,
                                                     false,
                                                      1,
                                                      1,
                                                      1,
                                                     10,
                                                     "testPersistentQueueMessageA()");
        st.doTest();
    }


    /**
     * Unit test for persistent queue messages with the following settings:
     * 10 Producers, 10 Consumers, 1 Subject, 10 Messages
     *
     * @throws Exception
     */
    public void testPersistentQueueMessageB() throws Exception{
        SystemTestSupport st = new SystemTestSupport(false,
                                                     true,
                                                     false,
                                                     10,
                                                     10,
                                                      1,
                                                     10,
                                                     "testPersistentQueueMessageB()");
        st.doTest();
    }


    /**
     * Unit test for persistent queue messages with the following settings:
     * 10 Producers, 10 Consumers, 10 Subjects, 10 Messages
     *
     * @throws Exception
     */
    public void testPersistentQueueMessageC() throws Exception{
        SystemTestSupport st = new SystemTestSupport(false,
                                                     true,
                                                     false,
                                                     10,
                                                     10,
                                                     10,
                                                     10,
                                                     "testPersistentQueueMessageC()");
        st.doTest();
    }

    /**
     * Unit test for non-persistent queue messages with the following settings:
     * 1 Producer, 1 Consumer, 1 Subject, 10 Messages
     *
     * @throws Exception
     */
    public void testNonPersistentQueueMessageA() throws Exception {
        SystemTestSupport st = new SystemTestSupport(false,
                                                     false,
                                                     false,
                                                      1,
                                                      1,
                                                      1,
                                                     10,
                                                     "testNonPersistentQueueMessageA()");
        st.doTest();
    }

    /**
     * Unit test for non-persistent queue messages with the following settings:
     * 10 Producers, 10 Consumers, 1 Subject, 10 Messages
     *
     * @throws Exception
     */
    public void testNonPersistentQueueMessageB() throws Exception{
        SystemTestSupport st = new SystemTestSupport(false,
                                                     false,
                                                     false,
                                                     10,
                                                     10,
                                                      1,
                                                     10,
                                                     "testNonPersistentQueueMessageB()");
        st.doTest();
    }
    

    /**
     * Unit test for non-persistent queue messages with the following settings:
     * 10 Producers, 10 Consumers, 10 Subjects, 10 Messages
     *
     * @throws Exception
     */
    public void testNonPersistentQueueMessageC() throws Exception{
        SystemTestSupport st = new SystemTestSupport(false,
                                                     false,
                                                     false,
                                                     10,
                                                     10,
                                                     10,
                                                     10,
                                                     "testNonPersistentQueueMessageC()");
        st.doTest();
    }
}
