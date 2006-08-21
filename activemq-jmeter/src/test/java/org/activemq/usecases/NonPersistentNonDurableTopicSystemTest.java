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

public class NonPersistentNonDurableTopicSystemTest extends SystemTestSupport {

    /**
     * Unit test for non-persistent non-durable topic messages with the following settings:
     * 1 Producer, 1 Consumer, 1 Subject, 10 Messages
     *
     * @throws Exception
     */
    public void testNonPersistentNonDurableTopicMessageA() throws Exception{
        SystemTestSupport st = new SystemTestSupport(true,
                                                     false,
                                                     false,
                                                      1,
                                                      1,
                                                      1,
                                                     10,
                                                     "testNonPersistentNonDurableTopicMessageA()");
        st.doTest();
    }

    /**
     * Unit test for non-persistent non-durable topic messages with the following settings:
     * 10 Producers, 10 Consumers, 1 Subject, 10 Messages
     *
     * @throws Exception
     */
    public void testNonPersistentNonDurableTopicMessageB() throws Exception{
        SystemTestSupport st = new SystemTestSupport(true,
                                                     false,
                                                     false,
                                                     10,
                                                     10,
                                                      1,
                                                     10,
                                                     "testNonPersistentNonDurableTopicMessageB()");
        st.doTest();
    }

    /**
     * Unit test for non-persistent non-durable topic messages with the following settings:
     * 10 Producers, 10 Consumers, 10 Subjects, 10 Messages
     *
     * @throws Exception
     */
    public void testNonPersistentNonDurableTopicMessageC() throws Exception{
        SystemTestSupport st = new SystemTestSupport(true,
                                                     false,
                                                     false,
                                                     10,
                                                     10,
                                                     10,
                                                     10,
                                                     "testNonPersistentNonDurableTopicMessageC()");
        st.doTest();
    }
}
