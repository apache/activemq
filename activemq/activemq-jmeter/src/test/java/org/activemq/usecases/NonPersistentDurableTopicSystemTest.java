package org.activemq.usecases;

public class NonPersistentDurableTopicSystemTest extends SystemTestSupport {

    /**
     * Unit test for non-persistent durable topic messages with the following settings:
     * 1 Producer, 1 Consumer, 1 Subject, 10 Messages
     *
     * @throws Exception
     */
    public void testNonPersistentDurableTopicMessageA() throws Exception{
        SystemTestSupport st = new SystemTestSupport(true,
                                                    false,
                                                    true,
                                                     1,
                                                     1,
                                                     1,
                                                    10,
                                                    "testNonPersistentDurableTopicMessageA()");
        st.doTest();
    }

    /**
     * Unit test for non-persistent durable topic messages with the following settings:
     * 10 Producers, 10 Consumers, 1 Subject, 10 Messages
     *
     * @throws Exception
     */
    public void testNonPersistentDurableTopicMessageB() throws Exception{
        SystemTestSupport st = new SystemTestSupport(true,
                                                     false,
                                                     true,
                                                     10,
                                                     10,
                                                     1,
                                                     10,
                                                     "testNonPersistentDurableTopicMessageB()");
        st.doTest();
    }

    /**
     * Unit test for non-persistent durable topic messages with the following settings:
     * 10 Producers, 10 Consumers, 10 Subjects, 10 Messages
     *
     * @throws Exception
     */
    public void testNonPersistentDurableTopicMessageC() throws Exception{
        SystemTestSupport st = new SystemTestSupport(true,
                                                     false,
                                                     true,
                                                     10,
                                                     10,
                                                     10,
                                                     10,
                                                     "testNonPersistentDurableTopicMessageC()");
        st.doTest();
    }
}