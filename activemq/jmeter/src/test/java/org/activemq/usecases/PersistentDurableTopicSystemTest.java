package org.activemq.usecases;

public class PersistentDurableTopicSystemTest extends SystemTestSupport {

    /**
     * Unit test for persistent durable topic messages with the following settings:
     * 1 Producer, 1 Consumer, 1 Subject, 10 Messages
     *
     * @throws Exception
     */
    public void testPersistentDurableTopicMessageA() throws Exception {
        SystemTestSupport st = new SystemTestSupport(true,
                                                     true,
                                                     true,
                                                     1,
                                                     1,
                                                     1,
                                                     10,
                                                     "testPersistentDurableTopicMessageA()");
        st.doTest();
    }

    /**
     * Unit test for persistent durable topic messages with the following settings:
     * 10 Producers, 10 Consumers, 1 Subject, 10 Messages
     *
     * @throws Exception
     */
    public void testPersistentDurableTopicMessageB() throws Exception{
        SystemTestSupport st = new SystemTestSupport(true,
                                                     true,
                                                     true,
                                                     10,
                                                     10,
                                                      1,
                                                     10,
                                                     "testPersistentDurableTopicMessageB()");
        st.doTest();
    }

    /**
     * Unit test for persistent durable topic messages with the following settings:
     * 10 Producers, 10 Consumers, 10 Subjects, 10 Messages
     *
     * @throws Exception
     */
    public void testPersistentDurableTopicMessageC() throws Exception{
        SystemTestSupport st = new SystemTestSupport(true,
                                                     true,
                                                     true,
                                                     10,
                                                     10,
                                                     10,
                                                     10,
                                                     "testPersistentDurableTopicMessageC()");
        st.doTest();
     }
}