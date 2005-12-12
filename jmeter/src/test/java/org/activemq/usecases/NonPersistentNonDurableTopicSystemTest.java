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