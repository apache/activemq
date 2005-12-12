package org.activemq.usecases;

public class PersistentNonDurableTopicSystemTest extends SystemTestSupport {

    /**
     * Unit test for persistent non-durable topic messages with the following settings:
     * 1 Producer, 1 Consumer, 1 Subject, 10 Messages
     *
     * @throws Exception
     */
    public void testPersistentNonDurableTopicMessageA() throws Exception{
        SystemTestSupport st = new SystemTestSupport(true,
                                                     true,
                                                     false,
                                                      1,
                                                      1,
                                                      1,
                                                     10,
                                                     "testPersistentNonDurableTopicMessageA()");
        st.doTest();
    }

    /**
     * Unit test for persistent non-durable topic messages with the following settings:
     * 10 Producers, 10 Consumers, 1 Subject, 10 Messages
     *
     * @throws Exception
     */
    public void testPersistentNonDurableTopicMessageB() throws Exception{
        SystemTestSupport st = new SystemTestSupport(true,
                                                     true,
                                                     false,
                                                     10,
                                                     10,
                                                      1,
                                                     10,
                                                     "testPersistentNonDurableTopicMessageB()");
        st.doTest();
    }

    /**
     * Unit test for persistent non-durable topic messages with the following settings:
     * 10 Producers, 10 Consumers, 10 Subjects, 10 Messages
     *
     * @throws Exception
     */
    public void testPersistentNonDurableTopicMessageC() throws Exception{
        SystemTestSupport st = new SystemTestSupport(true,
                                                     true,
                                                     false,
                                                     10,
                                                     10,
                                                     10,
                                                     10,
                                                     "testPersistentNonDurableTopicMessageC()");
        st.doTest();
    }
}