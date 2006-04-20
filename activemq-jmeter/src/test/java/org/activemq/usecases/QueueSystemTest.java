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