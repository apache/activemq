package org.activemq.usecases;

import junit.framework.TestCase;

import javax.jms.*;
import java.util.*;
import java.net.URI;

import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.command.ActiveMQMessage;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.BrokerFactory;
import org.apache.activemq.broker.TransportConnector;

import org.apache.jorphan.logging.LoggingManager;
import org.apache.log.Logger;

import EDU.oswego.cs.dl.util.concurrent.ConcurrentHashMap;

public class SystemTestSupport extends TestCase implements MessageListener {
    public static final String ACTIVEMQ_SERVER = "ActiveMQ Server";
    public static final boolean TRANSACTED_FALSE = false;
    public static final String TOOL_DEFAULT = "TOOL.DEFAULT";
    public static final String LAST_MESSAGE = "#LAST";

    public String userName;
    public String password;

    private static int msgCounter = 0;
    public static Map ProducerMap = Collections.synchronizedMap(new HashMap());

    private int producerCount = 0;
    private int consumerCount = 0;
    private int subjectCount = 0;
    private int messageCount = 0;
    private boolean isPersistent = true;
    private boolean isDurable = true;
    private boolean isTopic = true;
    private boolean testStillRunning = true;

    protected BrokerService broker;

    Map consumerMap = new ConcurrentHashMap();
    Map prodNameMap = new TreeMap();
    Map prodMsgMap = new TreeMap();

    /**
     * Default constructor
     */
    protected SystemTestSupport(){
        super();
        ProducerMap.clear();
        msgCounter = 0;
        testStillRunning = true;
    }

    /**
     * Constructor
     *
     * @param isTopic - true when topic, false when queue.
     * @param isPersistent - true when the delivery mode is persistent.
     * @param isDurable - true when the suscriber is durable(For topic only).
     * @param producerCount - number of producers.
     * @param consumerCount - number of consumers.
     * @param subjectCount - number of destinations.
     * @param messageCount - number of messages to be delivered.
     * @param testTitle - test title/name.
     *
     */
    protected SystemTestSupport(boolean isTopic,
                         boolean isPersistent,
                         boolean isDurable,
                         int producerCount,
                         int consumerCount,
                         int subjectCount,
                         int messageCount,
                         String testTitle){
        super();
        this.isTopic = isTopic;
        this.isPersistent = isPersistent;
        this.isDurable = isDurable;
        this.producerCount = producerCount;
        this.consumerCount = consumerCount;
        this.subjectCount = subjectCount;
        this.messageCount = messageCount;
        this.testParameterSettings(testTitle);

        ProducerMap.clear();
        consumerMap.clear();
        prodNameMap.clear();
        prodMsgMap.clear();
        msgCounter = 0;
        testStillRunning = true;
    }

    /****************************************************
     *
     *  Producer section
     *
     ****************************************************/

    /**
     * Creates the message producer threads.
     */
    protected void publish() throws JMSException {
        String subjects[] = getSubjects();

        for (int i = 0; i < producerCount; i++) {
            final int x = i;
            final String subject = subjects[i % subjects.length];

            Thread thread = new Thread() {
                public void run() {
                    try {
                        publish(x, subject);

                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            };

            thread.start();
        }
    }

    /**
     * Creates the producer and send the messages.
     *
     * @param x - producer number.
     * @param subject -  the destination where the messages will be sent.
     */
    protected void publish(int x, String subject) throws Exception {
        ConnectionFactory factory = createConnectionFactory();
        ActiveMQConnection connection = (ActiveMQConnection) factory.createConnection(userName, password);
        connection.start();

        String messageBody;

        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        Destination destination = createDestination(session, subject);
        MessageProducer publisher = session.createProducer(destination);

        if (isPersistent) {
            publisher.setDeliveryMode(DeliveryMode.PERSISTENT);
        } else {
            publisher.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
        }

        StringBuffer sb = new StringBuffer();

        //Sending messages
        for (int i = 1; i <= messageCount; ++i) {
            if (messageCount != i)
                messageBody = "#BODY";
            else
                messageBody = LAST_MESSAGE;

            sb.append("PROD");
            sb.append(x);
            sb.append(messageBody);
            sb.append("#");
            sb.append(i);

            TextMessage message = session.createTextMessage(sb.toString());
            publisher.send(message);

            sb.delete(0, sb.length());
        }

        publisher.close();
        session.close();
        connection.stop();
        connection = null;
    }

    /****************************************************
     *
     *  Consumer section
     *
     ****************************************************/

    /**
     * Generates the topic/queue destinations.
     *
     * @return String[] - topic/queue destination name.
     */
    protected String[] getSubjects() {
        //Create the subjects.
        String[] subjects = new String[subjectCount];

        //Appended to the subject to determine if its a queue or topic.
        String prefix = null;
        if (this.isTopic) {
            prefix = ".TOPIC";
        } else {
            prefix = ".QUEUE";
        }

        for (int i = 0; i < subjects.length; i++) {
            subjects[i] = TOOL_DEFAULT + prefix + i;
        }

        return subjects;
    }

    /**
     * Suscribes the consumers to the topic/queue destinations.
     */
    protected void subscribe() throws Exception {
        String subjects[] = getSubjects();

        for (int i = 0; i < consumerCount; i++) {
            String subject = subjects[i % subjectCount];
            subscribe(subject);
        }
    }

    /**
     * Suscribes the consumer to the topic/queue specified by the subject.
     *
     * @param subject - the Destination where the consumer waits upon for messages.
     */
    protected void subscribe(String subject) throws Exception {
        ConnectionFactory factory = createConnectionFactory();
        ActiveMQConnection connection = (ActiveMQConnection) factory.createConnection(userName, password);
        connection.start();

        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        Destination destination = createDestination(session, subject);

        MessageConsumer consumer = session.createConsumer(destination);
        consumer.setMessageListener(this);
        connection.stop();
    }

    /**
     * Processes the received message.
     *
     * @param message - message received by the listener.
     */
    public void onMessage(Message message) {
        try {
            ActiveMQMessage amsg = (ActiveMQMessage) message;
            TextMessage textMessage = (TextMessage) message;

            StringBuffer sb = new StringBuffer();
            sb.append(textMessage.getText());
            sb.append("#");
            sb.append(amsg.getJMSMessageID());

            msgCounter++;
            String strMsgCounter = String.valueOf(msgCounter);
            ProducerMap.put(strMsgCounter, sb.toString());

        } catch (JMSException e) {
            System.out.println("Unable to force deserialize the content " + e);
        }
    }

    /**
     * Validates the result of the producing and consumption of messages by the server.
     * It checks for duplicate messages, message count and order.
     */
    protected synchronized void timerLoop() {
        System.out.println("MessagingSystemTest.timerLoop() * started * ");
        Map ProducerTextMap = new HashMap();
        Map currentProducerMap = null;
        String producerName = null;
        String msgBody = null;
        String consumerName = null;
        String ProdSequenceNo = null;
        String mapKey = null;
        int expectedNoOfMessages = messageCount;
        boolean dowhile = true;

        while (dowhile) {
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            //Retrieve the map containing the received messages data
            currentProducerMap = resetProducerMap();

            if (currentProducerMap.size() == 0) {
                dowhile = false;
            }

            //Put the map values to another map for parsing.
            for (int i = 1; i <= currentProducerMap.size(); i++) {
                String ProdMsg = (String) currentProducerMap.get(String.valueOf(i));
                producerName = ProdMsg.substring(0, ProdMsg.indexOf("#"));
                msgBody = ProdMsg.substring(ProdMsg.indexOf("#") + 1, ProdMsg.lastIndexOf("#"));
                ProdSequenceNo = ProdMsg.substring(ProdMsg.indexOf("#", ProdMsg.indexOf("#", ProdMsg.indexOf("#")+1)) + 1, ProdMsg.lastIndexOf("#"));
                consumerName = ProdMsg.substring(ProdMsg.lastIndexOf("#"), ProdMsg.length());

                if (isTopic) {
                    mapKey = consumerName + producerName;
                } else {
                    mapKey = producerName;
                }

                if (ProducerTextMap.containsKey(mapKey)) {
                    //Increment the counter value
                    Integer value = (Integer) ProducerTextMap.get(mapKey);
                    ProducerTextMap.put(mapKey, new Integer(value.intValue() + 1));
                } else {
                    //Put the Producer Name in the map
                    ProducerTextMap.put(mapKey, new Integer(1));
                }

                Integer messageCounter = (Integer) ProducerTextMap.get(mapKey);
                Integer ProducerSeqID = Integer.valueOf(ProdSequenceNo);

                if (isTopic) {
                    // Check for duplicate message.
                    if (messageCounter.intValue() > expectedNoOfMessages) {
                        assertTrue("Should not have received duplicate messages!", messageCounter.intValue() <= expectedNoOfMessages);
                        break;
                    } else if (LAST_MESSAGE.equals(msgBody)) {
                        System.out.println("entered MsgBody.equals(LAST_MESSAGE)..." + mapKey +
                                       " " + messageCounter.intValue() +"=" + expectedNoOfMessages);

                        // Validates that the messages received is equal to the number
                        // of expected messages
                        if (messageCounter.intValue() != expectedNoOfMessages) {
                            System.out.println("entered messageCounter.intValue() != expectedNoOfMessages...");

                            // Checks for message order.
                            assertTrue("Should have received messages in order!", messageCounter.intValue() == expectedNoOfMessages);
                            if (messageCounter.intValue() != expectedNoOfMessages) {
                                System.out.println("Should have received messages in order!");
                            }
                            break;
                        } else if (currentProducerMap.size() == i) {
                            System.out.println("MessagingSystemTest.timerLoop() says system_test_pass!!!... ");
                            break;
                        }
                    }

                } else {
                    //Create map for each consumer
                    for (int j = 0 ; j < consumerCount ; j++) {
                        if (!consumerMap.containsKey(new String(consumerName))) {
                            consumerMap.put(new String(consumerName), new LinkedHashMap());
                        }
                    }

                    //create Producer Name Map
                    if (!prodNameMap.containsKey(producerName)) {
                        prodNameMap.put(producerName, (null));
                    }

                    //Get the current size of consumer
                    int seqVal = 0;
                    Object[] cObj = consumerMap.keySet().toArray();
                    for (int k = 0; k < cObj.length; k++) {
                        String cMapKey = (String)cObj[k];
                        Map cMapVal = (Map)consumerMap.get(cObj[k]);
                        if (cMapKey.equals(consumerName)) {
                            seqVal = cMapVal.size();
                            break;
                        }
                    }

                    //Put object to its designated consumer map
                    Object[] consumerObj = consumerMap.keySet().toArray();
                    for (int j = 0; j < consumerObj.length; j++) {
                        String cMapKey = (String)consumerObj[j];
                        Map cMapVal = (LinkedHashMap)consumerMap.get(consumerObj[j]);
                        if (cMapKey.equals(consumerName)) {
                            cMapVal.put(new Integer(seqVal), (producerName + "/" + ProducerSeqID));
                        }
                    }
                }

                // Add data to table row
                if (!isTopic) {
                    String msgKey = consumerName + "#" + producerName + "#" + String.valueOf(ProdSequenceNo);
                    String msgVal = String.valueOf(messageCounter) + "#" + msgBody;

                    if (!prodMsgMap.containsKey(msgKey)) {
                        prodMsgMap.put((msgKey), (msgVal));
                    }
                }
            }
        }

        if (!isTopic) {
            //Validate message sequence
            boolean isMsgNotOrdered = validateMsg(prodNameMap, consumerMap);
            assertFalse("Should have received messages in order!", isMsgNotOrdered);
        }

        testStillRunning = false;
        System.out.println("MessagingSystemTest.timerLoop() * ended * ");
    }

    /**
     * Returns the message entries and clears the map for another set
     * of messages to be processed.
     *
     * @return Map - messages to be processed.
     */
    protected synchronized Map resetProducerMap() {
        Map copy = Collections.synchronizedMap(new HashMap(ProducerMap));
        ProducerMap.clear();
        msgCounter = 0;

        return copy;
    }

    /****************************************************
     *
     *  Utility section
     *
     ****************************************************/

    /**
     * Creates the session destination.
     *
     * @param session - connection session.
     * @param subject - destination name.
     * @return Destination - session destination.
     */
    protected ActiveMQDestination createDestination(Session session, String subject)
                                                    throws JMSException {
        if (isTopic) {
            return (ActiveMQDestination) session.createQueue(subject);
        } else {
            return (ActiveMQDestination) session.createTopic(subject);
        }
    }

    protected ConnectionFactory createConnectionFactory() throws Exception {
        return new ActiveMQConnectionFactory("vm://localhost?broker.persistent=false");
    }

    /****************************************************
     *
     *  Unit test section
     *
     ****************************************************/

    /**
     * Executes the unit test by running the producers and consumers.
     * It checks for duplicate messages, message count and order.
     */
    protected void doTest() throws Exception {
        System.out.println("MessagingSystemTest.doTest() * start *");

        //Set up the consumers
        subscribe();

        System.out.println("MessagingSystemTest.doTest() after suscribe()...");

        //Set up the producers
        publish();

        System.out.println("MessagingSystemTest.doTest() after publish()...");

        //Run the test
        Thread timer = new Thread() {
            public void run() {
                timerLoop();
            }
        };
        timer.setPriority(Thread.MIN_PRIORITY);
        timer.start();

        while (testStillRunning) {
            try {
                if (Thread.currentThread() == timer) {
                    Thread.sleep(1000);
                }
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        System.out.println("MessagingSystemTest.doTest() * end *");
    }

    /**
     * Validates the messages received.
     *
     * @param prodNameMap
     * @param cMap
     * @return
     */
    protected boolean validateMsg(Map prodNameMap, Map cMap) {
        boolean ret = false;
        Object[]  cObj = cMap.keySet().toArray();
        for (int j = 0; j < cObj.length; j++) {
            Map childMap = (Map)cMap.get(cObj[j]);

            Object[]  nameObj = prodNameMap.keySet().toArray();
            for (int i = 0; i < nameObj.length; i++) {
                String prodName = (String)nameObj[i];
                String tempProdHolder = null;
                String tempProdIDHolder = null;

                Object[] childObj = childMap.keySet().toArray();
                for (int k = 0; k < childObj.length; k++) {
                    Integer childMapKey = (Integer)childObj[k];
                    String childMapVal = (String)childMap.get(childObj[k]);
                    String prodVal = childMapVal.substring(0, childMapVal.indexOf("/"));
                    String prodIDVal = childMapVal.substring(childMapVal.indexOf("/")+1, childMapVal.length());

                    if (prodVal.equals(prodName)) {
                        if (tempProdHolder == null) {
                            tempProdHolder = prodVal;
                            tempProdIDHolder = prodIDVal;
                            continue;
                        }
                        if (Integer.parseInt(prodIDVal) > Integer.parseInt(tempProdIDHolder)) {
                            tempProdHolder = prodVal;
                            tempProdIDHolder = prodIDVal;
                        } else {
                            ret = true;
                            break;
                        }
                    } else {
                        continue;
                    }
                }
            }
        }
        return ret;
    }

    /**
     * Prints the test settings.
     *
     * @param strTestTitle - unit test name.
     */
    public void testParameterSettings(String strTestTitle) {
        System.out.println(strTestTitle);
        System.out.println("============================================================");
        System.out.println("Test settings:");
        System.out.println("isTopic=" + new Boolean(isTopic).toString());
        System.out.println("isPersistent=" + new Boolean(isPersistent).toString());
        System.out.println("isDurable=" + new Boolean(isDurable).toString());
        System.out.println("producerCount=" + producerCount);
        System.out.println("consumerCount=" + consumerCount);
        System.out.println("subjectCount=" + subjectCount);
        System.out.println("messageCount=" + messageCount);
        System.out.println("");
    }
}
