/**
 *
 * Copyright 2004 The Apache Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.activemq.perf;

import javax.jms.BytesMessage;
import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Session;
import junit.framework.TestCase;
import org.activemq.ActiveMQConnectionFactory;
import org.activemq.broker.BrokerService;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
/**
 * @version $Revision: 1.3 $
 */
public class SimpleTopicTest extends TestCase{
    private static final Log log=LogFactory.getLog(SimpleTopicTest.class);
    protected BrokerService broker;
    protected String bindAddress="tcp://localhost:61616";
    protected PerfProducer[] producers;
    protected PerfConsumer[] consumers;
    protected String DESTINATION_NAME=getClass().toString();
    protected int NUMBER_OF_CONSUMERS=1;
    protected int NUMBER_OF_PRODUCERS=1;
    protected BytesMessage payload;
    protected int PAYLOAD_SIZE=1024;
    protected int MESSAGE_COUNT=1000000;
    protected byte[] array=null;

    /**
     * Sets up a test where the producer and consumer have their own connection.
     * 
     * @see junit.framework.TestCase#setUp()
     */
    protected void setUp() throws Exception{
        if(broker==null){
            broker=createBroker();
        }
        array=new byte[PAYLOAD_SIZE];
        for(int i=0;i<array.length;i++){
            array[i]=(byte) i;
        }
        ConnectionFactory fac=createConnectionFactory();
        Connection con=fac.createConnection();
        Session session=con.createSession(false,Session.AUTO_ACKNOWLEDGE);
        payload=session.createBytesMessage();
        payload.writeBytes(array);
        Destination dest=createDestination(session,DESTINATION_NAME);
        con.close();
        producers=new PerfProducer[NUMBER_OF_PRODUCERS];
        consumers=new PerfConsumer[NUMBER_OF_CONSUMERS];
        for(int i=0;i<NUMBER_OF_CONSUMERS;i++){
            consumers[i]=createConsumer(fac,dest,i);
            consumers[i].start();
        }
        for(int i=0;i<NUMBER_OF_PRODUCERS;i++){
            producers[i]=createProducer(fac,dest,i);
            producers[i].start();
        }
        super.setUp();
    }

    protected void tearDown() throws Exception{
        super.tearDown();
        for(int i=0;i<NUMBER_OF_CONSUMERS;i++){
            consumers[i].shutDown();
        }
        for(int i=0;i<NUMBER_OF_PRODUCERS;i++){
            producers[i].shutDown();
        }
        if(broker!=null){
            broker.stop();
            broker=null;
        }
    }

    protected Destination createDestination(Session s,String destinationName) throws JMSException{
        return s.createTopic(destinationName);
    }

    /**
     * Factory method to create a new broker
     * 
     * @throws Exception
     */
    protected BrokerService createBroker() throws Exception{
        BrokerService answer=new BrokerService();
        configureBroker(answer);
        answer.start();
        return answer;
    }

    protected PerfProducer createProducer(ConnectionFactory fac,Destination dest,int number) throws JMSException{
        return new PerfProducer(fac,dest);
    }

    protected PerfConsumer createConsumer(ConnectionFactory fac,Destination dest,int number) throws JMSException{
        return new PerfConsumer(fac,dest);
    }

    protected void configureBroker(BrokerService answer) throws Exception{
        answer.addConnector(bindAddress);
        answer.setDeleteAllMessagesOnStartup(true);
    }

    protected ActiveMQConnectionFactory createConnectionFactory() throws Exception{
        ActiveMQConnectionFactory cf = new ActiveMQConnectionFactory(bindAddress);
//        ActiveMQConnectionFactory cf = new ActiveMQConnectionFactory("vm://localhost?marshal=true");
//        ActiveMQConnectionFactory cf = new ActiveMQConnectionFactory("vm://localhost?marshal=true&wireFormat.cacheEnabled=false");
        cf.setAsyncDispatch(false);
        return cf;
    }

    public void testPerformance() throws JMSException{
        for(int i=0;i<MESSAGE_COUNT;i++){
            if(i%5000==0){
                dumpProducerRate();
                dumpConsumerRate();
            }
            payload.clearBody();
            payload.writeBytes(array);
            for(int k=0;k<producers.length;k++){
                producers[k].sendMessage(payload);
            }
        }
    }

    protected void dumpProducerRate(){
        int count=0;
        int totalCount=0;
        for(int i=0;i<producers.length;i++){
            count+=producers[i].getRate().getRate();
            totalCount+=consumers[i].getRate().getTotalCount();
        }
        count=count/producers.length;
        log.info("Producer rate = "+count+" msg/sec total count = "+totalCount);
        for(int i=0;i<producers.length;i++){
            producers[i].getRate().start();
        }
    }

    protected void dumpConsumerRate(){
        int count=0;
        int totalCount=0;
        for(int i=0;i<consumers.length;i++){
            count+=consumers[i].getRate().getRate();
            totalCount+=consumers[i].getRate().getTotalCount();
        }
        count=count/consumers.length;
        log.info("Consumer rate = "+count+" msg/sec total count = "+totalCount);
        for(int i=0;i<consumers.length;i++){
            consumers[i].getRate().start();
        }
    }
}