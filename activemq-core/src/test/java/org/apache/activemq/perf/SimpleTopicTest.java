/**
 * 
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. See the NOTICE
 * file distributed with this work for additional information regarding copyright ownership. The ASF licenses this file
 * to You under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
 * License. You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */

package org.apache.activemq.perf;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Session;
import junit.framework.TestCase;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * @version $Revision: 1.3 $
 */
public class SimpleTopicTest extends TestCase{

    private final Log log=LogFactory.getLog(getClass());
    protected BrokerService broker;
    // protected String
    // bindAddress="tcp://localhost:61616?wireFormat.cacheEnabled=true&wireFormat.tightEncodingEnabled=true&jms.useAsyncSend=false";
    //protected String bindAddress="tcp://localhost:61616";
    protected String bindAddress="tcp://localhost:61616";
    //protected String bindAddress="vm://localhost?marshal=true";
    //protected String bindAddress="vm://localhost";
    protected PerfProducer[] producers;
    protected PerfConsumer[] consumers;
    protected String DESTINATION_NAME=getClass().getName();
    protected int SAMPLE_COUNT=10;
    protected long SAMPLE_INTERVAL=1000;
    protected int NUMBER_OF_CONSUMERS=10;
    protected int NUMBER_OF_PRODUCERS=1;
    protected int PAYLOAD_SIZE=1024;
    protected byte[] array=null;
    protected ConnectionFactory factory;
    protected Destination destination;
    protected long CONSUMER_SLEEP_DURATION=0;

    /**
     * Sets up a test where the producer and consumer have their own connection.
     * 
     * @see junit.framework.TestCase#setUp()
     */
    protected void setUp() throws Exception{
        if(broker==null){
            broker=createBroker();
        }
        factory=createConnectionFactory();
        Connection con=factory.createConnection();
        Session session=con.createSession(false,Session.AUTO_ACKNOWLEDGE);
        destination=createDestination(session,DESTINATION_NAME);
        log.info("Testing against destination: "+destination);
        log.info("Running "+NUMBER_OF_PRODUCERS+" producer(s) and "+NUMBER_OF_CONSUMERS+" consumer(s)");
        con.close();
        producers=new PerfProducer[NUMBER_OF_PRODUCERS];
        consumers=new PerfConsumer[NUMBER_OF_CONSUMERS];
        for(int i=0;i<NUMBER_OF_CONSUMERS;i++){
            consumers[i]=createConsumer(factory,destination,i);
            consumers[i].setSleepDuration(CONSUMER_SLEEP_DURATION);
        }
        for(int i=0;i<NUMBER_OF_PRODUCERS;i++){
            array=new byte[PAYLOAD_SIZE];
            for(int j=i;j<array.length;j++){
                array[j]=(byte)j;
            }
            producers[i]=createProducer(factory,destination,i,array);
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

    protected PerfProducer createProducer(ConnectionFactory fac,Destination dest,int number,byte[] payload)
            throws JMSException{
        return new PerfProducer(fac,dest,payload);
    }

    protected PerfConsumer createConsumer(ConnectionFactory fac,Destination dest,int number) throws JMSException{
        return new PerfConsumer(fac,dest);
    }

    protected void configureBroker(BrokerService answer) throws Exception{
        answer.addConnector(bindAddress);
        answer.setDeleteAllMessagesOnStartup(true);
    }

    protected ActiveMQConnectionFactory createConnectionFactory() throws Exception{
        return new ActiveMQConnectionFactory(bindAddress);
    }

    public void testPerformance() throws JMSException,InterruptedException{
        for(int i=0;i<NUMBER_OF_CONSUMERS;i++){
            consumers[i].start();
        }
        for(int i=0;i<NUMBER_OF_PRODUCERS;i++){
            producers[i].start();
        }
        log.info("Sampling performance "+SAMPLE_COUNT+" times at a "+SAMPLE_INTERVAL+" ms interval.");
        for(int i=0;i<SAMPLE_COUNT;i++){
            Thread.sleep(SAMPLE_INTERVAL);
            dumpProducerRate();
            dumpConsumerRate();
        }
        for(int i=0;i<NUMBER_OF_PRODUCERS;i++){
            producers[i].stop();
        }
        for(int i=0;i<NUMBER_OF_CONSUMERS;i++){
            consumers[i].stop();
        }
    }

    protected void dumpProducerRate(){
        int totalRate=0;
        int totalCount=0;
        for(int i=0;i<producers.length;i++){
            PerfRate rate=producers[i].getRate().cloneAndReset();
            totalRate+=rate.getRate();
            totalCount+=rate.getTotalCount();
        }
        int avgRate=totalRate/producers.length;
        log.info("Avg producer rate = "+avgRate+" msg/sec | Total rate = "+totalRate+", sent = "+totalCount);
    }

    protected void dumpConsumerRate(){
        int totalRate=0;
        int totalCount=0;
        for(int i=0;i<consumers.length;i++){
            PerfRate rate=consumers[i].getRate().cloneAndReset();
            totalRate+=rate.getRate();
            totalCount+=rate.getTotalCount();
        }
        if(consumers!=null&&consumers.length>0){
            int avgRate=totalRate/consumers.length;
            log.info("Avg consumer rate = "+avgRate+" msg/sec | Total rate = "+totalRate+", received = "+totalCount);
        }
    }
}
