/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.activemq.load;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.DeliveryMode;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;

import org.apache.activemq.perf.PerfRate;

/**
 * @version $Revision: 1.3 $
 */
public class LoadClient implements Runnable{
    protected String name;
    protected ConnectionFactory factory;
    protected Connection connection;
    protected Destination startDestination;
    protected Destination nextDestination;
    protected Session session;
    protected MessageConsumer consumer;
    protected MessageProducer producer;
    protected PerfRate rate = new PerfRate();
    protected int deliveryMode = DeliveryMode.PERSISTENT;
    private boolean connectionPerMessage = false;
    private boolean running;
    private int timeout = 10000;
    

    public LoadClient(String name,ConnectionFactory factory) {
       this.name=name;
       this.factory = factory;
    }

   

    public synchronized void start() throws JMSException {
        if (!running) {
            rate.reset();
            running = true;
            if (!connectionPerMessage) {
                connection = factory.createConnection();
                connection.start();
                session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
                consumer = session.createConsumer(this.startDestination);
                producer = session.createProducer(this.nextDestination);
                producer.setDeliveryMode(this.deliveryMode);
                
            }
            
            Thread t = new  Thread(this);
            t.setName(name);
            t.start();
        }
    }

    public void stop() throws JMSException, InterruptedException {
        running = false;
        connection.stop();
    }

    
    public void run() {
        try {
            while (running) {
                String result = consume();
                if (result == null && running) {
                    throw new Exception(name + "Failed to consume ");
                }
                send(result);
                rate.increment();
            }
        } catch (Throwable e) {
            e.printStackTrace();
        } 
    }
    
    protected String consume() throws JMSException {
        Connection con  = null;
        MessageConsumer c = consumer;
        if (connectionPerMessage){
            con = factory.createConnection();
            con.start();
            Session s = con.createSession(false, Session.AUTO_ACKNOWLEDGE);
            c = s.createConsumer(startDestination);
        }
        TextMessage result = (TextMessage) c.receive(timeout);
        if (connectionPerMessage) {
            con.close();
        }
        return result != null ? result.getText() : null;
    }
    
    protected void send(String text) throws JMSException {
        Connection con  = connection;
        MessageProducer p = producer;
        Session s = session;
        if (connectionPerMessage){
            con = factory.createConnection();
            con.start();
            s = con.createSession(false, Session.AUTO_ACKNOWLEDGE);
            p = s.createProducer(nextDestination);
            p.setDeliveryMode(deliveryMode);
        }
        TextMessage message = s.createTextMessage(text);
        p.send(message);
        //System.out.println(name + " SENT " + text + " TO " + nextDestination);
        if (connectionPerMessage) {
            con.close();
        }
    }



    public String getName() {
        return name;
    }



    public void setName(String name) {
        this.name = name;
    }



    public Destination getStartDestination() {
        return startDestination;
    }



    public void setStartDestination(Destination startDestination) {
        this.startDestination = startDestination;
    }



    public Destination getNextDestination() {
        return nextDestination;
    }



    public void setNextDestination(Destination nextDestination) {
        this.nextDestination = nextDestination;
    }



    public int getDeliveryMode() {
        return deliveryMode;
    }



    public void setDeliveryMode(int deliveryMode) {
        this.deliveryMode = deliveryMode;
    }



    public boolean isConnectionPerMessage() {
        return connectionPerMessage;
    }



    public void setConnectionPerMessage(boolean connectionPerMessage) {
        this.connectionPerMessage = connectionPerMessage;
    }



    public int getTimeout() {
        return timeout;
    }



    public void setTimeout(int timeout) {
        this.timeout = timeout;
    }

}
