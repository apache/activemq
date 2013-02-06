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

import javax.jms.ConnectionFactory;
import javax.jms.Destination;
import javax.jms.JMSException;

/**
 *
 */
public class LoadController extends LoadClient{

    private int numberOfBatches=1;
    private int batchSize =1000;
    private int count;
    private final CountDownLatch stopped = new CountDownLatch(1);

    public LoadController(String name,ConnectionFactory factory) {
       super(name,factory);
    }


    public int awaitTestComplete() throws InterruptedException {
        stopped.await(60*5,TimeUnit.SECONDS);
        return count;
    }

    @Override
    public void stop() throws JMSException, InterruptedException {
        running = false;
        stopped.countDown();
        if (connection != null) {
            this.connection.stop();
        }
    }

    @Override
    public void run() {
        try {
            for (int i = 0; i < numberOfBatches; i++) {
                for (int j = 0; j < batchSize; j++) {
                    String payLoad = "batch[" + i + "]no:" + j;
                    send(payLoad);
                }
                for (int j = 0; j < batchSize; j++) {
                    String result = consume();
                    if (result != null) {
                        count++;
                    rate.increment();
                    }
                }
            }
        } catch (Throwable e) {
            e.printStackTrace();
        } finally {
            stopped.countDown();
        }
    }

    public int getNumberOfBatches() {
        return numberOfBatches;
    }

    public void setNumberOfBatches(int numberOfBatches) {
        this.numberOfBatches = numberOfBatches;
    }

    public int getBatchSize() {
        return batchSize;
    }

    public void setBatchSize(int batchSize) {
        this.batchSize = batchSize;
    }

    @Override
    protected Destination getSendDestination() {
        return startDestination;
    }

    @Override
    protected Destination getConsumeDestination() {
        return nextDestination;
    }
}
