/**
 *
 * Copyright 2005-2006 The Apache Software Foundation
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
package org.apache.activemq.tool;

import edu.emory.mathcs.backport.java.util.concurrent.atomic.AtomicLong;

import javax.jms.JMSException;

public class JmsPerformanceSupport extends JmsClientSupport implements PerfMeasurable {

    private static int clientCounter;
    
    protected AtomicLong throughput = new AtomicLong(0);
    protected PerfEventListener listener = null;
    private int clientNumber;

    public void reset() {
        setThroughput(0);
    }
    
    public synchronized int getClientNumber() {
        if (clientNumber == 0) {
            clientNumber = incrementClientCounter();
        }
        return clientNumber;
    }

    public String getClientName() {
        try {
            return getConnection().getClientID();
        } catch (JMSException e) {
            return "";
        }
    }

    public long getThroughput() {
        return throughput.get();
    }

    public void setThroughput(long val) {
        throughput.set(val);
    }

    public void incThroughput() {
        throughput.incrementAndGet();
    }

    public void incThroughput(long val) {
        throughput.addAndGet(val);
    }

    public void setPerfEventListener(PerfEventListener listener) {
        this.listener = listener;
    }

    public PerfEventListener getPerfEventListener() {
        return listener;
    }

    protected static synchronized int incrementClientCounter() {
        return ++clientCounter;
    }
}
