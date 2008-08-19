/**
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.activemq.soaktest.localBroker;

import java.util.concurrent.atomic.AtomicLong;

import org.apache.camel.Exchange;
import org.apache.camel.Processor;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.InitializingBean;

/**
 * @version $Revision: 1.1 $
 */
public class CounterBean implements InitializingBean, Processor {
    private static final transient Log LOG = LogFactory.getLog(CounterBean.class);
    protected static final long SAMPLES = Integer.parseInt(System.getProperty("SAMPLES", "" + 10000));
    protected static final long SAMPLE_DURATION = Integer.parseInt(System.getProperty("SAMPLES_DURATION", "" + 1000 * 5));
    final AtomicLong counter = new AtomicLong();

    public void process(Exchange exchange) throws Exception {
        counter.incrementAndGet();
    }

    public void afterPropertiesSet() throws Exception {
        Thread thread = new Thread() {
            @Override
            public void run() {
                LOG.info("Starting to monitor consumption");

                for (int i = 0; i < SAMPLES; i++) {
                    try {
                        Thread.sleep(SAMPLE_DURATION);
                    }
                    catch (InterruptedException e) {
                        LOG.warn("Caught: " + e, e);
                    }

                    long count = counter.getAndSet(0);
                    LOG.info("   Total: " + (1000.0 * count / SAMPLE_DURATION) + " messages per second.");
                }
            }
        };
        thread.setDaemon(false);
        thread.start();
    }
}
