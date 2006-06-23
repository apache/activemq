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

import org.apache.activemq.tool.sampler.MeasurableClient;

import javax.jms.ConnectionFactory;

import edu.emory.mathcs.backport.java.util.concurrent.atomic.AtomicLong;

public abstract class AbstractJmsMeasurableClient extends AbstractJmsClient implements MeasurableClient {
    protected AtomicLong throughput = new AtomicLong(0);

    public AbstractJmsMeasurableClient( ConnectionFactory factory) {
        super( factory);
    }

    public void reset() {
        setThroughput(0);
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
}
