/**
 * 
 * Copyright 2004 Protique Ltd
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
 * 
 **/
package org.activeio.stats;

import edu.emory.mathcs.backport.java.util.concurrent.atomic.AtomicLong;

import javax.management.j2ee.statistics.CountStatistic;

/**
 * Shamelessly taken from the ActiveMQ project ( http://activemq.com )
 * A count statistic implementation
 *
 * @version $Revision: 1.1 $
 */
public class CountStatisticImpl extends StatisticImpl implements CountStatistic {

    private final AtomicLong counter = new AtomicLong(0);
    private CountStatisticImpl parent;

    public CountStatisticImpl(CountStatisticImpl parent, String name, String description) {
        this(name, description);
        this.parent = parent;
    }

    public CountStatisticImpl(String name, String description) {
        this(name, "count", description);
    }

    public CountStatisticImpl(String name, String unit, String description) {
        super(name, unit, description);
    }

    public void reset() {
        super.reset();
        counter.set(0);
    }

    public long getCount() {
        return counter.get();
    }

    public void setCount(long count) {
        counter.set(count);
    }

    public void add(long amount) {
        counter.addAndGet(amount);
        updateSampleTime();
        if (parent != null) {
            parent.add(amount);
        }
    }

    public void increment() {
        counter.incrementAndGet();
        updateSampleTime();
        if (parent != null) {
            parent.increment();
        }
    }

    public void subtract(long amount) {
        counter.addAndGet(-amount);
        updateSampleTime();
        if (parent != null) {
            parent.subtract(amount);
        }
    }
    
    public void decrement() {
        counter.decrementAndGet();
        updateSampleTime();
        if (parent != null) {
            parent.decrement();
        }
    }

    public CountStatisticImpl getParent() {
        return parent;
    }

    public void setParent(CountStatisticImpl parent) {
        this.parent = parent;
    }

    protected void appendFieldDescription(StringBuffer buffer) {
        buffer.append(" count: ");
        buffer.append(Long.toString(counter.get()));
        super.appendFieldDescription(buffer);
    }
}
