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
package org.apache.activemq.management;

import java.util.concurrent.atomic.LongAdder;

/**
 * A count statistic implementation
 * 
 * 
 */
public class CountStatisticImpl extends StatisticImpl implements CountStatistic {

    private final LongAdder counter = new LongAdder();
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
        if (isDoReset()) {
            super.reset();
            counter.reset();
        }
    }

    public long getCount() {
        return counter.sum();
    }

    public void setCount(long count) {
        if (isEnabled()) {
            counter.reset();
            counter.add(count);
        }
    }

    public void add(long amount) {
        if (isEnabled()) {
            counter.add(amount);
            updateSampleTime();
            if (parent != null) {
                parent.add(amount);
            }
        }
    }

    public void increment() {
        if (isEnabled()) {
            counter.increment();
            updateSampleTime();
            if (parent != null) {
                parent.increment();
            }
        }
    }

    public void subtract(long amount) {
        if (isEnabled()) {
            counter.add(-amount);
            updateSampleTime();
            if (parent != null) {
                parent.subtract(amount);
            }
        }
    }

    public void decrement() {
        if (isEnabled()) {
            counter.decrement();
            updateSampleTime();
            if (parent != null) {
                parent.decrement();
            }
        }
    }

    public CountStatisticImpl getParent() {
        return parent;
    }

    public void setParent(CountStatisticImpl parent) {
        if (this.parent != null) {
            this.parent.subtract(this.getCount());
        }
        this.parent = parent;
    }

    protected void appendFieldDescription(StringBuffer buffer) {
        buffer.append(" count: ");
        buffer.append(Long.toString(counter.sum()));
        super.appendFieldDescription(buffer);
    }

    /**
     * @return the average time period that elapses between counter increments
     *         since the last reset.
     */
    public double getPeriod() {
        double count = counter.sum();
        if (count == 0) {
            return 0;
        }
        double time = System.currentTimeMillis() - getStartTime();
        return time / (count * 1000.0);
    }

    /**
     * @return the number of times per second that the counter is incrementing
     *         since the last reset.
     */
    public double getFrequency() {
        double count = counter.sum();
        double time = System.currentTimeMillis() - getStartTime();
        return count * 1000.0 / time;
    }

}
