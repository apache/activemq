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

import java.util.concurrent.atomic.LongAccumulator;
import java.util.concurrent.atomic.LongAdder;


/**
 * A time statistic implementation
 *
 * 
 */
public class TimeStatisticImpl extends StatisticImpl {
    private final LongAdder count = new LongAdder();
    private final LongAccumulator maxTime = new LongAccumulator(Math::max, 0L);
    private final LongAccumulator minTime = new LongAccumulator(Math::min, Long.MAX_VALUE);
    private final LongAdder totalTime = new LongAdder();
    private volatile TimeStatisticImpl parent;

    public TimeStatisticImpl(String name, String description) {
        this(name, "millis", description);
    }

    public TimeStatisticImpl(TimeStatisticImpl parent, String name, String description) {
        this(name, description);
        this.parent = parent;
    }

    public TimeStatisticImpl(String name, String unit, String description) {
        super(name, unit, description);
    }

    public void reset() {
        if(isDoReset()) {
            super.reset();
            count.reset();
            maxTime.reset();
            minTime.reset();
            totalTime.reset();
        }
    }

    public long getCount() {
        return count.sum();
    }

    public void addTime(long time) {
        count.increment();
        totalTime.add(time);
        maxTime.accumulate(time);
        if (time != 0) {
            minTime.accumulate(time);
        }
        updateSampleTime();
        if (parent != null) {
            parent.addTime(time);
        }
    }

    /**
     * @return the maximum time of any step
     */
    public long getMaxTime() {
        return maxTime.get();
    }

    /**
     * @return the minimum time of any step
     */
    public long getMinTime() {
        long min = minTime.get();
        return min == Long.MAX_VALUE ? 0 : min;
    }

    /**
     * @return the total time of all the steps added together
     */
    public long getTotalTime() {
        return totalTime.sum();
    }

    /**
     * @return the average time calculated by dividing the
     *         total time by the number of counts
     */
    public double getAverageTime() {
        long c = getCount();
        if (c == 0) {
            return 0;
        }
        double d = getTotalTime();
        return d / c;
    }


    /**
     * @return the average time calculated by dividing the
     *         total time by the number of counts but excluding the
     *         minimum and maximum times.
     */
    public double getAverageTimeExcludingMinMax() {
        long c = getCount();
        if (c <= 2) {
            return 0;
        }
        double d = getTotalTime() - getMinTime() - getMaxTime();
        return d / (c - 2);
    }


    /**
     * @return the average number of steps per second
     */
    public double getAveragePerSecond() {
        double d = 1000;
        double averageTime = getAverageTime();
        if (averageTime == 0) {
            return 0;
        }
        return d / averageTime;
    }

    /**
     * @return the average number of steps per second excluding the min & max values
     */
    public double getAveragePerSecondExcludingMinMax() {
        double d = 1000;
        double average = getAverageTimeExcludingMinMax();
        if (average == 0) {
            return 0;
        }
        return d / average;
    }

    public TimeStatisticImpl getParent() {
        return parent;
    }

    public void setParent(TimeStatisticImpl parent) {
        this.parent = parent;
    }

    protected void appendFieldDescription(StringBuffer buffer) {
        buffer.append(" count: ");
        buffer.append(Long.toString(getCount()));
        buffer.append(" maxTime: ");
        buffer.append(Long.toString(getMaxTime()));
        buffer.append(" minTime: ");
        buffer.append(Long.toString(getMinTime()));
        buffer.append(" totalTime: ");
        buffer.append(Long.toString(getTotalTime()));
        buffer.append(" averageTime: ");
        buffer.append(Double.toString(getAverageTime()));
        buffer.append(" averageTimeExMinMax: ");
        buffer.append(Double.toString(getAverageTimeExcludingMinMax()));
        buffer.append(" averagePerSecond: ");
        buffer.append(Double.toString(getAveragePerSecond()));
        buffer.append(" averagePerSecondExMinMax: ");
        buffer.append(Double.toString(getAveragePerSecondExcludingMinMax()));
        super.appendFieldDescription(buffer);
    }

}
