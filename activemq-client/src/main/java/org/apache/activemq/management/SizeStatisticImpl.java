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

import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;

public class SizeStatisticImpl extends StatisticImpl {

    private final LongAdder count = new LongAdder();
    private final AtomicLong maxSize = new AtomicLong();
    private final AtomicLong minSize = new AtomicLong();
    private final LongAdder totalSize = new LongAdder();
    private volatile SizeStatisticImpl parent;

    public SizeStatisticImpl(String name, String description) {
        this(name, "bytes", description);
    }

    public SizeStatisticImpl(SizeStatisticImpl parent, String name, String description) {
        this(name, description);
        this.parent = parent;
    }

    public SizeStatisticImpl(String name, String unit, String description) {
        super(name, unit, description);
    }

    @Override
    public void reset() {
        if (isDoReset()) {
            super.reset();
            count.reset();
            maxSize.set(0);
            minSize.set(0);
            totalSize.reset();
        }
    }

    public long getCount() {
        return count.sum();
    }

    public void addSize(long size) {
        if (!isEnabled()) {
            return;
        }
        count.increment();
        totalSize.add(size);
        updateMinMax(size);
        updateSampleTime();
        SizeStatisticImpl p = parent;
        if (p != null) {
            p.addSize(size);
        }
    }

    private void updateMinMax(long size) {
        maxSize.accumulateAndGet(size, Math::max);
        minSize.updateAndGet(min -> size < min || min == 0 ? size : min);
    }

    /**
     * Reset the total size to the new value
     *
     * @param size
     */
    public void setTotalSize(long size) {
        count.increment();
        totalSize.reset();
        totalSize.add(size);
        updateMinMax(size);
        updateSampleTime();
    }

    /**
     * @return the maximum size of any step
     */
    public long getMaxSize() {
        return maxSize.get();
    }

    /**
     * @return the maximum size of any step
     */
    public void setMaxSize(long size) {
        maxSize.set(size);
    }

    /**
     * @return the minimum size of any step
     */
    public long getMinSize() {
        return minSize.get();
    }

    /**
     * @return the maximum size of any step
     */
    public void setMinSize(long size) {
        minSize.set(size);
    }

    /**
     * @return the total size of all the steps added together
     */
    public long getTotalSize() {
        return totalSize.sum();
    }

    public void setCount(long count) {
        this.count.reset();
        this.count.add(count);
    }

    /**
     * @return the average size calculated by dividing the total size by the
     *         number of counts
     */
    public double getAverageSize() {
        long c = count.sum();
        if (c == 0) {
            return 0;
        }
        double d = totalSize.sum();
        return d / c;
    }

    /**
     * @return the average size calculated by dividing the total size by the
     *         number of counts but excluding the minimum and maximum sizes.
     */
    public double getAverageSizeExcludingMinMax() {
        long c = count.sum();
        if (c <= 2) {
            return 0;
        }
        double d = totalSize.sum() - minSize.get() - maxSize.get();
        return d / (c - 2);
    }

    /**
     * @return the average number of steps per second
     */
    public double getAveragePerSecond() {
        double d = 1000;
        double averageSize = getAverageSize();
        if (averageSize == 0) {
            return 0;
        }
        return d / averageSize;
    }

    /**
     * @return the average number of steps per second excluding the min & max
     *         values
     */
    public double getAveragePerSecondExcludingMinMax() {
        double d = 1000;
        double average = getAverageSizeExcludingMinMax();
        if (average == 0) {
            return 0;
        }
        return d / average;
    }

    public SizeStatisticImpl getParent() {
        return parent;
    }

    public void setParent(SizeStatisticImpl parent) {
        this.parent = parent;
    }

    @Override
    protected void appendFieldDescription(StringBuffer buffer) {
        buffer.append(" count: ");
        buffer.append(Long.toString(count.sum()));
        buffer.append(" maxSize: ");
        buffer.append(Long.toString(maxSize.get()));
        buffer.append(" minSize: ");
        buffer.append(Long.toString(minSize.get()));
        buffer.append(" totalSize: ");
        buffer.append(Long.toString(totalSize.sum()));
        buffer.append(" averageSize: ");
        buffer.append(Double.toString(getAverageSize()));
        buffer.append(" averageTimeExMinMax: ");
        buffer.append(Double.toString(getAveragePerSecondExcludingMinMax()));
        buffer.append(" averagePerSecond: ");
        buffer.append(Double.toString(getAveragePerSecond()));
        buffer.append(" averagePerSecondExMinMax: ");
        buffer.append(Double.toString(getAveragePerSecondExcludingMinMax()));
        super.appendFieldDescription(buffer);
    }
}
