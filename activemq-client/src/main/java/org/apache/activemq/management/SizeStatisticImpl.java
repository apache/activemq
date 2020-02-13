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

public class SizeStatisticImpl extends StatisticImpl {

    private long count;
    private volatile long maxSize;
    private volatile long minSize;
    private long totalSize;
    private SizeStatisticImpl parent;

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
    public synchronized void reset() {
        if (isDoReset()) {
            super.reset();
            count = 0;
            maxSize = 0;
            minSize = 0;
            totalSize = 0;
        }
    }

    public synchronized long getCount() {
        return count;
    }

    public synchronized void addSize(long size) {
        count++;
        totalSize += size;
        if (size > maxSize) {
            maxSize = size;
        }
        if (size < minSize || minSize == 0) {
            minSize = size;
        }
        updateSampleTime();
        if (parent != null) {
            parent.addSize(size);
        }
    }

    /**
     * Reset the total size to the new value
     *
     * @param size
     */
    public synchronized void setTotalSize(long size) {
        count++;
        totalSize = size;
        if (size > maxSize) {
            maxSize = size;
        }
        if (size < minSize || minSize == 0) {
            minSize = size;
        }
        updateSampleTime();
    }

    /**
     * @return the maximum size of any step
     */
    public long getMaxSize() {
        return maxSize;
    }

    /**
     * @return the maximum size of any step
     */
    public void setMaxSize(long size) {
        maxSize = size;
    }

    /**
     * @return the minimum size of any step
     */
    public long getMinSize() {
        return minSize;
    }

    /**
     * @return the maximum size of any step
     */
    public void setMinSize(long size) {
        minSize = size;
    }

    /**
     * @return the total size of all the steps added together
     */
    public synchronized long getTotalSize() {
        return totalSize;
    }

    public synchronized void setCount(long count) {
        this.count = count;
    }

    /**
     * @return the average size calculated by dividing the total size by the
     *         number of counts
     */
    public synchronized double getAverageSize() {
        if (count == 0) {
            return 0;
        }
        double d = totalSize;
        return d / count;
    }

    /**
     * @return the average size calculated by dividing the total size by the
     *         number of counts but excluding the minimum and maximum sizes.
     */
    public synchronized double getAverageSizeExcludingMinMax() {
        if (count <= 2) {
            return 0;
        }
        double d = totalSize - minSize - maxSize;
        return d / (count - 2);
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
    protected synchronized void appendFieldDescription(StringBuffer buffer) {
        buffer.append(" count: ");
        buffer.append(Long.toString(count));
        buffer.append(" maxSize: ");
        buffer.append(Long.toString(maxSize));
        buffer.append(" minSize: ");
        buffer.append(Long.toString(minSize));
        buffer.append(" totalSize: ");
        buffer.append(Long.toString(totalSize));
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
