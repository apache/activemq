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


/**
 * A time statistic implementation
 *
 * @version $Revision: 1.2 $
 */
public class TimeStatisticImpl extends StatisticImpl {
    private long count;
    private long maxTime;
    private long minTime;
    private long totalTime;
    private TimeStatisticImpl parent;

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

    public synchronized void reset() {
        if(isDoReset()) {
            super.reset();
            count = 0;
            maxTime = 0;
            minTime = 0;
            totalTime = 0;
        }
    }

    public synchronized long getCount() {
        return count;
    }

    public synchronized void addTime(long time) {
        count++;
        totalTime += time;
        if (time > maxTime) {
            maxTime = time;
        }
        if (time < minTime || minTime == 0) {
            minTime = time;
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
        return maxTime;
    }

    /**
     * @return the minimum time of any step
     */
    public synchronized long getMinTime() {
        return minTime;
    }

    /**
     * @return the total time of all the steps added together
     */
    public synchronized long getTotalTime() {
        return totalTime;
    }

    /**
     * @return the average time calculated by dividing the
     *         total time by the number of counts
     */
    public synchronized double getAverageTime() {
        if (count == 0) {
            return 0;
        }
        double d = totalTime;
        return d / count;
    }


    /**
     * @return the average time calculated by dividing the
     *         total time by the number of counts but excluding the
     *         minimum and maximum times.
     */
    public synchronized double getAverageTimeExcludingMinMax() {
        if (count <= 2) {
            return 0;
        }
        double d = totalTime - minTime - maxTime;
        return d / (count - 2);
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

    protected synchronized void appendFieldDescription(StringBuffer buffer) {
        buffer.append(" count: ");
        buffer.append(Long.toString(count));
        buffer.append(" maxTime: ");
        buffer.append(Long.toString(maxTime));
        buffer.append(" minTime: ");
        buffer.append(Long.toString(minTime));
        buffer.append(" totalTime: ");
        buffer.append(Long.toString(totalTime));
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
