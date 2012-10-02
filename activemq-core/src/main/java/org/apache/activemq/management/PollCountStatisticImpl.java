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

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import javax.management.j2ee.statistics.CountStatistic;

/**
 * A count statistic implementation
 * 
 * 
 */
public class PollCountStatisticImpl extends StatisticImpl implements CountStatistic {

    private PollCountStatisticImpl parent;
    private List<PollCountStatisticImpl> children;

    public PollCountStatisticImpl(PollCountStatisticImpl parent, String name, String description) {
        this(name, description);
        setParent(parent);
    }

    public PollCountStatisticImpl(String name, String description) {
        this(name, "count", description);
    }

    public PollCountStatisticImpl(String name, String unit, String description) {
        super(name, unit, description);
    }

    public PollCountStatisticImpl getParent() {
        return parent;
    }

    public void setParent(PollCountStatisticImpl parent) {
        if (this.parent != null) {
            this.parent.removeChild(this);
        }
        this.parent = parent;
        if (this.parent != null) {
            this.parent.addChild(this);
        }
    }

    private synchronized void removeChild(PollCountStatisticImpl child) {
        if (children != null) {
            children.remove(child);
        }
    }

    private synchronized void addChild(PollCountStatisticImpl child) {
        if (children == null) {
            children = new ArrayList<PollCountStatisticImpl>();
        }
        children.add(child);
    }

    public synchronized long getCount() {
        if (children == null) {
            return 0;
        }
        long count = 0;
        for (Iterator<PollCountStatisticImpl> iter = children.iterator(); iter.hasNext();) {
            PollCountStatisticImpl child = iter.next();
            count += child.getCount();
        }
        return count;
    }

    protected void appendFieldDescription(StringBuffer buffer) {
        buffer.append(" count: ");
        buffer.append(Long.toString(getCount()));
        super.appendFieldDescription(buffer);
    }

    /**
     * @return the average time period that elapses between counter increments
     *         since the last reset.
     */
    public double getPeriod() {
        double count = getCount();
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
        double count = getCount();
        double time = System.currentTimeMillis() - getStartTime();
        return count * 1000.0 / time;
    }

}
