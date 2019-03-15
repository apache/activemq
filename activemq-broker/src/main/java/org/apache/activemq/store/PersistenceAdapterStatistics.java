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
package org.apache.activemq.store;

import org.apache.activemq.management.StatsImpl;
import org.apache.activemq.management.TimeStatisticImpl;

public class PersistenceAdapterStatistics extends StatsImpl {
    protected TimeStatisticImpl writeTime;
    protected TimeStatisticImpl readTime;

    public PersistenceAdapterStatistics() {
        writeTime = new TimeStatisticImpl("writeTime", "Time to write data to the PersistentAdapter.");
        readTime = new TimeStatisticImpl("readTime", "Time to read data from the PersistentAdapter.");
        addStatistic("writeTime", writeTime);
        addStatistic("readTime", readTime);
    }

    public void addWriteTime(final long time) {
        writeTime.addTime(time);
    }

    public void addReadTime(final long time) {
        readTime.addTime(time);
    }

    @Override
    public void setEnabled(boolean enabled) {
        super.setEnabled(enabled);
        writeTime.setEnabled(enabled);
        readTime.setEnabled(enabled);
    }

    public TimeStatisticImpl getWriteTime() {
        return writeTime;
    }

    public TimeStatisticImpl getReadTime() { return readTime; }

    @Override
    public void reset() {
        if (isDoReset()) {
            writeTime.reset();
            readTime.reset();
        }
    }

    public void setParent(PersistenceAdapterStatistics parent) {
        if (parent != null) {
            writeTime.setParent(parent.writeTime);
            readTime.setParent(parent.readTime);
        } else {
            writeTime.setParent(null);
            readTime.setParent(null);
        }

    }
}
