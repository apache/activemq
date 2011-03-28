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
 * A range statistic implementation
 *
 * 
 */
public class RangeStatisticImpl extends StatisticImpl {
    private long highWaterMark;
    private long lowWaterMark;
    private long current;

    public RangeStatisticImpl(String name, String unit, String description) {
        super(name, unit, description);
    }

    public void reset() {
        if (isDoReset()) {
            super.reset();
            current = 0;
            lowWaterMark = 0;
            highWaterMark = 0;
        }
    }

    public long getHighWaterMark() {
        return highWaterMark;
    }

    public long getLowWaterMark() {
        return lowWaterMark;
    }

    public long getCurrent() {
        return current;
    }

    public void setCurrent(long current) {
        this.current = current;
        if (current > highWaterMark) {
            highWaterMark = current;
        }
        if (current < lowWaterMark || lowWaterMark == 0) {
            lowWaterMark = current;
        }
        updateSampleTime();
    }

    protected void appendFieldDescription(StringBuffer buffer) {
        buffer.append(" current: ");
        buffer.append(Long.toString(current));
        buffer.append(" lowWaterMark: ");
        buffer.append(Long.toString(lowWaterMark));
        buffer.append(" highWaterMark: ");
        buffer.append(Long.toString(highWaterMark));
        super.appendFieldDescription(buffer);
    }
}
