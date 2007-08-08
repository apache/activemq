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

import javax.management.j2ee.statistics.Statistic;

/**
 * Base class for a Statistic implementation
 * 
 * @version $Revision: 1.2 $
 */
public class StatisticImpl implements Statistic, Resettable {
    private String name;
    private String unit;
    private String description;
    private long startTime;
    private long lastSampleTime;
    protected boolean enabled;

    public StatisticImpl(String name, String unit, String description) {
        this.name = name;
        this.unit = unit;
        this.description = description;
        startTime = System.currentTimeMillis();
        lastSampleTime = startTime;
    }

    public synchronized void reset() {
        startTime = System.currentTimeMillis();
        lastSampleTime = startTime;
    }

    protected synchronized void updateSampleTime() {
        lastSampleTime = System.currentTimeMillis();
    }

    public synchronized String toString() {
        StringBuffer buffer = new StringBuffer();
        buffer.append(name);
        buffer.append("{");
        appendFieldDescription(buffer);
        buffer.append(" }");
        return buffer.toString();
    }

    public String getName() {
        return name;
    }

    public String getUnit() {
        return unit;
    }

    public String getDescription() {
        return description;
    }

    public synchronized long getStartTime() {
        return startTime;
    }

    public synchronized long getLastSampleTime() {
        return lastSampleTime;
    }

    /**
     * @return the enabled
     */
    public boolean isEnabled() {
        return this.enabled;
    }

    /**
     * @param enabled the enabled to set
     */
    public void setEnabled(boolean enabled) {
        this.enabled = enabled;
    }

    protected synchronized void appendFieldDescription(StringBuffer buffer) {
        buffer.append(" unit: ");
        buffer.append(unit);
        buffer.append(" startTime: ");
        // buffer.append(new Date(startTime));
        buffer.append(startTime);
        buffer.append(" lastSampleTime: ");
        // buffer.append(new Date(lastSampleTime));
        buffer.append(lastSampleTime);
        buffer.append(" description: ");
        buffer.append(description);
    }

}
