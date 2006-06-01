/**
 *
 * Copyright 2005-2006 The Apache Software Foundation
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
 */
package org.apache.activemq.tool;

public class JmsPerfClientSupport extends JmsConfigurableClientSupport implements PerfMeasurable {

    protected long throughput   = 0;
    protected long interval     = 1000; // 1 sec
    protected long duration     = 1000 * 60 * 10; // 10 min
    protected long rampUpTime   = 1000 * 60 * 1;  // 1 min
    protected long rampDownTime = 1000 * 60 * 1;  // 1 min

    protected PerfEventListener listener = null;

    public void reset() {
        setThroughput(0);
    }

    public long getThroughput() {
        return throughput;
    }

    public synchronized void setThroughput(long val) {
        this.throughput = val;
    }

    public synchronized void incThroughput() {
        throughput++;
    }

    public synchronized void incThroughput(long val) {
        throughput += val;
    }

    public long getInterval() {
        return interval;
    }

    public void setInterval(long val) {
        this.interval = val;
    }

    public long getDuration() {
        return duration;
    }

    public void setDuration(long val) {
        this.duration = val;
    }

    public long getRampUpTime() {
        return rampUpTime;
    }

    public void setRampUpTime(long val) {
        this.rampUpTime = val;
    }

    public long getRampDownTime() {
        return rampDownTime;
    }

    public void setRampDownTime(long val) {
        this.rampDownTime = val;
    }

    public void setPerfEventListener(PerfEventListener listener) {
        this.listener = listener;
    }

    public PerfEventListener getPerfEventListener() {
        return listener;
    }
}
