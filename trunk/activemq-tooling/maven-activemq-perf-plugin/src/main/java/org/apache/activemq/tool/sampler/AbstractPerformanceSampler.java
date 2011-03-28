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
package org.apache.activemq.tool.sampler;

import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.activemq.tool.properties.AbstractObjectProperties;
import org.apache.activemq.tool.reports.PerformanceReportWriter;

public abstract class AbstractPerformanceSampler extends AbstractObjectProperties implements PerformanceSampler {
    
    protected long rampUpTime = 30 * 1000; // 30 secs
    protected long rampDownTime = 30 * 1000; // 30 secs
    protected long duration = 5 * 60 * 1000; // 5 mins
    protected long interval = 1000; // 1 sec
    protected PerformanceReportWriter perfReportWriter;
    protected PerformanceEventListener perfEventListener;
    protected final AtomicBoolean isRunning = new AtomicBoolean(false);
    protected long sampleIndex;

    public long getRampUpTime() {
        return rampUpTime;
    }

    public void setRampUpTime(long rampUpTime) {
        this.rampUpTime = rampUpTime;
    }

    public long getRampDownTime() {
        return rampDownTime;
    }

    public void setRampDownTime(long rampDownTime) {
        this.rampDownTime = rampDownTime;
    }

    public long getDuration() {
        return duration;
    }

    public void setDuration(long duration) {
        this.duration = duration;
    }

    public long getInterval() {
        return interval;
    }

    public void setInterval(long interval) {
        this.interval = interval;
    }

    public PerformanceReportWriter getPerfReportWriter() {
        return perfReportWriter;
    }

    public void setPerfReportWriter(PerformanceReportWriter perfReportWriter) {
        this.perfReportWriter = perfReportWriter;
    }

    public PerformanceEventListener getPerfEventListener() {
        return perfEventListener;
    }

    public void setPerfEventListener(PerformanceEventListener perfEventListener) {
        this.perfEventListener = perfEventListener;
    }

    public void startSampler() {
        isRunning.set(true);
        Thread t = new Thread(this);
        t.start();
    }

    public void run() {
        try {
            onRampUpStart();
            if (perfEventListener != null) {
                perfEventListener.onRampUpStart(this);
            }

            try {
                Thread.sleep(rampUpTime);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            onSamplerStart();
            if (perfEventListener != null) {
                perfEventListener.onSamplerStart(this);
            }

            sample();

            onSamplerEnd();
            if (perfEventListener != null) {
                perfEventListener.onSamplerEnd(this);
            }

            try {
                Thread.sleep(rampDownTime);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            onRampDownEnd();
            if (perfEventListener != null) {
                perfEventListener.onRampDownEnd(this);
            }
        } finally {
            isRunning.set(false);
            synchronized (isRunning) {
                isRunning.notifyAll();
            }
        }
    }

    protected void sample() {
        // Compute for the actual duration window of the sampler
        long endTime = System.currentTimeMillis() + duration - rampDownTime - rampUpTime;

        while (System.currentTimeMillis() < endTime) {
            try {
                Thread.sleep(interval);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            sampleData();
            sampleIndex++;
        }
    }

    public abstract void sampleData();

    public boolean isRunning() {
        return isRunning.get();
    }

    public void waitUntilDone() {
        while (isRunning()) {
            try {
                synchronized (isRunning) {
                    isRunning.wait(0);
                }
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    // Call back functions to customize behavior of thread.
    protected void onRampUpStart() {
    }

    protected void onSamplerStart() {
    }

    protected void onSamplerEnd() {
    }

    protected void onRampDownEnd() {
    }
}
