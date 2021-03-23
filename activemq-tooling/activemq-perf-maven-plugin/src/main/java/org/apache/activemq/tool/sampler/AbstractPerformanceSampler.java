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

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.activemq.tool.ClientRunBasis;
import org.apache.activemq.tool.properties.AbstractObjectProperties;
import org.apache.activemq.tool.reports.PerformanceReportWriter;
import org.apache.commons.lang3.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractPerformanceSampler extends AbstractObjectProperties implements PerformanceSampler {
    
    private final Logger log = LoggerFactory.getLogger(this.getClass());

    protected long rampUpPercent = 0;
    protected long rampDownPercent = 0;

    // the following are all optionally set; they are otherwise worked out at run time
    protected Long rampUpTime;
    protected Long rampDownTime;
    protected Long duration;

    protected long interval = 1000; // 1 sec
    protected PerformanceReportWriter perfReportWriter;
    protected PerformanceEventListener perfEventListener;
    protected final AtomicBoolean isRunning = new AtomicBoolean(false);
    protected CountDownLatch completionLatch;
    protected long sampleIndex;

    @Override
    public Long getRampUpTime() {
        return rampUpTime;
    }

    @Override
    public void setRampUpTime(long rampUpTime) {
        this.rampUpTime = rampUpTime;
    }

    @Override
    public Long getRampDownTime() {
        return rampDownTime;
    }

    @Override
    public void setRampDownTime(long rampDownTime) {
        this.rampDownTime = rampDownTime;
    }

    @Override
    public Long getDuration() {
        return duration;
    }

    @Override
    public void setDuration(long duration) {
        this.duration = duration;
    }

    @Override
    public long getInterval() {
        return interval;
    }

    @Override
    public void setInterval(long interval) {
        this.interval = interval;
    }

    @Override
    public long getRampUpPercent() {
        return rampUpPercent;
    }

    @Override
    public void setRampUpPercent(long rampUpPercent) {
        Validate.isTrue((rampUpPercent >= 0) && (rampUpPercent <= 100), "rampUpPercent must be a value between 0 and 100");
        this.rampUpPercent = rampUpPercent;
    }

    @Override
    public long getRampDownPercent() {
        return rampDownPercent;
    }

    @Override
    public void setRampDownPercent(long rampDownPercent) {
        Validate.isTrue((rampDownPercent >= 0) && (rampDownPercent < 100), "rampDownPercent must be a value between 0 and 99");
        this.rampDownPercent = rampDownPercent;
    }

    @Override
    public PerformanceReportWriter getPerfReportWriter() {
        return perfReportWriter;
    }

    @Override
    public void setPerfReportWriter(PerformanceReportWriter perfReportWriter) {
        this.perfReportWriter = perfReportWriter;
    }

    @Override
    public PerformanceEventListener getPerfEventListener() {
        return perfEventListener;
    }

    @Override
    public void setPerfEventListener(PerformanceEventListener perfEventListener) {
        this.perfEventListener = perfEventListener;
    }

    @Override
    public void startSampler(CountDownLatch completionLatch, ClientRunBasis clientRunBasis, long clientRunDuration) {
        Validate.notNull(clientRunBasis);
        Validate.notNull(completionLatch);

        if (clientRunBasis == ClientRunBasis.time) {
            // override the default durations
            // if the user has overridden a duration, then use that
            duration = (duration == null) ? clientRunDuration : this.duration;
            rampUpTime = (rampUpTime == null) ? (duration / 100 * rampUpPercent) : this.rampUpTime;
            rampDownTime = (rampDownTime == null) ? (duration / 100 * rampDownPercent) : this.rampDownTime;

            Validate.isTrue(duration >= (rampUpTime + rampDownTime),
                    "Ramp times (up: " + rampDownTime + ", down: " + rampDownTime + ") exceed the sampler duration (" + duration + ")");
            log.info("Sampling duration: {} ms, ramp up: {} ms, ramp down: {} ms", duration, rampUpTime, rampDownTime);

            // spawn notifier thread to stop the sampler, taking ramp-down time into account
            Thread notifier = new Thread(new RampDownNotifier(this));
            notifier.setName("RampDownNotifier[" + this.getClass().getSimpleName() + "]");
            notifier.start();
        } else {
            log.info("Performance test running on count basis; ignoring duration and ramp times");
            setRampUpTime(0);
            setRampDownTime(0);
        }

        this.completionLatch = completionLatch;
        Thread t = new Thread(this);
        t.setName(this.getClass().getSimpleName());
        t.start();
        isRunning.set(true);
    }

    @Override
    public void finishSampling() {
        isRunning.set(false);
    }

    @Override
    public void run() {
        try {
            log.debug("Ramp up start");
            onRampUpStart();
            if (perfEventListener != null) {
                perfEventListener.onRampUpStart(this);
            }

            if (rampUpTime > 0) {
                try {
                    Thread.sleep(rampUpTime);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }

            log.debug("Sampler start");
            onSamplerStart();
            if (perfEventListener != null) {
                perfEventListener.onSamplerStart(this);
            }

            sample();

            log.debug("Sampler end");
            onSamplerEnd();
            if (perfEventListener != null) {
                perfEventListener.onSamplerEnd(this);
            }

            if (rampDownTime > 0) {
                try {
                    Thread.sleep(rampDownTime);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }

            log.debug("Ramp down end");
            onRampDownEnd();
            if (perfEventListener != null) {
                perfEventListener.onRampDownEnd(this);
            }
        } finally {
            completionLatch.countDown();
        }
    }

    protected void sample() {
        while (isRunning.get()) {
            try {
                Thread.sleep(interval);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            sampleData();
            sampleIndex++;
        }
    }

    @Override
    public abstract void sampleData();

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
