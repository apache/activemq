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

import edu.emory.mathcs.backport.java.util.concurrent.atomic.AtomicBoolean;

import javax.jms.JMSException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;

import org.apache.activemq.tool.reports.PerformanceReportWriter;

public class PerfMeasurementTool implements PerfEventListener, Runnable {
    public static final String PREFIX_CONFIG_SYSTEM_TEST = "sampler.";

    private long duration     = 5 * 60 * 1000; // 5 mins by default test duration
    private long interval     = 1000;          // 1 sec sample interval
    private long rampUpTime   = 1 * 60 * 1000; // 1 min default test ramp up time
    private long rampDownTime = 1 * 60 * 1000; // 1 min default test ramp down time
    private long sampleIndex  = 0;

    private AtomicBoolean start = new AtomicBoolean(false);
    private AtomicBoolean stop  = new AtomicBoolean(false);
    private AtomicBoolean isRunning = new AtomicBoolean(false);
    private PerformanceReportWriter perfWriter = null;
    private Properties samplerSettings = new Properties();

    private List perfClients = new ArrayList();

    public void registerClient(PerfMeasurable client) {
        client.setPerfEventListener(this);
        perfClients.add(client);
    }

    public void registerClient(PerfMeasurable[] clients) {
        for (int i = 0; i < clients.length; i++) {
            registerClient(clients[i]);
        }
    }

    public Properties getSamplerSettings() {
        return samplerSettings;
    }

    public void setSamplerSettings(Properties samplerSettings) {
        this.samplerSettings = samplerSettings;
        ReflectionUtil.configureClass(this, samplerSettings);
    }

    public PerformanceReportWriter getPerfWriter() {
        return perfWriter;
    }

    public void setPerfWriter(PerformanceReportWriter writer) {
        this.perfWriter = writer;
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

    public void onConfigStart(PerfMeasurable client) {
    }

    public void onConfigEnd(PerfMeasurable client) {
    }

    public void onPublishStart(PerfMeasurable client) {
    }

    public void onPublishEnd(PerfMeasurable client) {
    }

    public void onConsumeStart(PerfMeasurable client) {
    }

    public void onConsumeEnd(PerfMeasurable client) {
    }

    public void onJMSException(PerfMeasurable client, JMSException e) {
    }

    public void onException(PerfMeasurable client, Exception e) {
        stop.set(true);
    }

    public void startSampler() {
        Thread t = new Thread(this);
        t.setName("Performance Sampler");

        isRunning.set(true);
        t.start();
    }

    public void run() {
        // Compute for the actual duration window of the sampler
        long endTime = System.currentTimeMillis() + duration - rampDownTime;
        try {
            try {
                Thread.sleep(rampUpTime);
            } catch (InterruptedException e) {
            }

            // Let's reset the throughput first and start getting the samples
            for (Iterator i = perfClients.iterator(); i.hasNext();) {
                PerfMeasurable client = (PerfMeasurable) i.next();
                client.reset();
            }

            while (System.currentTimeMillis() < endTime && !stop.get()) {
                try {
                    Thread.sleep(interval);
                } catch (InterruptedException e) {
                }
                sampleClients();
                sampleIndex++;
            }
        } finally {
            isRunning.set(false);
            synchronized (isRunning) {
                isRunning.notifyAll();
            }
        }
    }

    public void sampleClients() {
        for (Iterator i = perfClients.iterator(); i.hasNext();) {
            PerfMeasurable client = (PerfMeasurable) i.next();
            if (perfWriter != null) {
                perfWriter.writePerfData("index=" + sampleIndex + ",clientName=" + client.getClientName() + ",throughput=" + client.getThroughput());
            }
            client.reset();
        }
    }

    public void waitForSamplerToFinish(long timeout) {
        while (isRunning.get()) {
            try {
                synchronized (isRunning) {
                    isRunning.wait(timeout);
                }
            } catch (InterruptedException e) {
            }
        }
    }
}
