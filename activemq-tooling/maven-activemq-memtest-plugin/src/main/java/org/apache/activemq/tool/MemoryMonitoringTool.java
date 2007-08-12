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
package org.apache.activemq.tool;

import java.io.DataOutputStream;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

public class MemoryMonitoringTool implements Runnable {

    protected Properties testSettings = new Properties();
    protected ReportGenerator reportGenerator = new ReportGenerator();

    private long checkpointInterval = 5000;          // 5 sec sample checkpointInterval
    private long resultIndex;
    private AtomicBoolean isRunning = new AtomicBoolean(false);
    private DataOutputStream dataDoutputStream;
    private MemoryMXBean memoryBean;

    public Properties getTestSettings() {
        return testSettings;
    }

    public void setTestSettings(Properties sysTestSettings) {
        this.testSettings = sysTestSettings;
    }

    public DataOutputStream getDataOutputStream() {
        return dataDoutputStream;
    }

    public void setDataOutputStream(DataOutputStream dataDoutputStream) {
        this.dataDoutputStream = dataDoutputStream;
    }


    public void stopMonitor() {
        isRunning.set(false);
    }


    public long getCheckpointInterval() {
        return checkpointInterval;
    }

    public void setCheckpointInterval(long checkpointInterval) {
        this.checkpointInterval = checkpointInterval;
    }


    public Thread startMonitor() {

        String intervalStr = this.getTestSettings().getProperty("checkpoint_interval");
        checkpointInterval = new Integer(intervalStr).intValue();
        this.getTestSettings().remove("checkpoint_interval");

        memoryBean = ManagementFactory.getMemoryMXBean();
        reportGenerator.setTestSettings(getTestSettings());
        addTestInformation();

        Thread t = new Thread(this);
        t.setName("Memory monitoring tool");
        isRunning.set(true);
        t.start();

        return t;

    }


    public void addTestInformation() {
        reportGenerator.setReportName(this.getTestSettings().getProperty("report_name"));
        reportGenerator.setReportDirectory(this.getTestSettings().getProperty("report_directory"));
        reportGenerator.startGenerateReport();

        reportGenerator.addTestInformation();
        reportGenerator.writeWithIndent(4, "<jvm_memory_settings>");
        reportGenerator.writeWithIndent(6, "<heap_memory>");
        reportGenerator.writeWithIndent(8, "<committed>" + memoryBean.getHeapMemoryUsage().getCommitted() + "</committed>");
        reportGenerator.writeWithIndent(8, "<max>" + memoryBean.getHeapMemoryUsage().getMax() + "</max>");
        reportGenerator.writeWithIndent(6, "</heap_memory>");
        reportGenerator.writeWithIndent(6, "<non_heap_memory>");
        reportGenerator.writeWithIndent(8, "<committed>" + memoryBean.getNonHeapMemoryUsage().getCommitted() + "</committed>");
        reportGenerator.writeWithIndent(8, "<max>" + memoryBean.getNonHeapMemoryUsage().getMax() + "</max>");
        reportGenerator.writeWithIndent(6, "</non_heap_memory>");
        reportGenerator.writeWithIndent(4, "</jvm_memory_settings>");

        reportGenerator.addClientSettings();
        reportGenerator.endTestInformation();
    }


    public void run() {

        long nonHeapMB = 0;
        long heapMB = 0;
        long oneMB = 1024 * 1024;

        reportGenerator.startTestResult(getCheckpointInterval());
        while (isRunning.get()) {

            try {
                //wait every check point before getting the next memory usage
                Thread.sleep(checkpointInterval);

                nonHeapMB = memoryBean.getNonHeapMemoryUsage().getUsed() / oneMB;
                heapMB = memoryBean.getHeapMemoryUsage().getUsed() / oneMB;

                reportGenerator.writeWithIndent(6, "<memory_usage index=" + resultIndex 
                                                + " non_heap_mb=" + nonHeapMB 
                                                + " non_heap_bytes=" 
                                                + memoryBean.getNonHeapMemoryUsage().getUsed() 
                                                + " heap_mb=" + heapMB 
                                                + " heap_bytes=" + memoryBean.getHeapMemoryUsage().getUsed() + "/>");

                resultIndex++;

            } catch (Exception e) {
                e.printStackTrace();

            }


        }
        reportGenerator.endTestResult();
        reportGenerator.stopGenerateReport();

    }


}
