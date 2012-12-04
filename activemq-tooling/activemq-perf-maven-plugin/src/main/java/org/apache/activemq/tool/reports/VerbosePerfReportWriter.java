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
package org.apache.activemq.tool.reports;

import java.util.Arrays;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;

import org.apache.activemq.tool.reports.plugins.CpuReportPlugin;
import org.apache.activemq.tool.reports.plugins.ThroughputReportPlugin;

public class VerbosePerfReportWriter extends AbstractPerfReportWriter {

    public void openReportWriter() {
        // Do nothing
    }

    public void closeReportWriter() {
        writeHeader("Performance Summary");
        writePerfSummary();
    }

    public void writeInfo(String info) {
        System.out.println("[PERF-INFO]: " + info);
    }

    public void writeCsvData(int csvType, String csvData) {
        if (csvType == REPORT_PLUGIN_THROUGHPUT) {
            System.out.println("[PERF-TP]: " + csvData);
        } else if (csvType == REPORT_PLUGIN_CPU) {
            System.out.println("[PERF-CPU]: " + csvData);
        }
        handleCsvData(csvType, csvData);
    }

    public void writeProperties(String header, Properties props) {
        writeHeader(header);
        writeProperties(props);
    }

    public void writeProperties(Properties props) {
        for (Iterator i = props.keySet().iterator(); i.hasNext();) {
            String key = (String)i.next();
            String val = props.getProperty(key, "");
            System.out.println("[PERF-PROP]: " + key + "=" + val);
        }
    }

    public void writePerfSummary() {

        Map summary;

        summary = getSummary(REPORT_PLUGIN_THROUGHPUT);
        if (summary != null && summary.size() > 0) {
            writeThroughputSummary(summary);
        }

        summary = getSummary(REPORT_PLUGIN_CPU);
        if (summary != null && summary.size() > 0) {
            writeCpuSummary(summary);
        }

    }

    protected void writeThroughputSummary(Map summary) {
        writeHeader("SYSTEM THROUGHPUT SUMMARY");
        System.out.println("[PERF-TP-SUMMARY] System Total Throughput: " + summary.get(ThroughputReportPlugin.KEY_SYS_TOTAL_TP));
        System.out.println("[PERF-TP-SUMMARY] System Total Clients: " + summary.get(ThroughputReportPlugin.KEY_SYS_TOTAL_CLIENTS));
        System.out.println("[PERF-TP-SUMMARY] System Average Throughput: " + summary.get(ThroughputReportPlugin.KEY_SYS_AVE_TP));
        System.out.println("[PERF-TP-SUMMARY] System Average Throughput Excluding Min/Max: " + summary.get(ThroughputReportPlugin.KEY_SYS_AVE_EMM_TP));
        System.out.println("[PERF-TP-SUMMARY] System Average Client Throughput: " + summary.get(ThroughputReportPlugin.KEY_SYS_AVE_CLIENT_TP));
        System.out.println("[PERF-TP-SUMMARY] System Average Client Throughput Excluding Min/Max: " + summary.get(ThroughputReportPlugin.KEY_SYS_AVE_CLIENT_EMM_TP));
        System.out.println("[PERF-TP-SUMMARY] Min Client Throughput Per Sample: " + summary.get(ThroughputReportPlugin.KEY_MIN_CLIENT_TP));
        System.out.println("[PERF-TP-SUMMARY] Max Client Throughput Per Sample: " + summary.get(ThroughputReportPlugin.KEY_MAX_CLIENT_TP));
        System.out.println("[PERF-TP-SUMMARY] Min Client Total Throughput: " + summary.get(ThroughputReportPlugin.KEY_MIN_CLIENT_TOTAL_TP));
        System.out.println("[PERF-TP-SUMMARY] Max Client Total Throughput: " + summary.get(ThroughputReportPlugin.KEY_MAX_CLIENT_TOTAL_TP));
        System.out.println("[PERF-TP-SUMMARY] Min Client Average Throughput: " + summary.get(ThroughputReportPlugin.KEY_MIN_CLIENT_AVE_TP));
        System.out.println("[PERF-TP-SUMMARY] Max Client Average Throughput: " + summary.get(ThroughputReportPlugin.KEY_MAX_CLIENT_AVE_TP));
        System.out.println("[PERF-TP-SUMMARY] Min Client Average Throughput Excluding Min/Max: " + summary.get(ThroughputReportPlugin.KEY_MIN_CLIENT_AVE_EMM_TP));
        System.out.println("[PERF-TP-SUMMARY] Max Client Average Throughput Excluding Min/Max: " + summary.get(ThroughputReportPlugin.KEY_MAX_CLIENT_AVE_EMM_TP));
    }

    protected void writeCpuSummary(Map summary) {
        writeHeader("SYSTEM CPU USAGE SUMMARY");
        System.out.println("[PERF-CPU-SUMMARY] Total Blocks Received: " + summary.get(CpuReportPlugin.KEY_BLOCK_RECV));
        System.out.println("[PERF-CPU-SUMMARY] Ave Blocks Received: " + summary.get(CpuReportPlugin.KEY_AVE_BLOCK_RECV));

        System.out.println("[PERF-CPU-SUMMARY] Total Blocks Sent: " + summary.get(CpuReportPlugin.KEY_BLOCK_SENT));
        System.out.println("[PERF-CPU-SUMMARY] Ave Blocks Sent: " + summary.get(CpuReportPlugin.KEY_AVE_BLOCK_SENT));

        System.out.println("[PERF-CPU-SUMMARY] Total Context Switches: " + summary.get(CpuReportPlugin.KEY_CTX_SWITCH));
        System.out.println("[PERF-CPU-SUMMARY] Ave Context Switches: " + summary.get(CpuReportPlugin.KEY_AVE_CTX_SWITCH));

        System.out.println("[PERF-CPU-SUMMARY] Total User Time: " + summary.get(CpuReportPlugin.KEY_USER_TIME));
        System.out.println("[PERF-CPU-SUMMARY] Ave User Time: " + summary.get(CpuReportPlugin.KEY_AVE_USER_TIME));

        System.out.println("[PERF-CPU-SUMMARY] Total System Time: " + summary.get(CpuReportPlugin.KEY_SYS_TIME));
        System.out.println("[PERF-CPU-SUMMARY] Ave System Time: " + summary.get(CpuReportPlugin.KEY_AVE_SYS_TIME));

        System.out.println("[PERF-CPU-SUMMARY] Total Idle Time: " + summary.get(CpuReportPlugin.KEY_IDLE_TIME));
        System.out.println("[PERF-CPU-SUMMARY] Ave Idle Time: " + summary.get(CpuReportPlugin.KEY_AVE_IDLE_TIME));

        System.out.println("[PERF-CPU-SUMMARY] Total Wait Time: " + summary.get(CpuReportPlugin.KEY_WAIT_TIME));
        System.out.println("[PERF-CPU-SUMMARY] Ave Wait Time: " + summary.get(CpuReportPlugin.KEY_AVE_WAIT_TIME));
    }

    protected void writeHeader(String header) {
        char[] border = new char[header.length() + 8]; // +8 for spacing
        Arrays.fill(border, '#');
        String borderStr = new String(border);

        System.out.println(borderStr);
        System.out.println("#   " + header + "   #");
        System.out.println(borderStr);
    }

}
