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
package org.apache.activemq.tool.reports;

import java.util.Properties;
import java.util.Iterator;
import java.util.Map;
import java.util.Arrays;

public class VerbosePerfReportWriter extends AbstractPerfReportWriter {

    public void openReportWriter() {
        writeProperties("System Properties", System.getProperties());
    }

    public void closeReportWriter() {
        writeHeader("Performance Summary");
        writePerfSummary();
    }

    public void writeInfo(String info) {
        System.out.println("[PERF-INFO]: " + info);
    }

    public void writeHeader(String header) {
        char[] border = new char[header.length() + 8]; // +8 for spacing
        Arrays.fill(border, '#');
        String borderStr = new String(border);

        System.out.println(borderStr);
        System.out.println("#   " + header + "   #");
        System.out.println(borderStr);
    }

    public void writePerfData(String data) {
        System.out.println("[PERF-DATA]: " + data);
        // Assume data is a CSV of key-value pair
        parsePerfCsvData(data);
    }

    public void writeProperties(String header, Properties props) {
        writeHeader(header);
        writeProperties(props);
    }

    public void writeProperties(Properties props) {
        for (Iterator i=props.keySet().iterator(); i.hasNext();) {
            String key = (String)i.next();
            String val = props.getProperty(key, "");
            System.out.println("[PERF-PROP]: " + key + "=" + val);
        }
    }

    public void writePerfSummary() {
        Map summary = createPerfSummary(clientThroughputs);

        System.out.println("[PERF-SUMMARY] System Total Throughput: " + summary.get(KEY_SYS_TOTAL_TP));
        System.out.println("[PERF-SUMMARY] System Total Clients: " + summary.get(KEY_SYS_TOTAL_CLIENTS));
        System.out.println("[PERF-SUMMARY] System Average Throughput: " + summary.get(KEY_SYS_AVE_TP));
        System.out.println("[PERF-SUMMARY] System Average Throughput Excluding Min/Max: " + summary.get(KEY_SYS_AVE_EMM_TP));
        System.out.println("[PERF-SUMMARY] Min Client Throughput Per Sample: " + summary.get(KEY_MIN_CLIENT_TP));
        System.out.println("[PERF-SUMMARY] Max Client Throughput Per Sample: " + summary.get(KEY_MAX_CLIENT_TP));
        System.out.println("[PERF-SUMMARY] Min Client Total Throughput: " + summary.get(KEY_MIN_CLIENT_TOTAL_TP));
        System.out.println("[PERF-SUMMARY] Max Client Total Throughput: " + summary.get(KEY_MAX_CLIENT_TOTAL_TP));
        System.out.println("[PERF-SUMMARY] Min Client Average Throughput: " + summary.get(KEY_MIN_CLIENT_AVE_TP));
        System.out.println("[PERF-SUMMARY] Max Client Average Throughput: " + summary.get(KEY_MAX_CLIENT_AVE_TP));
        System.out.println("[PERF-SUMMARY] Min Client Average Throughput Excluding Min/Max: " + summary.get(KEY_MIN_CLIENT_AVE_EMM_TP));
        System.out.println("[PERF-SUMMARY] Max Client Average Throughput Excluding Min/Max: " + summary.get(KEY_MAX_CLIENT_AVE_EMM_TP));
    }

}
