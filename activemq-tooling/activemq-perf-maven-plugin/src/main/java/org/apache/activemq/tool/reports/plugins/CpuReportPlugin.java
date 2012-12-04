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
package org.apache.activemq.tool.reports.plugins;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;

import org.apache.activemq.tool.reports.PerformanceStatisticsUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CpuReportPlugin implements ReportPlugin {

    public static final String NAME_IGNORE_LIST = "$index$timeUnit$r$b$w$swpd$inact$active$free$buff$cache$si$so$in$";
    public static final String NAME_BLOCK_RECV = "bi";
    public static final String NAME_BLOCK_SENT = "bo";
    public static final String NAME_CTX_SWITCH = "cs";
    public static final String NAME_USER_TIME  = "us";
    public static final String NAME_SYS_TIME   = "sy";
    public static final String NAME_IDLE_TIME  = "id";
    public static final String NAME_WAIT_TIME  = "wa";

    public static final String KEY_BLOCK_RECV = "BlocksReceived";
    public static final String KEY_BLOCK_SENT = "BlocksSent";
    public static final String KEY_CTX_SWITCH = "ContextSwitches";
    public static final String KEY_USER_TIME  = "UserTime";
    public static final String KEY_SYS_TIME   = "SystemTime";
    public static final String KEY_IDLE_TIME  = "IdleTime";
    public static final String KEY_WAIT_TIME  = "WaitingTime";

    public static final String KEY_AVE_BLOCK_RECV = "AveBlocksReceived";
    public static final String KEY_AVE_BLOCK_SENT = "AveBlocksSent";
    public static final String KEY_AVE_CTX_SWITCH = "AveContextSwitches";
    public static final String KEY_AVE_USER_TIME  = "AveUserTime";
    public static final String KEY_AVE_SYS_TIME   = "AveSystemTime";
    public static final String KEY_AVE_IDLE_TIME  = "AveIdleTime";
    public static final String KEY_AVE_WAIT_TIME  = "AveWaitingTime";

    private static final Logger LOG = LoggerFactory.getLogger(CpuReportPlugin.class);

    protected List<Long> blockRecv = new ArrayList<Long>();
    protected List<Long> blockSent = new ArrayList<Long>();
    protected List<Long> ctxSwitch = new ArrayList<Long>();
    protected List<Long> userTime  = new ArrayList<Long>();
    protected List<Long> sysTime   = new ArrayList<Long>();
    protected List<Long> idleTime  = new ArrayList<Long>();
    protected List<Long> waitTime  = new ArrayList<Long>();

    public void handleCsvData(String csvData) {
        StringTokenizer tokenizer = new StringTokenizer(csvData, ",");
        String data;
        String key;
        String val;
        while (tokenizer.hasMoreTokens()) {
            data = tokenizer.nextToken();
            key  = data.substring(0, data.indexOf("="));
            val  = data.substring(data.indexOf("=") + 1);

            addToCpuList(key, val);
        }
    }

    public Map<String, String> getSummary() {
        long val;

        Map<String, String> summary = new HashMap<String, String>();

        if (blockRecv.size() > 0) {
            val = PerformanceStatisticsUtil.getSum(blockRecv);
            summary.put(KEY_BLOCK_RECV, String.valueOf(val));
            summary.put(KEY_AVE_BLOCK_RECV, String.valueOf((double)val / (double)blockRecv.size()));
        }

        if (blockSent.size() > 0) {
            val = PerformanceStatisticsUtil.getSum(blockSent);
            summary.put(KEY_BLOCK_SENT, String.valueOf(val));
            summary.put(KEY_AVE_BLOCK_SENT, String.valueOf((double)val / (double)blockSent.size()));
        }

        if (ctxSwitch.size() > 0) {
            val = PerformanceStatisticsUtil.getSum(ctxSwitch);
            summary.put(KEY_CTX_SWITCH, String.valueOf(val));
            summary.put(KEY_AVE_CTX_SWITCH, String.valueOf((double)val / (double)ctxSwitch.size()));
        }

        if (userTime.size() > 0) {
            val = PerformanceStatisticsUtil.getSum(userTime);
            summary.put(KEY_USER_TIME, String.valueOf(val));
            summary.put(KEY_AVE_USER_TIME, String.valueOf((double)val / (double)userTime.size()));
        }

        if (sysTime.size() > 0) {
            val = PerformanceStatisticsUtil.getSum(sysTime);
            summary.put(KEY_SYS_TIME, String.valueOf(val));
            summary.put(KEY_AVE_SYS_TIME, String.valueOf((double)val / (double)sysTime.size()));
        }

        if (idleTime.size() > 0) {
            val = PerformanceStatisticsUtil.getSum(idleTime);
            summary.put(KEY_IDLE_TIME, String.valueOf(val));
            summary.put(KEY_AVE_IDLE_TIME, String.valueOf((double)val / (double)idleTime.size()));
        }

        if (waitTime.size() > 0) {
            val = PerformanceStatisticsUtil.getSum(waitTime);
            summary.put(KEY_WAIT_TIME, String.valueOf(val));
            summary.put(KEY_AVE_WAIT_TIME, String.valueOf((double)val / (double)waitTime.size()));
        }

        if (summary.size() > 0) {
            return summary;
        } else {
            return null;
        }
    }

    protected void addToCpuList(String key, String val) {
        if (key.equals(NAME_BLOCK_RECV)) {
            blockRecv.add(Long.valueOf(val));
        } else if (key.equals(NAME_BLOCK_SENT)) {
            blockSent.add(Long.valueOf(val));
        } else if (key.equals(NAME_CTX_SWITCH)) {
            ctxSwitch.add(Long.valueOf(val));
        } else if (key.equals(NAME_USER_TIME)) {
            userTime.add(Long.valueOf(val));
        } else if (key.equals(NAME_SYS_TIME)) {
            sysTime.add(Long.valueOf(val));
        } else if (key.equals(NAME_IDLE_TIME)) {
            idleTime.add(Long.valueOf(val));
        } else if (key.equals(NAME_WAIT_TIME)) {
            waitTime.add(Long.valueOf(val));
        } else if (NAME_IGNORE_LIST.indexOf("$" + key + "$") != -1) {
            // Ignore key
        } else {
            LOG.warn("Unrecognized CPU data. " + key + "=" + val);
        }
    }
}
