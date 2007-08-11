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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.activemq.tool.reports.PerformanceStatisticsUtil;

import java.util.Map;
import java.util.StringTokenizer;
import java.util.List;
import java.util.ArrayList;
import java.util.HashMap;

public class CpuReportPlugin implements ReportPlugin {
    private static final Log log = LogFactory.getLog(CpuReportPlugin.class);

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

    protected List blockRecv = new ArrayList();
    protected List blockSent = new ArrayList();
    protected List ctxSwitch = new ArrayList();
    protected List userTime  = new ArrayList();
    protected List sysTime   = new ArrayList();
    protected List idleTime  = new ArrayList();
    protected List waitTime  = new ArrayList();

    public void handleCsvData(String csvData) {
        StringTokenizer tokenizer = new StringTokenizer(csvData, ",");
        String data, key, val;
        while (tokenizer.hasMoreTokens()) {
            data = tokenizer.nextToken();
            key  = data.substring(0, data.indexOf("="));
            val  = data.substring(data.indexOf("=") + 1);

            addToCpuList(key, val);
        }
    }

    public Map getSummary() {
        long val;

        Map summary = new HashMap();

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
            log.warn("Unrecognized CPU data. " + key + "=" + val);
        }
    }
}
