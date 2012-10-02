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
package org.apache.activemq.tool.sampler.plugins;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.StringTokenizer;
import java.util.concurrent.atomic.AtomicBoolean;

public class LinuxCpuSamplerPlugin implements CpuSamplerPlugin, Runnable {

    private Process vmstatProcess;
    private String vmstat;
    private String result = "";
    private final Object mutex = new Object();
    private AtomicBoolean stop = new AtomicBoolean(false);

    public LinuxCpuSamplerPlugin(long intervalInMs) {
        vmstat = "vmstat -n " + (int)(intervalInMs / 1000);
    }

    public void start() {
        stop.set(false);
        Thread t = new Thread(this);
        t.start();
    }

    public void stop() {
        stop.set(true);
        try {
            vmstatProcess.waitFor();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public void run() {

        try {
            vmstatProcess = Runtime.getRuntime().exec(vmstat);
            BufferedReader br = new BufferedReader(new InputStreamReader(vmstatProcess.getInputStream()), 1024);

            br.readLine(); // throw away the first line

            String header = br.readLine();
            String data;

            while (!stop.get()) {
                data = br.readLine();
                if (data != null) {
                    String csvData = convertToCSV(header, data);
                    synchronized (mutex) {
                        result = csvData;
                    }
                }
            }
            br.close();
            vmstatProcess.destroy();

        } catch (IOException ioe) {
            ioe.printStackTrace();
        }
    }

    public String getCpuUtilizationStats() {
        String data;
        synchronized (mutex) {
            data = result;
            result = "";
        }
        return data;
    }

    public String getVmstat() {
        return vmstat;
    }

    public void setVmstat(String vmstat) {
        this.vmstat = vmstat;
    }

    protected String convertToCSV(String header, String data) {
        StringTokenizer headerTokens = new StringTokenizer(header, " ");
        StringTokenizer dataTokens = new StringTokenizer(data, " ");

        String csv = "";
        while (headerTokens.hasMoreTokens()) {
            csv += headerTokens.nextToken() + "=" + dataTokens.nextToken() + ",";
        }

        return csv;
    }
}
