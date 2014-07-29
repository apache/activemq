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
package org.apache.activemq.tool.properties;

import java.io.File;
import java.util.HashSet;
import java.util.Set;

public class JmsClientSystemProperties extends AbstractObjectProperties {
    
    public static final String DEST_DISTRO_ALL    = "all";    // Each client will send/receive to all destination;
    public static final String DEST_DISTRO_EQUAL  = "equal";  // Equally divide the number of destinations to the number of clients
    public static final String DEST_DISTRO_DIVIDE = "divide"; // Divide the destination among the clients, even if some have more destination than others

    public static final String REPORT_VERBOSE  = "verbose"; // Report would be generated to the console
    public static final String REPORT_XML_FILE = "xml"; // Report would be generated to an xml file

    public static final String SAMPLER_TP  = "tp";
    public static final String SAMPLER_CPU = "cpu";

    protected File propsConfigFile;

    protected String reportType = REPORT_XML_FILE;
    protected String reportDir  = "./";
    protected String reportName;

    protected String samplers = SAMPLER_TP + "," + SAMPLER_CPU; // Start both samplers

    protected String spiClass = "org.apache.activemq.tool.spi.ActiveMQReflectionSPI";
    protected String clientPrefix = "JmsClient";
    protected int numClients = 1;
    protected int totalDests = 1;
    protected String destDistro = DEST_DISTRO_ALL;

    public String getReportType() {
        return reportType;
    }

    public void setReportType(String reportType) {
        this.reportType = reportType;
    }

    public String getReportDir() {
        return reportDir;
    }

    public void setReportDir(String reportDir) {
        this.reportDir = reportDir;
    }

    public String getReportName() {
        return reportName;
    }

    public void setReportName(String reportName) {
        this.reportName = reportName;
    }

    public String getSamplers() {
        return samplers;
    }

    public Set<String> getSamplersSet() {
        Set<String> samplersSet = new HashSet<>();
        for (String sampler : samplers.split(",")) {
            samplersSet.add(sampler.trim());
        }
        return samplersSet;
    }

    public void setSamplers(String samplers) {
        this.samplers = samplers;
    }

    public String getSpiClass() {
        return spiClass;
    }

    public void setSpiClass(String spiClass) {
        this.spiClass = spiClass;
    }

    public String getClientPrefix() {
        return clientPrefix;
    }

    public void setClientPrefix(String clientPrefix) {
        this.clientPrefix = clientPrefix;
    }

    public int getNumClients() {
        return numClients;
    }

    public void setNumClients(int numClients) {
        this.numClients = numClients;
    }

    public int getTotalDests() {
        return totalDests;
    }

    public void setTotalDests(int totalDests) {
        this.totalDests = totalDests;
    }

    public String getDestDistro() {
        return destDistro;
    }

    public void setDestDistro(String destDistro) {
        this.destDistro = destDistro;
    }

    public String getPropsConfigFile() {
        return this.propsConfigFile + "";
    }

    public void setPropsConfigFile(String propsConfigFile) {
        this.propsConfigFile = new File(propsConfigFile);
    }
}
