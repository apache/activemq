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

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Enumeration;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class ReportGenerator {
    
    private static final Log LOG = LogFactory.getLog(ReportGenerator.class);
    private String reportDirectory;
    private String reportName;
    private PrintWriter writer;
    private File reportFile;
    private Properties testSettings;

    public ReportGenerator() {
    }

    public ReportGenerator(String reportDirectory, String reportName) {
        this.setReportDirectory(reportDirectory);
        this.setReportName(reportName);
    }

    public void startGenerateReport() {

        File reportDir = new File(getReportDirectory());

        // Create output directory if it doesn't exist.
        if (!reportDir.exists()) {
            reportDir.mkdirs();
        }

        if (reportDir != null) {
            reportFile = new File(this.getReportDirectory() + File.separator + this.getReportName() + ".xml");
        }

        try {
            this.writer = new PrintWriter(new FileOutputStream(reportFile));
        } catch (IOException e1) {
            e1.printStackTrace(); // To change body of catch statement use
                                    // File | Settings | File Templates.
        }
    }

    public void stopGenerateReport() {
        writeWithIndent(0, "</test-report>");
        this.getWriter().flush();
        this.getWriter().close();
        LOG.info(" TEST REPORT OUTPUT : " + reportFile.getAbsolutePath());

    }

    protected void addTestInformation() {

        writeWithIndent(0, "<test-report>");
        writeWithIndent(2, "<test-information>");

        writeWithIndent(4, "<os-name>" + System.getProperty("os.name") + "</os-name>");
        writeWithIndent(4, "<java-version>" + System.getProperty("java.version") + "</java-version>");

    }

    protected void addClientSettings() {
        if (this.getTestSettings() != null) {
            Enumeration keys = getTestSettings().propertyNames();

            writeWithIndent(4, "<test-settings>");

            String key;
            while (keys.hasMoreElements()) {
                key = (String)keys.nextElement();
                writeWithIndent(6, "<" + key + ">" + getTestSettings().get(key) + "</" + key + ">");
            }

            writeWithIndent(4, "</test-settings>");
        }
    }

    protected void endTestInformation() {
        writeWithIndent(2, "</test-information>");

    }

    protected void startTestResult(long checkpointInterval) {
        long intervalInSec = checkpointInterval / 1000;
        writeWithIndent(2, "<test-result checkpoint_interval_in_sec=" + intervalInSec + " >");
    }

    protected void endTestResult() {
        writeWithIndent(2, "</test-result>");
    }

    protected void writeWithIndent(int indent, String result) {
        StringBuffer buffer = new StringBuffer();

        for (int i = 0; i < indent; ++i) {
            buffer.append(" ");
        }

        buffer.append(result);
        writer.println(buffer.toString());
    }

    public PrintWriter getWriter() {
        return this.writer;
    }

    public String getReportDirectory() {
        return reportDirectory;
    }

    public void setReportDirectory(String reportDirectory) {
        this.reportDirectory = reportDirectory;
    }

    public String getReportName() {
        return reportName;
    }

    public void setReportName(String reportName) {
        this.reportName = reportName;
    }

    public Properties getTestSettings() {
        return testSettings;
    }

    public void setTestSettings(Properties testSettings) {
        this.testSettings = testSettings;
    }
}
