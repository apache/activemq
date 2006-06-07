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

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Enumeration;
import java.util.Properties;

public class PerfReportGenerator {

    private String reportDirectory = null;
    private String reportName = null;
    private PrintWriter writer = null;

    private Properties testSettings;

    public PerfReportGenerator() {
    }

    public PerfReportGenerator(String reportDirectory, String reportName) {
        this.setReportDirectory(reportDirectory);
        this.setReportName(reportName);
    }

    public void startGenerateReport() {

        setReportDirectory(this.getTestSettings().getProperty("sysTest.reportDirectory"));

        File reportDir = new File(getReportDirectory());

        // Create output directory if it doesn't exist.
        if (!reportDir.exists()) {
            reportDir.mkdirs();
        }

        File reportFile = null;
        if (reportDir != null) {
            String filename = (this.getReportName()).substring(this.getReportName().lastIndexOf(".") + 1) + "-" + createReportName(getTestSettings());
            reportFile = new File(this.getReportDirectory() + File.separator + filename + ".xml");
        }

        try {
            this.writer = new PrintWriter(new FileOutputStream(reportFile));
            addTestInformation(getWriter());
        } catch (IOException e1) {
            e1.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        }
    }

    public void stopGenerateReport() {
        this.getWriter().println("</test-result>");
        this.getWriter().println("</test-report>");
        this.getWriter().flush();
        this.getWriter().close();
    }

    protected void addTestInformation(PrintWriter writer) {

        writer.println("<test-report>");
        writer.println("<test-information>");

        writer.println("<os-name>" + System.getProperty("os.name") + "</os-name>");
        writer.println("<java-version>" + System.getProperty("java.version") + "</java-version>");

        if (this.getTestSettings() != null) {
            Enumeration keys = getTestSettings().propertyNames();

            writer.println("<client-settings>");

            String key;
            String key2;
            while (keys.hasMoreElements()) {
                key = (String) keys.nextElement();
                key2 = key.substring(key.indexOf(".") + 1);
                writer.println("<" + key2 + ">" + getTestSettings().get(key) + "</" + key2 + ">");
            }

            writer.println("</client-settings>");
        }

        writer.println("</test-information>");
        writer.println("<test-result>");
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

    public String createReportName(Properties testSettings) {
        if (testSettings != null) {
            String[] keys = {"client.destCount", "client.asyncRecv", "client.durable",
                             "client.messageSize", "sysTest.numClients", "sysTest.totalDests"};

            StringBuffer buffer = new StringBuffer();
            String key;
            String val;
            String temp;
            for (int i = 0; i < keys.length; i++) {
                key = keys[i];
                val = testSettings.getProperty(key);

                if (val == null) continue;

                temp = key.substring(key.indexOf(".") + 1);
                buffer.append(temp + val);
            }

            return buffer.toString();
        }
        return null;
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
