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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.Properties;
import java.util.List;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Iterator;
import java.util.StringTokenizer;
import java.io.File;
import java.io.PrintWriter;
import java.io.BufferedOutputStream;
import java.io.FileOutputStream;
import java.io.FileNotFoundException;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.FileInputStream;
import java.io.IOException;

public class XmlFilePerfReportWriter extends AbstractPerfReportWriter {
    private static final Log log = LogFactory.getLog(XmlFilePerfReportWriter.class);

    private File tempLogFile;
    private PrintWriter tempLogFileWriter;

    private File xmlFile;
    private PrintWriter xmlFileWriter;

    private String reportDir;
    private String reportName;

    private Map  testPropsMap;
    private List testPropsList;

    public XmlFilePerfReportWriter() {
        this("", "PerformanceReport.xml");
    }

    public XmlFilePerfReportWriter(String reportDir, String reportName) {
        this.testPropsMap  = new HashMap();
        this.testPropsList = new ArrayList();
        this.reportDir     = reportDir;
        this.reportName    = reportName;
    }

    public void openReportWriter() {
        if (tempLogFile == null) {
            tempLogFile = createTempLogFile();
        }

        try {
            // Disable auto-flush and allocate 100kb of buffer
            tempLogFileWriter = new PrintWriter(new BufferedOutputStream(new FileOutputStream(tempLogFile), 102400), false);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
    }

    public void closeReportWriter() {
        // Flush and close log file writer
        tempLogFileWriter.flush();
        tempLogFileWriter.close();

        writeToXml();
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

    public void writeInfo(String info) {
        tempLogFileWriter.println("[INFO]" + info);
    }

    public void writePerfData(String data) {
        tempLogFileWriter.println("[DATA]" + data);
    }

    public void writeProperties(String header, Properties props) {
        testPropsMap.put(header, props);
    }

    public void writeProperties(Properties props) {
        testPropsList.add(props);
    }

    protected File createTempLogFile() {
        File f = null;
        try {
            f = File.createTempFile("tmpPL", null);
        } catch (IOException e) {
            f = new File("tmpPL" + System.currentTimeMillis() + ".tmp");
        }
        f.deleteOnExit();
        return f;
    }

    protected File createXmlFile() {
        String filename = (getReportName().endsWith(".xml") ? getReportName() : (getReportName() + ".xml"));
        String path = (getReportDir() == null) ? "" : getReportDir();

        File f = new File(path + filename);
        return f;
    }

    protected void writeToXml() {
        try {
            xmlFile = createXmlFile();
            xmlFileWriter = new PrintWriter(new FileOutputStream(xmlFile));
            writeXmlHeader();
            writeXmlTestSettings();
            writeXmlLogFile();
            writeXmlPerfSummary();
            writeXmlFooter();
            xmlFileWriter.close();

            System.out.println("Created performance report: " + xmlFile.getAbsolutePath());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    protected void writeXmlHeader() {
        xmlFileWriter.println("<testResult>");
    }

    protected void writeXmlFooter() {
        xmlFileWriter.println("</testResult>");
    }

    protected void writeXmlTestSettings() {
        Properties props;
        // Write system settings
        writeMap("systemSettings", System.getProperties());

        // Write test settings
        for (Iterator i=testPropsMap.keySet().iterator(); i.hasNext();) {
            String key = (String)i.next();
            props = (Properties)testPropsMap.get(key);
            writeMap(key, props);
        }

        int count = 1;
        for (Iterator i=testPropsList.iterator(); i.hasNext();) {
            props = (Properties)i.next();
            writeMap("settings" + count++, props);
        }
    }

    protected void writeXmlLogFile() throws IOException {
        // Write throughput data
        xmlFileWriter.println("<property name='performanceData'>");
        xmlFileWriter.println("<list>");

        BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(tempLogFile)));
        String line;
        while ((line = reader.readLine()) != null) {
            if (line.startsWith("[DATA]")) {
                parsePerfCsvData(line.substring("[DATA]".length()));
            } else if (line.startsWith("[INFO]")) {
                xmlFileWriter.println("<value>" + line + "</value>");
            } else {
                xmlFileWriter.println("<value>[ERROR]" + line + "</value>");
            }
        }

        xmlFileWriter.println("</list>");
        xmlFileWriter.println("</property>");
    }

    protected void writeXmlPerfSummary() {
        Map summary = createPerfSummary(clientThroughputs);

        xmlFileWriter.println("<property name='perfSummary'>");
        xmlFileWriter.println("<props>");

        String val, clientName, clientVal;

        val = (String)summary.get(KEY_SYS_TOTAL_TP);
        System.out.println("System Total Throughput: " + val);
        xmlFileWriter.println("<prop key='" + KEY_SYS_TOTAL_TP + "'>" + val + "</prop>");

        val = (String)summary.get(KEY_SYS_TOTAL_CLIENTS);
        System.out.println("System Total Clients: " + val);
        xmlFileWriter.println("<prop key='" + KEY_SYS_TOTAL_CLIENTS + "'>" + val + "</prop>");

        val = (String)summary.get(KEY_SYS_AVE_TP);
        System.out.println("System Average Throughput: " + val);
        xmlFileWriter.println("<prop key='" + KEY_SYS_AVE_TP + "'>" + val + "</prop>");

        val = (String)summary.get(KEY_SYS_AVE_EMM_TP);
        System.out.println("System Average Throughput Excluding Min/Max: " + val);
        xmlFileWriter.println("<prop key='" + KEY_SYS_AVE_EMM_TP + "'>" + val + "</prop>");

        val = (String)summary.get(KEY_SYS_AVE_CLIENT_TP);
        System.out.println("System Average Client Throughput: " + val);
        xmlFileWriter.println("<prop key='" + KEY_SYS_AVE_CLIENT_TP + "'>" + val + "</prop>");

        val = (String)summary.get(KEY_SYS_AVE_CLIENT_EMM_TP);
        System.out.println("System Average Client Throughput Excluding Min/Max: " + val);
        xmlFileWriter.println("<prop key='" + KEY_SYS_AVE_CLIENT_EMM_TP + "'>" + val + "</prop>");

        val = (String)summary.get(KEY_MIN_CLIENT_TP);
        clientName = val.substring(0, val.indexOf("="));
        clientVal  = val.substring(val.indexOf("=") + 1);
        System.out.println("Min Client Throughput Per Sample: clientName=" + clientName + ", value=" + clientVal);
        xmlFileWriter.println("<prop key='" + KEY_MIN_CLIENT_TP + "'>clientName=" + clientName + ",value=" + clientVal + "</prop>");

        val = (String)summary.get(KEY_MAX_CLIENT_TP);
        clientName = val.substring(0, val.indexOf("="));
        clientVal  = val.substring(val.indexOf("=") + 1);
        System.out.println("Max Client Throughput Per Sample: clientName=" + clientName + ", value=" + clientVal);
        xmlFileWriter.println("<prop key='" + KEY_MAX_CLIENT_TP + "'>clientName=" + clientName + ",value=" + clientVal + "</prop>");

        val = (String)summary.get(KEY_MIN_CLIENT_TOTAL_TP);
        clientName = val.substring(0, val.indexOf("="));
        clientVal  = val.substring(val.indexOf("=") + 1);
        System.out.println("Min Client Total Throughput: clientName=" + clientName + ", value=" + clientVal);
        xmlFileWriter.println("<prop key='" + KEY_MIN_CLIENT_TOTAL_TP + "'>clientName=" + clientName + ",value=" + clientVal + "</prop>");

        val = (String)summary.get(KEY_MAX_CLIENT_TOTAL_TP);
        clientName = val.substring(0, val.indexOf("="));
        clientVal  = val.substring(val.indexOf("=") + 1);
        System.out.println("Max Client Total Throughput: clientName=" + clientName + ", value=" + clientVal);
        xmlFileWriter.println("<prop key='" + KEY_MAX_CLIENT_TOTAL_TP + "'>clientName=" + clientName + ",value=" + clientVal + "</prop>");

        val = (String)summary.get(KEY_MIN_CLIENT_AVE_TP);
        clientName = val.substring(0, val.indexOf("="));
        clientVal  = val.substring(val.indexOf("=") + 1);
        System.out.println("Min Average Client Throughput: clientName=" + clientName + ", value=" + clientVal);
        xmlFileWriter.println("<prop key='" + KEY_MIN_CLIENT_AVE_TP + "'>clientName=" + clientName + ",value=" + clientVal + "</prop>");

        val = (String)summary.get(KEY_MAX_CLIENT_AVE_TP);
        clientName = val.substring(0, val.indexOf("="));
        clientVal  = val.substring(val.indexOf("=") + 1);
        System.out.println("Max Average Client Throughput: clientName=" + clientName + ", value=" + clientVal);
        xmlFileWriter.println("<prop key='" + KEY_MAX_CLIENT_AVE_TP + "'>clientName=" + clientName + ",value=" + clientVal + "</prop>");

        val = (String)summary.get(KEY_MIN_CLIENT_AVE_EMM_TP);
        clientName = val.substring(0, val.indexOf("="));
        clientVal  = val.substring(val.indexOf("=") + 1);
        System.out.println("Min Average Client Throughput Excluding Min/Max: clientName=" + clientName + ", value=" + clientVal);
        xmlFileWriter.println("<prop key='" + KEY_MIN_CLIENT_AVE_EMM_TP + "'>clientName=" + clientName + ",value=" + clientVal + "</prop>");

        val = (String)summary.get(KEY_MAX_CLIENT_AVE_EMM_TP);
        clientName = val.substring(0, val.indexOf("="));
        clientVal  = val.substring(val.indexOf("=") + 1);
        System.out.println("Max Average Client Throughput Excluding Min/Max: clientName=" + clientName + ", value=" + clientVal);
        xmlFileWriter.println("<prop key='" + KEY_MAX_CLIENT_AVE_EMM_TP + "'>clientName=" + clientName + ",value=" + clientVal + "</prop>");

        xmlFileWriter.println("</props>");
        xmlFileWriter.println("</property>");
    }

    protected void writeMap(String name, Map map) {
        xmlFileWriter.println("<property name='" + name + "'>");
        xmlFileWriter.println("<props>");
        for (Iterator i=map.keySet().iterator(); i.hasNext();) {
            String propKey = (String)i.next();
            Object propVal = map.get(propKey);
            xmlFileWriter.println("<prop key='" + propKey + "'>" + propVal.toString() + "</prop>");
        }
        xmlFileWriter.println("</props>");
        xmlFileWriter.println("</property>");
    }

    protected void parsePerfCsvData(String csvData) {
        StringTokenizer tokenizer = new StringTokenizer(csvData, ",");
        String data, key, val, clientName = null;
        Long throughput = null;
        int index = -1;
        while (tokenizer.hasMoreTokens()) {
            data = tokenizer.nextToken();
            key  = data.substring(0, data.indexOf("="));
            val  = data.substring(data.indexOf("=") + 1);

            if (key.equalsIgnoreCase("clientName")) {
                clientName = val;
            } else if (key.equalsIgnoreCase("throughput")) {
                throughput = Long.valueOf(val);
            } else if (key.equalsIgnoreCase("index")) {
                index = Integer.parseInt(val);
            }
        }
        addToClientTPList(clientName, throughput);
        xmlFileWriter.println("<value index='" + index + "' clientName='" + clientName +
                              "'>" + throughput.longValue() + "</value>");
    }
}
