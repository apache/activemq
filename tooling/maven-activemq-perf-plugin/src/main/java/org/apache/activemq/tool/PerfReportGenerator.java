package org.apache.activemq.tool;

import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Properties;
import java.util.Enumeration;
import java.util.Iterator;

/**
 * Created by IntelliJ IDEA.
 * User: admin
 * Date: Jun 5, 2006
 * Time: 10:57:52 AM
 * To change this template use File | Settings | File Templates.
 */
public class PerfReportGenerator {

    private String reportDirectory = null;
    private String reportName = null;
    private DataOutputStream dataOutputStream = null;

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
            String filename = (this.getReportName()).substring(this.getReportName().lastIndexOf(".")+1)+"-"+createReportName(getTestSettings());
            reportFile = new File(this.getReportDirectory() + File.separator + filename + ".xml");
        }

        try {
            dataOutputStream = new DataOutputStream(new FileOutputStream(reportFile));
            dataOutputStream.writeChars(getTestInformation().toString());
        } catch (IOException e1) {
            e1.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        }
    }

    public void stopGenerateReport() {
        try {
            dataOutputStream.writeChars("</test-result>\n</test-report>");
            dataOutputStream.flush();
            dataOutputStream.close();
        } catch (IOException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        }
    }

    protected String getTestInformation() {
        StringBuffer buffer = new StringBuffer();

        buffer.append("<test-report>\n");
        buffer.append("<test-information>\n");

        buffer.append("<os-name>" + System.getProperty("os.name") + "</os-name>\n");
        buffer.append("<java-version>" + System.getProperty("java.version") + "</java-version>\n");

        if(this.getTestSettings()!=null){
            Enumeration keys = getTestSettings().propertyNames();

            buffer.append("<client-settings>\n");

            String key;
            String key2;
            while(keys.hasMoreElements()){
                key = (String) keys.nextElement();
                key2 = key.substring(key.indexOf(".")+1);
                buffer.append("<" + key2 +">" + getTestSettings().get(key) + "</" + key2 +">\n");
            }

            buffer.append("</client-settings>\n");
        }

        buffer.append("</test-information>\n");
        buffer.append("<test-result>\n");

        return buffer.toString();
    }

    public DataOutputStream getDataOutputStream() {
        return this.dataOutputStream;
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
        if(testSettings!=null){
            String[] keys = {"client.destCount","consumer.asyncRecv","consumer.durable",
                             "producer.messageSize","sysTest.numClients","sysTest.totalDests"};

            StringBuffer buffer = new StringBuffer();
            String key;
            String val;
            String temp;
            for(int i=0;i<keys.length;i++){
                key = keys[i];
                val = testSettings.getProperty(key);

                if(val==null)continue;

                temp = key.substring(key.indexOf(".")+1);
                buffer.append(temp+val);
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
