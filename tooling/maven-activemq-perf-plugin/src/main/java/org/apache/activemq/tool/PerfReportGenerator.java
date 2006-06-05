package org.apache.activemq.tool;

import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Properties;

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
    private Properties clientSetting;

    public PerfReportGenerator() {
    }

    public PerfReportGenerator(String reportDirectory, String reportName) {
        this.setReportDirectory(reportDirectory);
        this.setReportName(reportName);
    }

    public void startGenerateReport() {

        setReportDirectory(reportDirectory);
        setReportName(reportName);

        File reportDir = new File(getReportDirectory());

        // Create output directory if it doesn't exist.
        if (!reportDir.exists()) {
            reportDir.mkdirs();
        }

        File reportFile = null;
        if (reportDir != null) {
            reportFile = new File(reportDirectory + File.separator + reportName + ".xml");
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

    public void setReportName(String reportName) {
        this.reportName = reportName;
    }
}
