package org.apache.activemq.tool.sampler;

import org.apache.activemq.tool.sampler.plugins.CpuSamplerPlugin;
import org.apache.activemq.tool.sampler.plugins.LinuxCpuSamplerPlugin;
import org.apache.activemq.tool.reports.AbstractPerfReportWriter;

import java.io.IOException;

public class CpuSamplerTask extends AbstractPerformanceSampler {

    private CpuSamplerPlugin plugin = null;

    public void createPlugin() throws IOException {
        createPlugin(System.getProperty("os.name"));
    }

    public void createPlugin(String osName) throws IOException {
        if (osName == null) {
            throw new IOException("No defined OS name found. Found: " + osName);
        }

        if (osName.equalsIgnoreCase(CpuSamplerPlugin.LINUX)) {
            plugin = new LinuxCpuSamplerPlugin(getInterval());
        } else {
            throw new IOException("No CPU Sampler Plugin found for OS: " + osName + ". CPU Sampler will not be started.");
        }
    }

    public void sampleData() {
        if (plugin != null && perfReportWriter != null) {
            perfReportWriter.writeCsvData(AbstractPerfReportWriter.REPORT_PLUGIN_CPU, "index=" + sampleIndex + "," + plugin.getCpuUtilizationStats());
        }
    }

    protected void onRampUpStart() {
        super.onRampUpStart();
        if (plugin != null) {
            plugin.start();
        }
    }

    protected void onRampDownEnd() {
        super.onRampDownEnd();
        if (plugin != null) {
            plugin.stop();
        }
    }
}
