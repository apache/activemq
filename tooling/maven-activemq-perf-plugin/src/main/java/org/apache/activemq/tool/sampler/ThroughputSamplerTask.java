package org.apache.activemq.tool.sampler;

import org.apache.activemq.tool.reports.AbstractPerfReportWriter;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class ThroughputSamplerTask extends AbstractPerformanceSampler {

    private final Object mutex = new Object();
    private List clients = new ArrayList();

    public void registerClient(MeasurableClient client) {
        synchronized (mutex) {
            clients.add(client);
        }
    }

	public void sampleData() {
		for (Iterator i = clients.iterator(); i.hasNext();) {
            MeasurableClient client = (MeasurableClient) i.next();
            if (perfReportWriter != null) {
            	perfReportWriter.writeCsvData(AbstractPerfReportWriter.REPORT_PLUGIN_THROUGHPUT,
                        "index=" + sampleIndex + ",clientName=" + client.getClientName() +
                        ",throughput=" + client.getThroughput());
            }
            client.reset();
        }
	}

    protected void onSamplerStart() {
        // Reset the throughput of the clients
        for (Iterator i = clients.iterator(); i.hasNext();) {
            MeasurableClient client = (MeasurableClient) i.next();
            client.reset();
        }
    }
}
