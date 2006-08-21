/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
