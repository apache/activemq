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

import org.apache.activemq.tool.reports.PerformanceReportWriter;

public interface PerformanceSampler extends Runnable {
	public long getRampUpTime();
	public void setRampUpTime(long rampUpTime);
	public long getRampDownTime();
	public void setRampDownTime(long rampDownTime);
	public long getDuration();
	public void setDuration(long duration);
	public long getInterval();
	public void setInterval(long interval);
	public PerformanceReportWriter getPerfReportWriter();
	public void setPerfReportWriter(PerformanceReportWriter writer);
	public PerformanceEventListener getPerfEventListener();
	public void setPerfEventListener(PerformanceEventListener listener);

	public void sampleData();
	public boolean isRunning();
	public void waitUntilDone();
}
