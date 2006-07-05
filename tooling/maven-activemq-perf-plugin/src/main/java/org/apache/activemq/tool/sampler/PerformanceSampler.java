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
