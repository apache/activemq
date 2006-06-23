package org.apache.activemq.tool.sampler;

public interface PerformanceEventListener {
	public void onRampUpStart(PerformanceSampler sampler);
	public void onSamplerStart(PerformanceSampler sampler);
	public void onSamplerEnd(PerformanceSampler sampler);
	public void onRampDownEnd(PerformanceSampler sampler);
}
