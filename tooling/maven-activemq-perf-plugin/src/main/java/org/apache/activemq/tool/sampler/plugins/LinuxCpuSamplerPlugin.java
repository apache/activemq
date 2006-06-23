package org.apache.activemq.tool.sampler.plugins;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.StringTokenizer;

public class LinuxCpuSamplerPlugin implements CpuSamplerPlugin {

	private String vmstat = "vmstat";
	
	public String getCpuUtilizationStats() {
		try {
			Process p = Runtime.getRuntime().exec(vmstat);
			BufferedReader br = new BufferedReader(new InputStreamReader(p.getInputStream()), 1024);
			
			br.readLine(); // throw away the first line
			
			String header = br.readLine();
			String data   = br.readLine();
			
			br.close();
			
			// Convert to CSV of key=value pair
			return convertToCSV(header, data);
		} catch (Exception e) {
			e.printStackTrace();
			return "";
		}
	}

	public String getVmstat() {
		return vmstat;
	}

	public void setVmstat(String vmstat) {
		this.vmstat = vmstat;
	}
	
	protected String convertToCSV(String header, String data) {
		StringTokenizer headerTokens = new StringTokenizer(header, " ");
		StringTokenizer dataTokens   = new StringTokenizer(data, " ");
		
		String csv = "";
		while (headerTokens.hasMoreTokens()) {
			csv += (headerTokens.nextToken() + "=" + dataTokens.nextToken() + ";");
		}
		
		return csv;
	}
}
