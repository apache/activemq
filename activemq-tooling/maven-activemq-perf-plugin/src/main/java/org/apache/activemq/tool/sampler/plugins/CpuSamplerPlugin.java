package org.apache.activemq.tool.sampler.plugins;

public interface CpuSamplerPlugin {
	public final static String WINDOWS_2000 = "Windows 2000";
	public final static String WINDOWS_NT   = "Windows NT";
	public final static String WINDOWS_XP   = "Windows XP";
	public final static String WINDOWS_95   = "Windows 95";
	public final static String WINDOWS_CE   = "Windows CE";
	public final static String LINUX        = "Linux";
	public final static String SOLARIS      = "Solaris";
	public final static String AIX          = "AIX";
	public final static String FREEBSD      = "FreeBSD";
	public final static String MAC_OS       = "Mac OS";
	public final static String MAC_OS_X     = "Mac OS X";
	public final static String POWERPC      = "PowerPC";
	public final static String OS_2         = "OS/2";

	public String getCpuUtilizationStats();
    public void start();
    public void stop();
}
