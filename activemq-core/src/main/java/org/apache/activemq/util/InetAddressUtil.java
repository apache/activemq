package org.apache.activemq.util;

import java.net.InetAddress;
import java.net.UnknownHostException;

public class InetAddressUtil {

	/**
	 * When using the {@link java.net.InetAddress#getHostName()} method in an 
	 * environment where neither a proper DNS lookup nor an <tt>/etc/hosts</tt> 
	 * entry exists for a given host, the following exception will be thrown: 
	 * <code>
	 * java.net.UnknownHostException: &lt;hostname&gt;: &lt;hostname&gt;
     *  at java.net.InetAddress.getLocalHost(InetAddress.java:1425)
     *   ...
	 * </code>
	 * Instead of just throwing an UnknownHostException and giving up, this 
	 * method grabs a suitable hostname from the exception and prevents the 
	 * exception from being thrown. If a suitable hostname cannot be acquired
	 * from the exception, only then is the <tt>UnknownHostException</tt> thrown. 
	 * 
	 * @return The hostname 
	 * @throws UnknownHostException
	 * @see {@link java.net.InetAddress#getLocalHost()}
	 * @see {@link java.net.InetAddress#getHostName()}
	 */
	public static String getLocalHostName() throws UnknownHostException {
		try {
			return (InetAddress.getLocalHost()).getHostName();
		} catch (UnknownHostException uhe) {
			String host = uhe.getMessage(); // host = "hostname: hostname"
			if (host != null) {
				int colon = host.indexOf(':');
				if (colon > 0) {
					return host.substring(0, colon);
				}
			}
			throw uhe;
		}
	}
}
