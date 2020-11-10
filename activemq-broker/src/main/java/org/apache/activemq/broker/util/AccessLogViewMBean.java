package org.apache.activemq.broker.util;

import org.apache.activemq.broker.jmx.MBeanInfo;

public interface AccessLogViewMBean {
    @MBeanInfo("Enabled")
    public boolean isEnabled();

    public void setEnabled(@MBeanInfo("enabled") final boolean enabled);


    @MBeanInfo("Threshold timing to log")
    public int getThreshold();

    public void setThreshold(@MBeanInfo("threshold") final int threshold);

}
