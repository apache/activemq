package org.apache.activemq.broker.util;

public class AccessLogView implements AccessLogViewMBean {

    private final AccessLogPlugin plugin;

    public AccessLogView(final AccessLogPlugin plugin) {
        this.plugin = plugin;
    }

    @Override
    public boolean isEnabled() {
        return plugin.isEnabled();
    }

    @Override
    public void setEnabled(final boolean enabled) {
        plugin.setEnabled(enabled);
    }

    @Override
    public int getThreshold() {
        return plugin.getThreshold();
    }

    @Override
    public void setThreshold(final int threshold) {
        plugin.setThreshold(threshold);
    }
}
