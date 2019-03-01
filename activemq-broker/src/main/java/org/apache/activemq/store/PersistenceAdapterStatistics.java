package org.apache.activemq.store;

import org.apache.activemq.management.StatsImpl;
import org.apache.activemq.management.TimeStatisticImpl;

public class PersistenceAdapterStatistics extends StatsImpl {
    protected TimeStatisticImpl writeTime;
    protected TimeStatisticImpl readTime;

    public PersistenceAdapterStatistics() {
        writeTime = new TimeStatisticImpl("writeTime", "Time to write data to the PersistentAdapter.");
        readTime = new TimeStatisticImpl("readTime", "Time to read data from the PersistentAdapter.");
        addStatistic("writeTime", writeTime);
        addStatistic("readTime", readTime);
    }

    public void addWriteTime(final long time) {
        writeTime.addTime(time);
    }

    public void addReadTime(final long time) {
        readTime.addTime(time);
    }

    public void setEnabled(boolean enabled) {
        super.setEnabled(enabled);
        writeTime.setEnabled(enabled);
        readTime.setEnabled(enabled);
    }

    public TimeStatisticImpl getWriteTime() {
        return writeTime;
    }

    public TimeStatisticImpl getReadTime() { return readTime; }

    public void reset() {
        if (isDoReset()) {
            writeTime.reset();
            readTime.reset();
        }
    }

    public void setParent(PersistenceAdapterStatistics parent) {
        if (parent != null) {
            writeTime.setParent(parent.writeTime);
            readTime.setParent(parent.readTime);
        } else {
            writeTime.setParent(null);
            readTime.setParent(null);
        }

    }
}
