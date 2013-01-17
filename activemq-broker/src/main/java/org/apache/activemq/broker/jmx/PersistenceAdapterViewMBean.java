package org.apache.activemq.broker.jmx;

public interface PersistenceAdapterViewMBean {

    @MBeanInfo("Name of this persistence adapter.")
    String getName();

    @MBeanInfo("Current inflight local transactions.")
    String getTransactions();

    @MBeanInfo("Current data.")
    String getData();

    @MBeanInfo("Current size.")
    long getSize();
}
