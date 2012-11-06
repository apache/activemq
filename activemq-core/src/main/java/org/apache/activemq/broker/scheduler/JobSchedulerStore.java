package org.apache.activemq.broker.scheduler;

import org.apache.activemq.Service;

import java.io.File;

/**
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
public interface JobSchedulerStore extends Service {
    File getDirectory();

    void setDirectory(File directory);

    long size();

    JobScheduler getJobScheduler(String name) throws Exception;

    boolean removeJobScheduler(String name) throws Exception;
}
