package org.apache.activemq.store;

import org.apache.activemq.Service;

import java.io.File;

/**
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
public interface PListStore extends Service {
    File getDirectory();

    void setDirectory(File directory);

    PList getPList(String name) throws Exception;

    boolean removePList(String name) throws Exception;

    long size();
}
