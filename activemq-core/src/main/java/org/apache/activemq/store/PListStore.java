package org.apache.activemq.store;

import org.apache.activemq.Service;
import org.apache.activemq.store.kahadb.plist.PListImpl;

import java.io.File;

/**
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
public interface PListStore extends Service {
    File getDirectory();

    void setDirectory(File directory);

    PListImpl getPList(String name) throws Exception;

    boolean removePList(String name) throws Exception;

    long size();
}
