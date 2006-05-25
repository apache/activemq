package org.apache.activemq.kaha.impl;

public interface RedoListener {

    void onRedoItem(DataItem item, Object object) throws Exception;

}
