package org.apache.activemq.replica;

public interface ActionListenerCallback {

    void onDeinitializationSuccess();

    void onFailOverAck() throws Exception;
  }