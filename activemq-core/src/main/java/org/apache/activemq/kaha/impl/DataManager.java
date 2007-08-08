package org.apache.activemq.kaha.impl;

import java.io.IOException;

import org.apache.activemq.kaha.Marshaller;
import org.apache.activemq.kaha.StoreLocation;
import org.apache.activemq.kaha.impl.data.RedoListener;

public interface DataManager {

    String getName();

    Object readItem(Marshaller marshaller, StoreLocation item) throws IOException;

    StoreLocation storeDataItem(Marshaller marshaller, Object payload) throws IOException;

    StoreLocation storeRedoItem(Object payload) throws IOException;

    void updateItem(StoreLocation location, Marshaller marshaller, Object payload) throws IOException;

    void recoverRedoItems(RedoListener listener) throws IOException;

    void close() throws IOException;

    void force() throws IOException;

    boolean delete() throws IOException;

    void addInterestInFile(int file) throws IOException;

    void removeInterestInFile(int file) throws IOException;

    void consolidateDataFiles() throws IOException;

    Marshaller getRedoMarshaller();

    void setRedoMarshaller(Marshaller redoMarshaller);

}
