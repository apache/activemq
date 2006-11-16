package org.apache.activemq.kaha.impl.data;

import java.io.FileNotFoundException;
import java.io.IOException;

import org.apache.activemq.kaha.Marshaller;
import org.apache.activemq.kaha.StoreLocation;

interface DataFileWriter {

	/**
	 * @param marshaller
	 * @param payload
	 * @param data_item2 
	 * @return
	 * @throws IOException
	 * @throws FileNotFoundException
	 */
	public StoreLocation storeItem(Marshaller marshaller, Object payload,
			byte type) throws IOException;

	public void updateItem(StoreLocation location, Marshaller marshaller,
			Object payload, byte type) throws IOException;

	public void force(DataFile dataFile) throws IOException;

	public void close() throws IOException;
}