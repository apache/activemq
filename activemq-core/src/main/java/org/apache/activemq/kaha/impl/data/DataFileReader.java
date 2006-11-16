package org.apache.activemq.kaha.impl.data;

import java.io.IOException;

import org.apache.activemq.kaha.Marshaller;
import org.apache.activemq.kaha.StoreLocation;

interface DataFileReader {

	/**
	 * Sets the size property on a DataItem and returns the type of item that this was 
	 * created as.
	 * 
	 * @param marshaller
	 * @param item
	 * @return
	 * @throws IOException
	 */
	byte readDataItemSize(DataItem item) throws IOException;

	Object readItem(Marshaller marshaller, StoreLocation item)
			throws IOException;

}