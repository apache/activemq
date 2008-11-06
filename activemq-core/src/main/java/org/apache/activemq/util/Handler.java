package org.apache.activemq.util;

public interface Handler<T> {
	
	void handle(T e);

}
