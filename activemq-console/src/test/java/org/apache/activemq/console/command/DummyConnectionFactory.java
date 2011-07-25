package org.apache.activemq.console.command;

import java.net.URI;

import org.apache.activemq.ActiveMQConnectionFactory;

public class DummyConnectionFactory extends ActiveMQConnectionFactory {
	public DummyConnectionFactory() {
		super();
	}

	public DummyConnectionFactory(String userName, String password, String brokerURL) {
		super(userName, password, brokerURL);
	}

	public DummyConnectionFactory(String userName, String password, URI brokerURL) {
		super(userName, password, brokerURL);
	}

	public DummyConnectionFactory(String brokerURL) {
		super(brokerURL);
	}

	public DummyConnectionFactory(URI brokerURL) {
		super(brokerURL);
	}

}
