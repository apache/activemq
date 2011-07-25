package org.apache.activemq.console.command;

public class LowercasingPasswordFactory implements PasswordFactory {
	@Override
	public String getPassword(String password) {
		return password.toLowerCase();
	}

};
