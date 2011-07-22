/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.activemq.console.command;

import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import junit.framework.TestCase;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.console.CommandContext;
import org.apache.activemq.console.formatter.CommandShellOutputFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.support.AbstractApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

public class TestAMQ3410 extends TestCase {
	public static class InvalidFactory extends ActiveMQConnectionFactory {
	}

	public static class NotAFactory {
	}

	public static class DummyFactory extends ActiveMQConnectionFactory {
		public DummyFactory() {
			super();
		}

		public DummyFactory(String userName, String password, String brokerURL) {
			super(userName, password, brokerURL);
		}

		public DummyFactory(String userName, String password, URI brokerURL) {
			super(userName, password, brokerURL);
		}

		public DummyFactory(String brokerURL) {
			super(brokerURL);
		}

		public DummyFactory(URI brokerURL) {
			super(brokerURL);
		}

	};

	private static final Logger LOG = LoggerFactory
			.getLogger(TestPurgeCommand.class);
	private static final Collection<String> DEFAULT_TOKENS = Arrays
			.asList(new String[] { "--amqurl", "tcp://localhost:61616",
					"FOO.QUEUE" });
	protected AbstractApplicationContext context;

	protected void setUp() throws Exception {
		super.setUp();

		context = createApplicationContext();

	}

	protected AbstractApplicationContext createApplicationContext() {
		return new ClassPathXmlApplicationContext("activemq.xml");
	}

	protected void tearDown() throws Exception {
		BrokerService broker = (BrokerService) context.getBean("localbroker");
		broker.stop();
		broker = (BrokerService) context.getBean("default");
		broker.stop();
		super.tearDown();
	}

	public void testNoFactorySet() throws Exception {
		AmqBrowseCommand command = new AmqBrowseCommand();
		CommandContext context = new CommandContext();

		context.setFormatter(new CommandShellOutputFormatter(System.out));

		command.setCommandContext(context);

		List<String> tokens = new ArrayList<String>();
		tokens.addAll(DEFAULT_TOKENS);
		command.execute(tokens);
		assertNotNull(command.getFactory());
	}

	public void testFactorySet() throws Exception {
		AmqBrowseCommand command = new AmqBrowseCommand();
		CommandContext context = new CommandContext();

		context.setFormatter(new CommandShellOutputFormatter(System.out));

		command.setCommandContext(context);

		List<String> tokens = new ArrayList<String>();
		tokens.addAll(DEFAULT_TOKENS);
		tokens.add("--factory");
		tokens
				.add("org.apache.activemq.console.command.TestAMQ3410.DummyFactory");
		command.execute(tokens);
		assertNotNull(command.getFactory());
	}

	public void testFactorySetWrong1() throws Exception {
		AmqBrowseCommand command = new AmqBrowseCommand();
		CommandContext context = new CommandContext();

		context.setFormatter(new CommandShellOutputFormatter(System.out));

		command.setCommandContext(context);

		List<String> tokens = new ArrayList<String>();
		tokens.addAll(DEFAULT_TOKENS);
		tokens.add("--factory");
		tokens
				.add("org.apache.activemq.console.command.TestAMQ3410.DoesntExistFactory");
		command.execute(tokens);
		assertNotNull(command.getFactory());
	}

	public void testFactorySetWrong2() throws Exception {
		AmqBrowseCommand command = new AmqBrowseCommand();
		CommandContext context = new CommandContext();

		context.setFormatter(new CommandShellOutputFormatter(System.out));

		command.setCommandContext(context);

		List<String> tokens = new ArrayList<String>();
		tokens.addAll(DEFAULT_TOKENS);
		tokens.add("--factory");
		tokens
				.add("org.apache.activemq.console.command.TestAMQ3410.InvalidFactory");
		command.execute(tokens);
		assertNotNull(command.getFactory());
	}

}
