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
package org.apache.activemq.broker;

/**
 * This exception is thrown by the broker when you try to use it after it has been stopped.
 *  
 * @author chirino
 */
public class BrokerStoppedException extends IllegalStateException {

	private static final long serialVersionUID = -3435230276850902220L;

	public BrokerStoppedException() {
		super();
	}

	public BrokerStoppedException(String message, Throwable cause) {
		super(message);
		initCause(cause);
	}

	public BrokerStoppedException(String s) {
		super(s);
	}

	public BrokerStoppedException(Throwable cause) {
		initCause(cause);
	}

}
