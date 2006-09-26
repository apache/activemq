/**
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.activeio.journal;

/**
 * Exception thrown by a Journal to indicate that an invalid RecordLocation was detected.
 * 
 * @version $Revision: 1.1 $
 */
public class InvalidRecordLocationException extends Exception {

	/**
     * Comment for <code>serialVersionUID</code>
     */
    private static final long serialVersionUID = 3618414947307239475L;

    /**
	 * 
	 */
	public InvalidRecordLocationException() {
		super();
	}

	/**
	 * @param msg
	 */
	public InvalidRecordLocationException(String msg) {
		super(msg);
	}

	/**
	 * @param msg
	 * @param rootCause
	 */
	public InvalidRecordLocationException(String msg, Throwable rootCause) {
		super(msg, rootCause);
	}
	
	/**
	 * @param rootCause
	 */
	public InvalidRecordLocationException(Throwable rootCause) {
		super(rootCause);
	}
}
