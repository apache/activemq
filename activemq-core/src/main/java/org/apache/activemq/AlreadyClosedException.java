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
package org.apache.activemq;

import javax.jms.JMSException;

/**
 * An exception which is closed if you try to access a resource which has already
 * been closed
 *
 * 
 */
public class AlreadyClosedException extends JMSException {

    private static final long serialVersionUID = -3203104889571618702L;

    public AlreadyClosedException() {
        super("this connection");
    }

    public AlreadyClosedException(String description) {
        super("Cannot use " + description + " as it has already been closed", "AMQ-1001");
    }
}
