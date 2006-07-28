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
package org.apache.activeio.xnet;

/**
 * 
 */
public class ServiceException extends Exception {

    /**
     * <p/>
     * Default constructor, which simply delegates exception
     * handling up the inheritance chain to <code>Exception</code>.
     * </p>
     */
    public ServiceException() {
        super();
    }

    /**
     * <p/>
     * This constructor allows a message to be supplied indicating the source
     * of the problem that occurred.
     * </p>
     *
     * @param message <code>String</code> identifying the cause of the problem.
     */
    public ServiceException(String message) {
        super(message);
    }

    /**
     * <p/>
     * This constructor allows a "root cause" exception to be supplied,
     * which may later be used by the wrapping application.
     * </p>
     *
     * @param rootCause <code>Throwable</code> that triggered the problem.
     */
    public ServiceException(Throwable rootCause) {
        super(rootCause);
    }

    /**
     * This constructor allows both a message identifying the
     * problem that occurred as well as a "root cause" exception
     * to be supplied, which may later be used by the wrapping
     * application.
     *
     * @param message   <code>String</code> identifying the cause of the problem.
     * @param rootCause <code>Throwable</code> that triggered this problem.
     */
    public ServiceException(String message, Throwable rootCause) {
        super(message, rootCause);
    }

}

