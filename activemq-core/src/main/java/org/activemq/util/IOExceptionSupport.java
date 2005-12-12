/**
* <a href="http://activemq.org">ActiveMQ: The Open Source Message Fabric</a>
*
* Copyright 2005 (C) LogicBlaze, Inc. http://www.logicblaze.com
*
* Licensed under the Apache License, Version 2.0 (the "License");
* you may not use this file except in compliance with the License.
* You may obtain a copy of the License at
*
* http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*
**/
package org.activemq.util;

import java.io.IOException;

final public class IOExceptionSupport {

    public static IOException create(String msg, Throwable cause) {
        IOException exception = new IOException(msg);
        exception.initCause(cause);
        return exception;
    }

    public static IOException create(String msg, Exception cause) {
        IOException exception = new IOException(msg);
        exception.initCause(cause);
        return exception;
    }
    
    public static IOException create(Throwable cause) {
        IOException exception = new IOException(cause.getMessage());
        exception.initCause(cause);
        return exception;
    }

    public static IOException create(Exception cause) {
        IOException exception = new IOException(cause.getMessage());
        exception.initCause(cause);
        return exception;
    }
    
}
