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
package org.apache.activemq.kaha.impl.index;

import java.io.IOException;

/**
 * Occurs when bad magic occurs in reading a file
 * 
 * @version $Revision: 1.2 $
 */
public class BadMagicException extends IOException {
    /**
     * 
     */
    private static final long serialVersionUID = -570930196733067056L;

    /**
     * Default Constructor
     * 
     */
    public BadMagicException() {
        super();
    }

    /**
     * Construct an Exception with a reason
     * 
     * @param s
     */
    public BadMagicException(String s) {
        super(s);
    }
}
