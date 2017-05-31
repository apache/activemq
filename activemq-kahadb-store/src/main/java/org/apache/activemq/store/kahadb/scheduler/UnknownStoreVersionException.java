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
package org.apache.activemq.store.kahadb.scheduler;

import java.io.IOException;

public class UnknownStoreVersionException extends IOException {

    private static final long serialVersionUID = -8544753506151157145L;

    private final String token;

    public UnknownStoreVersionException(Throwable cause) {
        super(cause);
        this.token = "";
    }

    public UnknownStoreVersionException(String token) {
        super("Failed to load Store, found unknown store token: " + token);
        this.token = token;
    }

    public String getToken() {
        return this.token;
    }
}
