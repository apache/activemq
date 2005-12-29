/**
 *
 * Copyright 2005-2006 The Apache Software Foundation
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
 */
package org.apache.activemq.transport.stomp;

import org.apache.activemq.command.Command;

import java.util.Properties;

public class CommandEnvelope {
    
    private final Command command;
    private final Properties headers;
    private final ResponseListener responseListener;
    
    public CommandEnvelope(Command command, Properties headers) {
        this(command, headers, null);
    }
    
    public CommandEnvelope(Command command, Properties headers, ResponseListener responseListener) {
        this.command = command;
        this.headers = headers;
        this.responseListener = responseListener;
    }

    public Properties getHeaders() {
        return headers;
    }

    public Command getCommand() {
        return command;
    }

    public ResponseListener getResponseListener() {
        return responseListener;
    }
    
}
