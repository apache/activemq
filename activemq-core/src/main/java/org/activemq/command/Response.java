/**
 *
 * Copyright 2004 The Apache Software Foundation
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
package org.activemq.command;

import org.activemq.state.CommandVisitor;

/**
 * @openwire:marshaller
 * @version $Revision: 1.6 $
 */
public class Response extends BaseCommand {
    
    public static final byte DATA_STRUCTURE_TYPE=CommandTypes.RESPONSE;
    short correlationId;
    
    public byte getDataStructureType() {
        return DATA_STRUCTURE_TYPE;
    }

    /**
     * @openwire:property version=1
     */
    public short getCorrelationId() {
        return correlationId;
    }
    
    public void setCorrelationId(short responseId) {
        this.correlationId = responseId;
    }
    
    public boolean isResponse() {
        return true;
    }

    public boolean isException() {
        return false;
    }

    public Response visit(CommandVisitor visitor) throws Throwable {
        return null;
    }
}
