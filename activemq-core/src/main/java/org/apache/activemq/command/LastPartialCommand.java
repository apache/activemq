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
package org.apache.activemq.command;

import org.apache.activemq.state.CommandVisitor;

/**
 * Represents the end marker of a stream of {@link PartialCommand} instances.
 * 
 * @openwire:marshaller code="61"
 * @version $Revision$
 */
public class LastPartialCommand extends PartialCommand {

    public static final byte DATA_STRUCTURE_TYPE = CommandTypes.PARTIAL_LAST_COMMAND;

    public LastPartialCommand() {
    }

    public byte getDataStructureType() {
        return DATA_STRUCTURE_TYPE;
    }

    public Response visit(CommandVisitor visitor) throws Exception {
        throw new IllegalStateException("The transport layer should filter out LastPartialCommand instances but received: " + this);
    }

    /**
     * Lets copy across any transient fields from this command 
     * to the complete command when it is unmarshalled on the other end
     *
     * @param completeCommand the newly unmarshalled complete command
     */
    public void configure(Command completeCommand) {
        // copy across the transient properties added by the low level transport
        completeCommand.setFrom(getFrom());
    }
}
