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
package org.apache.activemq.command;

import org.apache.activemq.state.CommandVisitor;

/**
 * Represents the end marker of a stream of {@link PartialCommand} instances.
 * 
 * @openwire:marshaller code="61"
 * @version $Revision$
 */
public class LastPartialCommand extends BaseCommand {

    public static final byte DATA_STRUCTURE_TYPE = CommandTypes.PARTIAL_LAST_COMMAND;

    public LastPartialCommand() {
    }

    public LastPartialCommand(boolean responseRequired) {
        setResponseRequired(responseRequired);
    }

    public byte getDataStructureType() {
        return DATA_STRUCTURE_TYPE;
    }

    public Response visit(CommandVisitor visitor) throws Exception {
        throw new IllegalStateException("The transport layer should filter out LastPartialCommand instances but received: " + this);
    }

    /**
     * Lets copy across the required fields from this last partial command to
     * the newly unmarshalled complete command
     *
     * @param completeCommand the newly unmarshalled complete command
     */
    public void configure(Command completeCommand) {
        // overwrite the commandId as the numbers change when we introduce 
        // fragmentation commands
        completeCommand.setCommandId(getCommandId());
        
        // copy across the transient properties
        completeCommand.setFrom(getFrom());

        // TODO should not be required as the large command would be marshalled with this property
        //completeCommand.setResponseRequired(isResponseRequired());
    }

}
