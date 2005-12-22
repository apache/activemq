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
package org.activeio.command;

import org.activeio.Channel;

import java.io.IOException;

/**
 * Allows command objects to be written into a channel
 *
 * @version $Revision: 1.1 $
 */
public interface AsyncCommandChannel extends Channel {

    /**
     * Sends a command down the channel towards the media, using a WireFormat
     * to decide how to marshal the command onto the media.
     *
     * @param command
     * @throws java.io.IOException
     */
    void writeCommand(Object command) throws IOException;

    /**
     * Allows a listener to be added for commands
     * 
     * @param listener
     */
    void setCommandListener(CommandListener listener);
}
