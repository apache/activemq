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

import java.io.IOException;

import org.activeio.ByteSequence;
import org.activeio.command.WireFormat;

public interface MarshallAware {

    public void beforeMarshall(WireFormat wireFormat) throws IOException;
    public void afterMarshall(WireFormat wireFormat) throws IOException;
    
    public void beforeUnmarshall(WireFormat wireFormat) throws IOException;
    public void afterUnmarshall(WireFormat wireFormat) throws IOException;
    
    public void setCachedMarshalledForm(WireFormat wireFormat, ByteSequence data);
    public ByteSequence getCachedMarshalledForm(WireFormat wireFormat);
}
