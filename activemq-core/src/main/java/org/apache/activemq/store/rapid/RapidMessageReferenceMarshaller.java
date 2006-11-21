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

package org.apache.activemq.store.rapid;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.activeio.journal.active.Location;
import org.apache.activemq.command.MessageId;
import org.apache.activemq.kaha.Marshaller;

public class RapidMessageReferenceMarshaller  implements Marshaller{
    

    
    public Object readPayload(DataInput dataIn) throws IOException{
        MessageId mid = new MessageId(dataIn.readUTF());
        Location loc = new Location(dataIn.readInt(),dataIn.readInt());
        RapidMessageReference rmr = new RapidMessageReference(mid,loc);
        return rmr;
    }

    public void writePayload(Object object,DataOutput dataOut) throws IOException{
        RapidMessageReference rmr = (RapidMessageReference)object;
        dataOut.writeUTF(rmr.getMessageId().toString());
        dataOut.writeInt(rmr.getLocation().getLogFileId());
        dataOut.writeInt(rmr.getLocation().getLogFileOffset());
        
    }
}
