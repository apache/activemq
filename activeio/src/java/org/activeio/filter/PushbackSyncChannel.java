/**
 * 
 * Copyright 2004 Hiram Chirino
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
package org.activeio.filter;

import java.io.IOException;

import org.activeio.FilterSyncChannel;
import org.activeio.Packet;
import org.activeio.SyncChannel;

/**
 *
 */
public class PushbackSyncChannel extends FilterSyncChannel {

    private Packet putback;

    public PushbackSyncChannel(SyncChannel next) {
        this(next, null);
    }
    
    public PushbackSyncChannel(SyncChannel next, Packet putback) {
        super(next);
        this.putback=putback;
    }
    
    public void putback(Packet packet) {
        this.putback = packet;
    }
    
    public Packet read(long timeout) throws IOException {
        if(putback!=null ) {
            Packet p = putback;
            putback=null;
            return p;
        }
        return super.read(timeout);
    }

}
