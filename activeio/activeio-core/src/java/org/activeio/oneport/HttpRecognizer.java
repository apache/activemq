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
package org.activeio.oneport;

import java.util.HashSet;

import org.activeio.packet.Packet;


public class HttpRecognizer implements ProtocolRecognizer {

    static private HashSet methods = new HashSet();
    static {
        // This list built using: http://www.w3.org/Protocols/HTTP/Methods.html
        methods.add("GET ");
        methods.add("PUT ");
        methods.add("POST ");
        methods.add("HEAD ");
        methods.add("LINK ");
        methods.add("TRACE ");
        methods.add("UNLINK ");
        methods.add("SEARCH ");
        methods.add("DELETE ");
        methods.add("CHECKIN ");
        methods.add("OPTIONS ");
        methods.add("CONNECT ");
        methods.add("CHECKOUT ");
        methods.add("SPACEJUMP ");
        methods.add("SHOWMETHOD ");
        methods.add("TEXTSEARCH ");        
    }
    
    static final public HttpRecognizer HTTP_RECOGNIZER = new HttpRecognizer();
    
    private HttpRecognizer() {}
    
    public boolean recognizes(Packet packet) {
        
        StringBuffer b = new StringBuffer(12);
        for (int i = 0; i < 11; i++) {
            int c = (char)packet.read();
            if( c == -1)
                return false;
            
            b.append((char)c);
            if(((char)c)==' ')
                break;
        }
        
        return methods.contains(b.toString());
    }
}