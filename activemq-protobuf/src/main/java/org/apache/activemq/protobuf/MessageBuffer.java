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
package org.apache.activemq.protobuf;

import java.io.IOException;
import java.io.OutputStream;

public interface MessageBuffer<B, MB extends MessageBuffer> extends PBMessage<B, MB> {
   
    public int serializedSizeUnframed();
    
    public int serializedSizeFramed();

    public Buffer toUnframedBuffer();
    
    public Buffer toFramedBuffer();

    public byte[] toUnframedByteArray();
   
    public byte[] toFramedByteArray();
    
    public void writeUnframed(CodedOutputStream output) throws java.io.IOException;
    
    public void writeFramed(CodedOutputStream output) throws java.io.IOException;
    
    public void writeUnframed(OutputStream output) throws IOException;
    
    public void writeFramed(OutputStream output) throws java.io.IOException;    

}
