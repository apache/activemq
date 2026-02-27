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

abstract public class DeferredDecodeMessage<T> extends BaseMessage<T> {

    protected Buffer encodedForm;
    protected boolean decoded = true;

    @Override
    public T mergeFramed(CodedInputStream input) throws IOException {
        int length = input.readRawVarint32();
        int oldLimit = input.pushLimit(length);
        T rc = mergeUnframed(input.readRawBytes(length));
        input.popLimit(oldLimit);
        return rc;
    }

    @SuppressWarnings("unchecked")
    @Override
    public T mergeUnframed(Buffer data) throws InvalidProtocolBufferException {
        encodedForm = data;
        decoded = false;
        return (T) this;
    }

    @Override
    public Buffer toUnframedBuffer() {
        if (encodedForm == null) {
            encodedForm = super.toUnframedBuffer();
        }
        return encodedForm;
    }
    
    protected void load() {
        if (!decoded) {
            decoded = true;
            try {
                Buffer originalForm = encodedForm;
                encodedForm=null;
                CodedInputStream input = new CodedInputStream(originalForm);
                mergeUnframed(input);
                input.checkLastTagWas(0);
                // We need to reset the encoded form because the mergeUnframed
                // from a stream clears it out.
                encodedForm = originalForm;
                checktInitialized();
            } catch (Throwable e) {
                throw new RuntimeException("Deferred message decoding failed: " + e.getMessage(), e);
            }
        }
    }

    protected void loadAndClear() {
        super.loadAndClear();
        load();
        encodedForm = null;
    }

    public void clear() {
        super.clear();
        encodedForm = null;
        decoded = true;
    }

    public boolean isDecoded() {
        return decoded;
    }

    public boolean isEncoded() {
        return encodedForm != null;
    }

}
