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
package org.apache.activemq.openwire;

import org.activeio.command.WireFormat;
import org.activeio.command.WireFormatFactory;

/**
 * @version $Revision$
 */
public class OpenWireFormatFactory implements WireFormatFactory {

    private int version=1;
    private boolean stackTraceEnabled=true;
    private boolean tcpNoDelayEnabled=false;
    private boolean cacheEnabled=true;

    public WireFormat createWireFormat() {
        OpenWireFormat format = new OpenWireFormat();
        format.setVersion(version);
        format.setStackTraceEnabled(stackTraceEnabled);
        format.setCacheEnabled(cacheEnabled);
        format.setTcpNoDelayEnabled(tcpNoDelayEnabled);
        return format;
    }

    public boolean isStackTraceEnabled() {
        return stackTraceEnabled;
    }

    public void setStackTraceEnabled(boolean stackTraceEnabled) {
        this.stackTraceEnabled = stackTraceEnabled;
    }

    public boolean isTcpNoDelayEnabled() {
        return tcpNoDelayEnabled;
    }

    public void setTcpNoDelayEnabled(boolean tcpNoDelayEnabled) {
        this.tcpNoDelayEnabled = tcpNoDelayEnabled;
    }

    public int getVersion() {
        return version;
    }

    public void setVersion(int version) {
        this.version = version;
    }

    public boolean isCacheEnabled() {
        return cacheEnabled;
    }

    public void setCacheEnabled(boolean cacheEnabled) {
        this.cacheEnabled = cacheEnabled;
    }
}
