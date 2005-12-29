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
package org.apache.activemq.transport;

import java.net.URI;

/**
 * A useful base class for implementations of {@link TransportServer}
 * 
 * @version $Revision: 1.1 $
 */
public abstract class TransportServerSupport implements TransportServer {

    private URI location;
    private TransportAcceptListener acceptListener;

    public TransportServerSupport() {
    }

    public TransportServerSupport(URI location) {
        this.location = location;
    }

    public URI getConnectURI() {
        return location;
    }

    /**
     * @return Returns the acceptListener.
     */
    public TransportAcceptListener getAcceptListener() {
        return acceptListener;
    }

    /**
     * Registers an accept listener
     * 
     * @param acceptListener
     */
    public void setAcceptListener(TransportAcceptListener acceptListener) {
        this.acceptListener = acceptListener;
    }

    /**
     * @return Returns the location.
     */
    public URI getLocation() {
        return location;
    }

    /**
     * @param location
     *            The location to set.
     */
    public void setLocation(URI location) {
        this.location = location;
    }

    protected void onAcceptError(Exception e) {
        if (acceptListener != null) {
            acceptListener.onAcceptError(e);
        }
    }
}
