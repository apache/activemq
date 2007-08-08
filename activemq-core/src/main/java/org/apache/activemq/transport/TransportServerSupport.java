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
package org.apache.activemq.transport;

import org.apache.activemq.util.ServiceSupport;

import java.net.URI;

/**
 * A useful base class for implementations of {@link TransportServer}
 * 
 * @version $Revision: 1.1 $
 */
public abstract class TransportServerSupport extends ServiceSupport implements TransportServer {

    private URI connectURI;
	private URI bindLocation;
    private TransportAcceptListener acceptListener;

    public TransportServerSupport() {
    }

    public TransportServerSupport(URI location) {
        this.connectURI = location;
        this.bindLocation = location;
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
    public URI getConnectURI() {
        return connectURI;
    }

    /**
     * @param location
     *            The location to set.
     */
    public void setConnectURI(URI location) {
        this.connectURI = location;
    }

    protected void onAcceptError(Exception e) {
        if (acceptListener != null) {
            acceptListener.onAcceptError(e);
        }
    }

	public URI getBindLocation() {
		return bindLocation;
	}

	public void setBindLocation(URI bindLocation) {
		this.bindLocation = bindLocation;
	}
}
