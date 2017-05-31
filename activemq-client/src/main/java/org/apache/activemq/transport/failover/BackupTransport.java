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


package org.apache.activemq.transport.failover;

import org.apache.activemq.command.BrokerInfo;
import org.apache.activemq.transport.DefaultTransportListener;
import org.apache.activemq.transport.Transport;

import java.io.IOException;
import java.net.URI;

class BackupTransport extends DefaultTransportListener{
	private final FailoverTransport failoverTransport;
	private Transport transport;
	private URI uri;
	private boolean disposed;
	private BrokerInfo brokerInfo;

	BackupTransport(FailoverTransport ft){
		this.failoverTransport=ft;
	}

	@Override
    public void onException(IOException error) {
		this.disposed=true;
		if (failoverTransport!=null) {
			this.failoverTransport.reconnect(false);
		}
	}

	@Override
	public void onCommand(Object command) {
		if (command instanceof BrokerInfo) {
			brokerInfo = (BrokerInfo) command;
		}
	}

	public BrokerInfo getBrokerInfo() {
		return brokerInfo;
	}

	public Transport getTransport() {
		return transport;
	}
	public void setTransport(Transport transport) {
		this.transport = transport;
		this.transport.setTransportListener(this);
	}
	public URI getUri() {
		return uri;
	}
	public void setUri(URI uri) {
		this.uri = uri;
	}
	
	public boolean isDisposed() {
		return disposed || transport != null && transport.isDisposed();
	}
	
	public void setDisposed(boolean disposed) {
		this.disposed = disposed;
	}
	
	@Override
    public int hashCode() {
		return uri != null ? uri.hashCode():-1;
	}
	
	@Override
    public boolean equals(Object obj) {
		if (obj instanceof BackupTransport) {
			BackupTransport other = (BackupTransport) obj;
			return uri== null && other.uri==null || 
				(uri != null && other.uri != null && uri.equals(other.uri));
		}
		return false;
	}

    @Override
    public String toString() {
        return "Backup transport: " + uri;
    }
}
