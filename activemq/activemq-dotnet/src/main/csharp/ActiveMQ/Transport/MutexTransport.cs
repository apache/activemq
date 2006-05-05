/*
 * Copyright 2006 The Apache Software Foundation or its licensors, as
 * applicable.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
using ActiveMQ.Commands;
using ActiveMQ.Transport;
using System;

namespace ActiveMQ.Transport
{
	
	/// <summary>
	/// A Transport which gaurds access to the next transport using a mutex.
	/// </summary>
	public class MutexTransport : TransportFilter
    {
		
		private readonly object transmissionLock = new object();
		
		public MutexTransport(ITransport next) : base(next) {
		}
		
		
		public override void Oneway(Command command)
		{
            lock (transmissionLock)
            {
				this.next.Oneway(command);
			}
		}
		
		public override FutureResponse AsyncRequest(Command command)
		{
            lock (transmissionLock)
            {
				return base.AsyncRequest(command);
			}
		}
		
		public override Response Request(Command command)
		{
            lock (transmissionLock)
            {
				return base.Request(command);
			}
		}
		
		public override void Dispose()
		{
            lock (transmissionLock)
            {
				base.Dispose();
			}
		}
		
    }
}

