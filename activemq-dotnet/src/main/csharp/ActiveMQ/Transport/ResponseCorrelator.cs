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

using System;
using System.Collections;

using ActiveMQ.Commands;
using ActiveMQ.Transport;

namespace ActiveMQ.Transport
{
	
	/// <summary>
	/// A Transport which gaurds access to the next transport using a mutex.
	/// </summary>
	public class ResponseCorrelator : TransportFilter
    {

        private readonly IDictionary requestMap = Hashtable.Synchronized(new Hashtable());
        private readonly Object mutex = new Object();
        private short nextCommandId;
		
		public ResponseCorrelator(ITransport next) : base(next) {
		}

		short GetNextCommandId() {
			lock(mutex) {
				return ++nextCommandId;
			}
		}
		
		public override void Oneway(Command command)
		{
			command.CommandId = GetNextCommandId();
			command.ResponseRequired = false;
			next.Oneway(command);
		}
		
		public override FutureResponse AsyncRequest(Command command)
		{
			command.CommandId = GetNextCommandId();
			command.ResponseRequired = true;
			FutureResponse future = new FutureResponse();
			requestMap[command.CommandId] = future;
			next.Oneway(command);
			return future;

		}
		
		public override Response Request(Command command)
		{
			FutureResponse future = AsyncRequest(command);
            Response response = future.Response;
            if (response is ExceptionResponse)
            {
                ExceptionResponse er = (ExceptionResponse) response;
                BrokerError brokerError = er.Exception;
                throw new BrokerException(brokerError);
            }
            return response;
		}
		
		protected override void OnCommand(ITransport sender, Command command)
		{
			if( command is Response ) {
				
				Response response = (Response) command;
				FutureResponse future = (FutureResponse) requestMap[response.CorrelationId];
				if (future != null)
				{
					if (response is ExceptionResponse)
					{
						ExceptionResponse er = (ExceptionResponse) response;
						BrokerError brokerError = er.Exception;
						this.exceptionHandler(this, new BrokerException(brokerError));
					}
					future.Response = response;
				}
				else
				{
					Console.WriteLine("ERROR: Unknown response ID: " + response.CommandId + " for response: " + response);
				}
			} else {
				this.commandHandler(sender, command);
			}
		}
		
    }
}

