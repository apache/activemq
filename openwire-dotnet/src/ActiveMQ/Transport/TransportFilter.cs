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
using JMS;
using System;

/// <summary>
/// Used to implement a filter on the transport layer.
/// </summary>
namespace ActiveMQ.Transport
{
	public class TransportFilter : ITransport
    {
		protected readonly ITransport next;
		protected CommandHandler command;
		protected ExceptionHandler exception;
		
		public TransportFilter(ITransport next) {
			this.next = next;
			this.next.Command = new CommandHandler(OnCommand);
			this.next.Exception = new ExceptionHandler(OnException);
		}
		
		protected virtual void OnCommand(ITransport sender, Command command) {
			this.command(sender, command);
		}
		
		protected virtual void OnException(ITransport sender, Exception command) {
			this.exception(sender, command);
		}
		
		
		/// <summary>
		/// Method Oneway
		/// </summary>
		/// <param name="command">A  Command</param>
		public virtual void Oneway(Command command)
		{
			this.next.Oneway(command);
		}
		
		/// <summary>
		/// Method AsyncRequest
		/// </summary>
		/// <returns>A FutureResponse</returns>
		/// <param name="command">A  Command</param>
		public virtual FutureResponse AsyncRequest(Command command)
		{
			return this.next.AsyncRequest(command);
		}
		
		/// <summary>
		/// Method Request
		/// </summary>
		/// <returns>A Response</returns>
		/// <param name="command">A  Command</param>
		public virtual Response Request(Command command)
		{
			return this.next.Request(command);
		}
		
		/// <summary>
		/// Method Start
		/// </summary>
		public virtual void Start()
		{
			if( command == null )
				throw new InvalidOperationException ("command cannot be null when Start is called.");
			if( exception == null )
				throw new InvalidOperationException ("exception cannot be null when Start is called.");
			this.next.Start();
		}
		
		/// <summary>
		/// Method Dispose
		/// </summary>
		public virtual void Dispose()
		{
			this.next.Dispose();
		}
		
		public CommandHandler Command {
            get { return command; }
            set { this.command = value; }
        }
		
        public  ExceptionHandler Exception {
            get { return exception; }
            set { this.exception = value; }
        }
		
    }
}

