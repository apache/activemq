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
using ActiveMQ;
using ActiveMQ.Commands;
using ActiveMQ.OpenWire;
using ActiveMQ.Transport;
using System;
using System.Collections;
using System.IO;
using System.Net;
using System.Net.Sockets;
using System.Threading;

namespace ActiveMQ.Transport.Tcp
{
	
	/// <summary>
	/// An implementation of ITransport that uses sockets to communicate with the broker
	/// </summary>
	public class TcpTransport : ITransport
    {
        private Socket socket;
        private OpenWireFormat wireformat = new OpenWireFormat();
        private BinaryReader socketReader;
        private BinaryWriter socketWriter;
        private Thread readThread;
        private bool started;
        volatile private bool closed;
        
        private CommandHandler commandHandler;
        private ExceptionHandler exceptionHandler;
        
        public TcpTransport(Socket socket)
        {
			this.socket = socket;
        }
        
        /// <summary>
        /// Method Start
        /// </summary>
        public void Start()
        {
            if (!started)
            {
				if( commandHandler == null )
					throw new InvalidOperationException ("command cannot be null when Start is called.");
				if( exceptionHandler == null )
					throw new InvalidOperationException ("exception cannot be null when Start is called.");
				
                started = true;
                
                NetworkStream networkStream = new NetworkStream(socket);
                socketWriter = new OpenWireBinaryWriter(networkStream);
                socketReader = new OpenWireBinaryReader(networkStream);
                
                // now lets create the background read thread
                readThread = new Thread(new ThreadStart(ReadLoop));
                readThread.Start();
                
                // lets send the wireformat we're using
				WireFormatInfo info = new WireFormatInfo();
				info.StackTraceEnabled=false;
				info.TightEncodingEnabled=false;
				info.TcpNoDelayEnabled=false;
				info.CacheEnabled=false;
				info.SizePrefixDisabled=false;
				
                Oneway(info);
            }
        }
        
		public void Oneway(Command command)
        {
			wireformat.Marshal(command, socketWriter);
			socketWriter.Flush();
        }
        
        public FutureResponse AsyncRequest(Command command)
        {
            throw new NotImplementedException("Use a ResponseCorrelator if you want to issue AsyncRequest calls");
        }
        
        public Response Request(Command command)
        {
            throw new NotImplementedException("Use a ResponseCorrelator if you want to issue Request calls");
        }
        
        public void Dispose()
        {
			closed = true;
			socket.Close();
			readThread.Join();
            socketWriter.Close();
            socketReader.Close();
        }
        
        public void ReadLoop()
        {
            while (!closed)
            {
                try
                {
                    Command command = (Command) wireformat.Unmarshal(socketReader);
					this.commandHandler(this, command);
                }
				catch (ObjectDisposedException)
                {
                    break;
                }
				catch ( Exception e) {
					if( e.GetBaseException() is ObjectDisposedException ) {
						break;
					}
					if( !closed ) {
						this.exceptionHandler(this,e);
					}
					break;
				}
            }
        }
                
        // Implementation methods
                
		public CommandHandler Command {
            get { return commandHandler; }
            set { this.commandHandler = value; }
        }
		
        public  ExceptionHandler Exception {
            get { return exceptionHandler; }
            set { this.exceptionHandler = value; }
        }
		
    }
}



