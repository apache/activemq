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
using ActiveMQ.OpenWire;
using System;
using System.Collections;
using System.IO;
using System.Net;
using System.Net.Sockets;
using System.Threading;



/// <summary>
/// An implementation of ITransport that uses sockets to communicate with the broker
/// </summary>
namespace ActiveMQ.Transport
{
	public class SocketTransport : ITransport
    {
        private readonly object transmissionLock = new object();
        private Socket socket;
        private OpenWireFormat wireformat = new OpenWireFormat();
        private BinaryReader socketReader;
        private BinaryWriter socketWriter;
        private Thread readThread;
        private bool closed;
        private IDictionary requestMap = new Hashtable(); // TODO threadsafe
        private short nextCommandId;
        private bool started;
        
        public event CommandHandler Command;
        public event ExceptionHandler Exception;
        
        
        
        public SocketTransport(string host, int port)
        {
            //Console.WriteLine("Opening socket to: " + host + " on port: " + port);
            socket = Connect(host, port);
        }
        
        /// <summary>
        /// Method Start
        /// </summary>
        public void Start()
        {
            if (!started)
            {
                started = true;
                
                NetworkStream networkStream = new NetworkStream(socket);
                socketWriter = new BinaryWriter(networkStream);
                socketReader = new BinaryReader(networkStream);
                
                // now lets create the background read thread
                readThread = new Thread(new ThreadStart(ReadLoop));
                readThread.Start();
                
                // lets send the wireformat we're using
                Oneway(wireformat.WireFormatInfo);
            }
        }
        
        
        public void Oneway(Command command)
        {
            command.CommandId = GetNextCommandId();
            command.ResponseRequired = false;
            Send(command);
        }
        
        public FutureResponse AsyncRequest(Command command)
        {
            command.CommandId = GetNextCommandId();
            command.ResponseRequired = true;
            Send(command);
            FutureResponse future = new FutureResponse();
            requestMap[command.CommandId] = future;
            return future;
        }
        
        public Response Request(Command command)
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
        
        public void Dispose()
        {
            lock (transmissionLock)
            {
                socket.Close();
                closed = true;
            }
            socketWriter.Close();
            socketReader.Close();
        }
        
        public void ReadLoop()
        {
            while (!closed)
            {
                Command command = null;
                try
                {
                    command = (Command) wireformat.Unmarshal(socketReader);
                }
                catch (EndOfStreamException)
                {
                    // stream closed
                    break;
                }
                catch (ObjectDisposedException)
                {
                    // stream closed
                    break;
                }
                catch (IOException)
                {
                    // error, assume closing
                    break;
                }
                if (command is Response)
                {
                    Response response = (Response) command;
                    FutureResponse future = (FutureResponse) requestMap[response.CorrelationId];
                    if (future != null)
                    {
                        if (response is ExceptionResponse)
                        {
                            ExceptionResponse er = (ExceptionResponse) response;
                            BrokerError brokerError = er.Exception;
                            if (this.Exception != null)
                            {
                                this.Exception(this, new BrokerException(brokerError));
                            }
                        }
                        future.Response = response;
                    }
                    else
                    {
                        Console.WriteLine("ERROR: Unknown response ID: " + response.CommandId + " for response: " + response);
                    }
                }
                else
                {
                    if (this.Command != null)
                    {
                        this.Command(this, command);
                    }
                    else
                    {
                        Console.WriteLine("ERROR: No handler available to process command: " + command);
                    }
                }
            }
        }
        
        
        // Implementation methods
        
        protected void Send(Command command)
        {
            lock (transmissionLock)
            {
                //Console.WriteLine("Sending command: " + command  + " with ID: " + command.CommandId + " response: " + command.ResponseRequired);
                
                wireformat.Marshal(command, socketWriter);
                socketWriter.Flush();
            }
        }
        
        protected short GetNextCommandId()
        {
            lock (transmissionLock)
            {
                return++nextCommandId;
            }
        }
        
        protected Socket Connect(string host, int port)
        {
            // Looping through the AddressList allows different type of connections to be tried
            // (IPv4, IPv6 and whatever else may be available).
            IPHostEntry hostEntry = Dns.Resolve(host);
            foreach (IPAddress address in hostEntry.AddressList)
            {
                Socket socket = new Socket(
                    address.AddressFamily,
                    SocketType.Stream,
                    ProtocolType.Tcp);
                socket.Connect(new IPEndPoint(address, port));
                if (socket.Connected)
                {
                    return socket;
                }
            }
            throw new SocketException();
        }
    }
}


