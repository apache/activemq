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
using System;
using System.Threading;

namespace ActiveMQ.Transport
{
	
	/// <summary>
	/// Handles asynchronous responses
	/// </summary>
	public class FutureResponse : IAsyncResult
    {
        
        private Response response;
        private Mutex asyncWaitHandle = new Mutex();
        private Object semaphore = new Object();
        private int maxWait = 3000;
        private bool isCompleted;
        
        public WaitHandle AsyncWaitHandle
        {
            get { return asyncWaitHandle; }
        }
        
        public object AsyncState
        {
            get { return response; }
            set { Response = (Response) value; }
        }
        
        public bool IsCompleted
        {
            get { return isCompleted; }
        }
        
        public bool CompletedSynchronously
        {
            get { return false; }
        }
        
        public Response Response
        {
            // Blocks the caller until a value has been set
            get {
                while (response == null)
                {
                    try
					{
						lock (semaphore)
						{
							Monitor.Wait(semaphore, maxWait);
						}
                    }
                    catch (Exception e)
					{
                        Console.WriteLine("Caught while waiting on monitor: " + e);
                    }
                }
                return response;
            }
            set {
                lock (semaphore)
                {
                    response = value;
                    isCompleted = true;
                    Monitor.PulseAll(semaphore);
                }
            }
        }
    }
}

