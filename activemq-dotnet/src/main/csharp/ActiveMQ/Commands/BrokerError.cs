/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
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
using System.IO;


namespace ActiveMQ.Commands
{
    public struct StackTraceElement
    {
        public string ClassName;
        public string FileName;
        public string MethodName;
        public int LineNumber;
    }

    /// <summary>
    /// Represents an exception on the broker
    /// </summary>
    public class BrokerError : BaseCommand
    {
        private string message;
        private string exceptionClass;
        private StackTraceElement[] stackTraceElements = {};
        private BrokerError cause;
        
        public string Message
        {
            get { return message; }
            set { message = value; }
        }
        
        public string ExceptionClass
        {
            get { return exceptionClass; }
            set { exceptionClass = value; }
        }
        
        public StackTraceElement[] StackTraceElements
        {
            get { return stackTraceElements; }
            set { stackTraceElements = value; }
        }
        
        public BrokerError Cause
        {
            get { return cause; }
            set { cause = value; }
        }
        
        public String StackTrace
        {
            get {
                StringWriter writer = new StringWriter();
                PrintStackTrace(writer);
                return writer.ToString();
            }
        }
        
        public void PrintStackTrace(TextWriter writer)
        {
            writer.WriteLine(exceptionClass + ": " + message);
            for (int i = 0; i < stackTraceElements.Length; i++)
            {
                StackTraceElement element = stackTraceElements[i];
                writer.WriteLine("    at " + element.ClassName + "." + element.MethodName + "(" + element.FileName + ":" + element.LineNumber + ")");
            }
            if (cause != null)
            {
                writer.WriteLine("Nested Exception:");
                cause.PrintStackTrace(writer);
            }
        }
    }
}

