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
#include "LogWriter.h"

#include <iostream>
#include <activemq/concurrent/Thread.h>
#include <activemq/concurrent/Concurrent.h>
#include <activemq/concurrent/Mutex.h>

using namespace activemq;
using namespace activemq::logger;
using namespace activemq::concurrent;
using namespace std;

////////////////////////////////////////////////////////////////////////////////
concurrent::Mutex LogWriter::mutex;

////////////////////////////////////////////////////////////////////////////////
LogWriter::LogWriter(void)
{
}

////////////////////////////////////////////////////////////////////////////////
LogWriter::~LogWriter(void)
{
}

////////////////////////////////////////////////////////////////////////////////
void LogWriter::log(const std::string& file,
                    const int          line,
                    const std::string& prefix,
                    const std::string& message)
{
   synchronized(&mutex)
   {
      cout << prefix  << " "
           << message << " - tid: " << Thread::getId() << endl;   
   }
}

////////////////////////////////////////////////////////////////////////////////
void LogWriter::log(const std::string& message)
{
   synchronized(&mutex)
   {
      cout << message << " - tid: " << Thread::getId() << endl;   
   }
}

////////////////////////////////////////////////////////////////////////////////
LogWriter& LogWriter::getInstance(void)
{
    // This one instance
    static LogWriter instance;
      
    return instance;
}

