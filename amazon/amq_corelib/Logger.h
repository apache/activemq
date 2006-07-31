/*
  Licensed to the Apache Software Foundation (ASF) under one
  or more contributor license agreements.  See the NOTICE file
  distributed with this work for additional information
  regarding copyright ownership.  The ASF licenses this file
  to you under the Apache License, Version 2.0 (the
  "License"); you may not use this file except in compliance
  with the License.  You may obtain a copy of the License at
  
  http://www.apache.org/licenses/LICENSE-2.0
  
  Unless required by applicable law or agreed to in writing,
  software distributed under the License is distributed on an
  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  KIND, either express or implied.  See the License for the
  specific language governing permissions and limitations
  under the License.
*/

#ifndef ACTIVEMQ_LOGGER_H
#define ACTIVEMQ_LOGGER_H

#include <string>

#include "LogLevel.h"

namespace ActiveMQ {
    /// Log event handler interface
    /**
       This class represents a handler for log events.  It is a way
       for the library to access your local logging policy.
    */
    class Logger {
    public:
        /// Test for a log level being enabled
        virtual bool isEnabled(const LogLevel& level) = 0;
        
        /// Log at FATAL
        virtual void logFatal(const std::string& msg) = 0;

        /// Log at ERROR
        virtual void logError(const std::string& msg) = 0;

        /// Log at WARNING
        virtual void logWarning(const std::string& msg) = 0;

        /// Log at INFORM
        virtual void logInform(const std::string& msg) = 0;

        /// Log at DEBUG
        virtual void logDebug(const std::string& msg) = 0;
    };
};

#endif // ACTIVEMQ_LOGGER_H
