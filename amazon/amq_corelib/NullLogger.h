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

#ifndef ACTIVEMQ_NULLLOGGER_H
#define ACTIVEMQ_NULLLOGGER_H

#include "Logger.h"

namespace ActiveMQ {
    /// default, "null" logger
    /**
       An instance of this class is used as the logger before one is
       set explicitly.  It reports all logging levels as disabled and
       all log operations are no-ops.
    */
    class NullLogger : public Logger {
    public:
        bool isEnabled(const LogLevel& l) { return false; }
        void logFatal(const std::string& msg) {}
        void logError(const std::string& msg) {}
        void logWarning(const std::string& msg) {}
        void logInform(const std::string& msg) {}
        void logDebug(const std::string& msg) {}
    };
};

#endif // ACTIVEMQ_NULLLOGGER_H
