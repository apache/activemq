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

#include "LogLevel.h"
#include "RCSID.h"

using ActiveMQ::LogLevel;

RCSID(LogLevel, "$Id$");

static const char *loglevel_strings[] = {
    "FATAL",
    "ERROR",
    "WARNING",
    "INFORM",
    "DEBUG"};

static const int loglevel_num = 5;

LogLevel::LogLevel(int n) {
    if (n >= loglevel_num)
        desc_ = "INVALID";
    else
        desc_ = loglevel_strings[n];
}

const char *
LogLevel::toString() const {
    return desc_;
}

const LogLevel LogLevel::Fatal = LogLevel(0);
const LogLevel LogLevel::Error = LogLevel(1);
const LogLevel LogLevel::Warning = LogLevel(2);
const LogLevel LogLevel::Inform = LogLevel(3);
const LogLevel LogLevel::Debug = LogLevel(4);
