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
#include "SimpleLogger.h"

#include <iostream>
#include <activemq/logger/LogWriter.h>

using namespace activemq;
using namespace activemq::logger;
using namespace activemq::concurrent;
using namespace std;

////////////////////////////////////////////////////////////////////////////////
SimpleLogger::SimpleLogger(const std::string& name)
{
   this->name = name;
}

////////////////////////////////////////////////////////////////////////////////
SimpleLogger::~SimpleLogger()
{}

////////////////////////////////////////////////////////////////////////////////
void SimpleLogger::mark(const std::string& message)
{
   LogWriter::getInstance().log("", 0, "", message);
}

////////////////////////////////////////////////////////////////////////////////
void SimpleLogger::debug(const std::string& file,
                   const int          line,
                   const std::string& message)
{
   LogWriter::getInstance().log(file, line, "DEBUG:", message);
}

////////////////////////////////////////////////////////////////////////////////
void SimpleLogger::info(const std::string& file,
                  const int          line,
                  const std::string& message)
{
   LogWriter::getInstance().log(file, line, "INFO:", message);
}

////////////////////////////////////////////////////////////////////////////////
void SimpleLogger::warn(const std::string& file,
                  const int          line,
                  const std::string& message)
{
   LogWriter::getInstance().log(file, line, "WARNING:", message);
}

////////////////////////////////////////////////////////////////////////////////
void SimpleLogger::error(const std::string& file,
                   const int          line,
                   const std::string& message)
{
   LogWriter::getInstance().log(file, line, "ERROR:", message);
}

////////////////////////////////////////////////////////////////////////////////
void SimpleLogger::fatal(const std::string& file,
                   const int          line,
                   const std::string& message)
{
   LogWriter::getInstance().log(file, line, "FATAL:", message);
}

////////////////////////////////////////////////////////////////////////////////
void SimpleLogger::log(const std::string& message)
{
   LogWriter::getInstance().log(message);
}

