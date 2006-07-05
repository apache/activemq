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
#include "Logger.h"

#include <activemq/logger/Handler.h>
#include <algorithm>

using namespace std;
using namespace activemq;
using namespace activemq::logger;
using namespace activemq::exceptions;

////////////////////////////////////////////////////////////////////////////////
Logger::Logger(const std::string& name, Logger* parent)
{
}

////////////////////////////////////////////////////////////////////////////////
Logger::~Logger(void)
{
}

////////////////////////////////////////////////////////////////////////////////
void Logger::addHandler(Handler* handler) throw ( IllegalArgumentException )
{
    if(handler == NULL)
    {
        IllegalArgumentException(
            __FILE__, __LINE__, 
            "Logger::addHandler - HAndler cannot be null");
    }
    
    if(find(handlers.begin(), handlers.end(), handler) != handlers.end())
    {
        handlers.push_back(handler);
    }
}


////////////////////////////////////////////////////////////////////////////////
void Logger::removeHandler(Handler* handler)
{
    list<Handler*>::iterator itr = 
        find(handlers.begin(), handlers.end(), handler);

    if(itr != handlers.end())
    {
        delete *itr;
        handlers.erase(itr);
    }
}

////////////////////////////////////////////////////////////////////////////////
void Logger::setFilter(Filter* filter)
{
}

////////////////////////////////////////////////////////////////////////////////
bool Logger::isLoggable(Level level) const
{
    return false;
}

////////////////////////////////////////////////////////////////////////////////
void Logger::entry(const std::string& blockName, 
                   const std::string& file, 
                   const int line)
{
}

////////////////////////////////////////////////////////////////////////////////
void Logger::exit(const std::string& blockName, 
                  const std::string& file, 
                  const int line)
{
}

////////////////////////////////////////////////////////////////////////////////
void Logger::debug(const std::string& file, 
                   const int line, 
                   const std::string fnctionName, 
                   const std::string& message)
{
}

////////////////////////////////////////////////////////////////////////////////
void Logger::info(const std::string& file, 
                  const int line, 
                  const std::string fnctionName, 
                  const std::string& message)
{
}

////////////////////////////////////////////////////////////////////////////////
void Logger::error(const std::string& file, 
                   const int line, 
                   const std::string fnctionName,
                   const std::string& message)
{
}

////////////////////////////////////////////////////////////////////////////////
void Logger::warn(const std::string& file, 
                  const int line, 
                  const std::string fnctionName, 
                  const std::string& message)
{
}

////////////////////////////////////////////////////////////////////////////////
void Logger::fatal(const std::string& file, 
                   const int line, 
                   const std::string fnctionName, 
                   const std::string& message)
{
}

////////////////////////////////////////////////////////////////////////////////
void Logger::log(Level level, const std::string& message)
{
}

////////////////////////////////////////////////////////////////////////////////
void Logger::log(Level level, 
                 const std::string& file, 
                 const int line, 
                 const std::string& message, 
                 cms::CMSException& ex)
{
}

////////////////////////////////////////////////////////////////////////////////
void Logger::log(Level level, 
                 const std::string& file, 
                 const int line, 
                 const std::string& message, ...)
{
}

////////////////////////////////////////////////////////////////////////////////
void Logger::log(LogRecord& record)
{
}

////////////////////////////////////////////////////////////////////////////////
Logger* Logger::getLogger(const std::string& name)
{
    return NULL;
}

////////////////////////////////////////////////////////////////////////////////
Logger* Logger::getAnonymousLogger(void)
{
    return NULL;
}
