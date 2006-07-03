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
#ifndef _ACTIVEMQ_LOGGER_LOGRECORD_H_
#define _ACTIVEMQ_LOGGER_LOGRECORD_H_

#include <activemq/logger/LoggerCommon.h>

#include <string>

namespace activemq{
namespace logger{

   class LogRecord
   {
   private:
   
      // Level of this Record
      Level level;
      
      // Name of the source Logger
      std::string loggerName;
      
      // Name of the File that originated the Log
      std::string sourceFile;
      
      // Line in the source file where log occured
      unsigned long sourceLine;
      
      // The message to Log.
      std::string message;
      
      // The function Name where the log occured
      std::string functionName;
      
      // Time in Mills since UTC that this Record was logged
      unsigned long timeStamp;
      
      // Thread Id of the Thread that logged this Record
      unsigned long threadId;

   public:

      /**
       * Constructor
       */
      LogRecord() {}
      
      /**
       * Destructor
       */
      virtual ~LogRecord() {}
      
      /**
       * Get Level of this log record
       * @return Level enumeration value.
       */
      Level getLevel(void) const { return level; };
      
      /**
       * Set the Level of this Log Record
       * @param Level Enumeration Value
       */
      void setLevel(Level value) { level = value; };
      
      /**
       * Gets the Source Logger's Name
       * @return the source loggers name
       */
      const std::string& getLoggerName(void) const { return loggerName; };
      
      /**
       * Sets the Source Logger's Name
       * @param the source loggers name
       */
      void setLoggerName(const std::string& loggerName) { 
         this->loggerName = loggerName;
      };
      
      /**
       * Gets the Source Log File name
       * @return the source loggers name
       */
      const std::string& getSourceFile(void) const { return sourceFile; };
      
      /**
       * Sets the Source Log File Name
       * @param the source loggers name
       */
      void setSourceFile(const std::string& loggerName) { 
         this->sourceFile = sourceFile;
      };

      /**
       * Gets the Source Log line number
       * @return the source loggers line number
       */
      unsigned long getSourceLine(void) const { return sourceLine; };
      
      /**
       * Sets the Source Log line number
       * @param the source logger's line number
       */
      void setSourceLine(long sourceLine) { 
         this->sourceLine = sourceLine;
      };

      /**
       * Gets the Message to be Logged
       * @return the source logger's message
       */
      const std::string& getMessage(void) const { return message; };
      
      /**
       * Sets the Message to be Logged
       * @param the source loggers message
       */
      void setMessage(const std::string& message) { 
         this->message = message;
      };
      
      /**
       * Gets the name of the function where this log was logged
       * @return the source logger's message
       */
      const std::string& getSourceFunction(void) const { return functionName; };
      
      /**
       * Sets the name of the function where this log was logged
       * @param the source loggers message
       */
      void setSourceFunction(const std::string& functionName) { 
         this->functionName = functionName;
      };

      /**
       * Gets the time in mills that this message was logged.
       * @return UTC time in milliseconds
       */
      unsigned long getTimestamp(void) const { return timeStamp; };
      
      /**
       * Sets the time in mills that this message was logged.
       * @param UTC Time in Milliseconds.
       */
      void setTimestamp(long timeStamp) { 
         this->timeStamp = timeStamp;
      };

      /**
       * Gets the Thread Id where this Log was created
       * @return the source loggers line number
       */
      unsigned long getTreadId(void) const { return threadId; };
      
      /**
       * Sets the Thread Id where this Log was created
       * @param the source logger's line number
       */
      void setTreadId(long threadId) { 
         this->threadId = threadId;
      };
   };

}}

#endif /*_ACTIVEMQ_LOGGER_LOGRECORD_H_*/
