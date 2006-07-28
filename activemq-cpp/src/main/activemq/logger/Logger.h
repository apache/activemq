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
#ifndef _ACTIVEMQ_LOGGER_LOGGER_H_
#define _ACTIVEMQ_LOGGER_LOGGER_H_

#include <activemq/logger/LoggerCommon.h>
#include <activemq/logger/LogRecord.h>
#include <activemq/exceptions/IllegalArgumentException.h>

#include <list>
#include <string>
#include <stdarg.h>

namespace activemq{
namespace logger{
    
    class Handler;
    class Filter;

    class Logger
    {
    private:
    
        // The name of this Logger
        std::string name;
        
        // The Parent of this Logger
        Logger* parent;
        
        // list of Handlers owned by this logger
        std::list<Handler*> handlers;
        
        // Filter used by this Logger
        Filter* filter;
        
        // The Log Level of this Logger
        Level level;
        
        // Using Parent Handlers?
        bool useParentHandlers;
    
    public:
    
        /**
         * Creates a new instance of the Logger with the given name
         * and assign it the given parent logger.
         * <p>
         * The logger will be initially configured with a null Level 
         * and with useParentHandlers true.
         * @param name - A name for the logger. This should be a 
         * dot-separated name and should normally be based on the package 
         * name or class name of the subsystem, such as java.net or 
         * javax.swing. It may be null for anonymous Loggers.
         */
        Logger(const std::string& name, Logger* parent);
        
        /**
         * Destructor
         */
        virtual ~Logger(void);
        
        /**
         * Gets the name of this Logger
         * 
         * @return logger name
         */
        virtual const std::string& getName(void) const {
            return name;
        }

        /**
         * Add a log Handler to receive logging messages.
         * <p>
         * By default, Loggers also send their output to their parent logger.
         * Typically the root Logger is configured with a set of Handlers 
         * that essentially act as default handlers for all loggers.
         * 
         * @param A Logging Handler
         * #throws IllegalArgumentException
         */
        virtual void addHandler(Handler* handler) 
            throw ( exceptions::IllegalArgumentException );
        
        /**
         * Removes the specified Handler and destroys it
         * <p>
         * Returns silently if the given Handler is not found.
         * 
         * @param The Handler to remove
         */
        virtual void removeHandler(Handler* handler);

        /**
         * Gets a vector containing all the handlers that this class
         * has been assigned to use.
         */        
        virtual const std::list<Handler*>& getHandlers(void) const;
        
        /**
         * Set a filter to control output on this Logger.
         * <p>
         * After passing the initial "level" check, the Logger will call 
         * this Filter to check if a log record should really be published. 
         * <p>
         * The caller releases ownership of this filter to this logger
         * 
         * @param Filter to use, can be null
         */
        virtual void setFilter(Filter* filter); 
        
        /**
         * Gets the Filter object that this class is using.
         * @return the Filter in use, can be null
         */
        virtual const Filter* getFilter(void) const {
            return filter;
        }
        
        /**
         * Get the log Level that has been specified for this Logger. The 
         * result may be the Null level, which means that this logger's 
         * effective level will be inherited from its parent.
         */
        virtual Level getLevel(void) const {
            return level;
        }
        
        /**
         * Set the log level specifying which message levels will be logged 
         * by this logger. Message levels lower than this value will be 
         * discarded. The level value Level.OFF can be used to turn off 
         * logging.
         * <p>
         * If the new level is the Null Level, it means that this node 
         * should inherit its level from its nearest ancestor with a 
         * specific (non-null) level value. 
         * 
         * @param new Level value
         */
        virtual void setLevel(Level level) {
            this->level = level;
        }
        
        /**
         * Discover whether or not this logger is sending its output to 
         * its parent logger.
         * 
         * @return true if using Parent Handlers
         */
        virtual bool getUseParentHandlers(void) const {
            return useParentHandlers;
        }
        
        /**
         * pecify whether or not this logger should send its output to it's 
         * parent Logger. This means that any LogRecords will also be 
         * written to the parent's Handlers, and potentially to its parent, 
         * recursively up the namespace.
         * 
         * @param True is output is to be writen to the parent
         */
        virtual void setUseParentHandlers(bool value) {
            this->useParentHandlers = value;
        }
        
        /**
         * Logs an Block Enter message
         * <p>
         * This is a convenience method that is used to tag a block enter, a
         * log record with the class name function name and the string 
         * Entering is logged at the DEBUG log level.
         * @param source block name
         * @param source file name
         * @param source line name
         */
        virtual void entry(const std::string& blockName,
                           const std::string& file,
                           const int line);
        
        /**
         * Logs an Block Exit message
         * <p>
         * This is a convenience method that is used to tag a block exit, a
         * log record with the class name function name and the string 
         * Exiting is logged at the DEBUG log level.
         * @param source block name
         * @param source file name
         * @param source line name
         */
        virtual void exit(const std::string& blockName,
                          const std::string& file,
                          const int line);

        /**
         * Log a Debug Level Log
         * <p>
         * If the logger is currently enabled for the DEBUG message level 
         * then the given message is forwarded to all the registered output
         * Handler objects.
         * 
         * @param file name where the log was generated
         * @param line number where the log was generated
         * @param name of the function that logged this
         * @param the message to log
         */
        virtual void debug(const std::string& file,
                           const int line,
                           const std::string fnctionName,
                           const std::string& message);

        /**
         * Log a info Level Log
         * <p>
         * If the logger is currently enabled for the info message level 
         * then the given message is forwarded to all the registered output
         * Handler objects.
         * 
         * @param file name where the log was generated
         * @param line number where the log was generated
         * @param name of the function that logged this
         * @param the message to log
         */
        virtual void info(const std::string& file,
                          const int line,
                          const std::string fnctionName,
                          const std::string& message);

        /**
         * Log a warn Level Log
         * <p>
         * If the logger is currently enabled for the warn message level 
         * then the given message is forwarded to all the registered output
         * Handler objects.
         * 
         * @param file name where the log was generated
         * @param line number where the log was generated
         * @param name of the function that logged this
         * @param the message to log
         */
        virtual void warn(const std::string& file,
                          const int line,
                          const std::string fnctionName,
                          const std::string& message);
                        
        /**
         * Log a error Level Log
         * <p>
         * If the logger is currently enabled for the error message level 
         * then the given message is forwarded to all the registered output
         * Handler objects.
         * 
         * @param file name where the log was generated
         * @param line number where the log was generated
         * @param name of the function that logged this
         * @param the message to log
         */
        virtual void error(const std::string& file,
                           const int line,
                           const std::string fnctionName,
                           const std::string& message);

        /**
         * Log a fatal Level Log
         * <p>
         * If the logger is currently enabled for the fatal message level 
         * then the given message is forwarded to all the registered output
         * Handler objects.
         * 
         * @param file name where the log was generated
         * @param line number where the log was generated
         * @param name of the function that logged this
         * @param the message to log
         */
        virtual void fatal(const std::string& file,
                           const int line,
                           const std::string fnctionName,
                           const std::string& message);
                         
        /**
         * Log a Throw Message
         * <p>
         * If the logger is currently enabled for the Throwing message level 
         * then the given message is forwarded to all the registered output
         * Handler objects.
         * 
         * @param file name where the log was generated
         * @param line number where the log was generated
         * @param name of the function that logged this
         * @param the message to log
         */
        virtual void throwing(const std::string& file,
                              const int line,
                              const std::string fnctionName,
                              const std::string& message);
        
        /**
         * Check if a message of the given level would actually be logged 
         * by this logger. This check is based on the Loggers effective 
         * level, which may be inherited from its parent.
         * 
         * @param level - a message logging level 
         * returns true if the given message level is currently being logged.
         */
        virtual bool isLoggable(Level level) const;
        
        /**
         * Log a LogRecord.
         *
         * All the other logging methods in this class call through this 
         * method to actually perform any logging. Subclasses can override 
         * this single method to capture all log activity. 
         * 
         * @param record - the LogRecord to be published
         */
        virtual void log(LogRecord& record);
         
        /**
         * Log a message, with no arguments.
         * <p>
         * If the logger is currently enabled for the given message level 
         * then the given message is forwarded to all the registered output 
         * Handler objects
         * 
         * @param the Level to log at
         * @param the message to log
         */
        virtual void log(Level level, const std::string& message);

        /**
         * Log a message, with the list of params that is formatted into
         * the message string.
         * <p>
         * If the logger is currently enabled for the given message level 
         * then the given message is forwarded to all the registered output 
         * Handler objects
         * 
         * @param the Level to log at
         * @param the message to log
         * @param variable length arguement to format the message string.
         */
        virtual void log(Level level, 
                         const std::string& file,
                         const int line,
                         const std::string& message, ...);

        /**
         * Log a message, with associated Throwable information.
         *
         * If the logger is currently enabled for the given message level 
         * then the given arguments are stored in a LogRecord which is 
         * forwarded to all registered output handlers.
         *
         * Note that the thrown argument is stored in the LogRecord thrown 
         * property, rather than the LogRecord parameters property. Thus is 
         * it processed specially by output Formatters and is not treated 
         * as a formatting parameter to the LogRecord message property. 
         * 
         * @param the Level to log at
         * @param File that the message was logged in
         * @param line number where the message was logged at.
         * @param Exception to log
         */
        virtual void log(Level level, 
                         const std::string& file,
                         const int line,
                         const std::string& message, 
                         cms::CMSException& ex);

    public:
    
        /**
         * Creates an anonymous logger
         * <p>
         * The newly created Logger is not registered in the LogManager 
         * namespace. There will be no access checks on updates to the 
         * logger.
         * Even although the new logger is anonymous, it is configured to 
         * have the root logger ("") as its parent. This means that by 
         * default it inherits its effective level and handlers from the 
         * root logger.
         * <p>
         * The caller is responsible for destroying the returned logger.
         * 
         * @return Newly created anonymous logger
         */
        static Logger* getAnonymousLogger(void);

        /**
         * Find or create a logger for a named subsystem. If a logger has 
         * already been created with the given name it is returned. 
         * Otherwise a new logger is created.
         * <p>
         * If a new logger is created its log level will be configured based 
         * on the LogManager and it will configured to also send logging 
         * output to its parent loggers Handlers. It will be registered in 
         * the LogManager global namespace. 
         * 
         * @param name - A name for the logger. This should be a 
         * dot-separated name and should normally be based on the package 
         * name or class name of the subsystem, such as cms or 
         * activemq.core.ActiveMQConnection
         * 
         * @return a suitable logger.
         */
        static Logger* getLogger(const std::string& name);

    };

}}

#endif /*_ACTIVEMQ_LOGGER_LOGGER_H_*/
