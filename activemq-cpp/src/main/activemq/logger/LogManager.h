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
#ifndef _ACTIVEMQ_LOGGER_LOGMANAGER_H_
#define _ACTIVEMQ_LOGGER_LOGMANAGER_H_

#include <map>
#include <list>
#include <string>
#include <vector>

#include <activemq/util/SimpleProperties.h>
#include <activemq/concurrent/Mutex.h>

namespace activemq{
namespace logger{

    class Logger;
    class PropertyChangeListener;

    /**
     * There is a single global LogManager object that is used to maintain 
     * a set of shared state about Loggers and log services.
     *
     * This LogManager object:
     *
     *   * Manages a hierarchical namespace of Logger objects. All named 
     *     Loggers are stored in this namespace.
     *   * Manages a set of logging control properties. These are simple 
     *     key-value pairs that can be used by Handlers and other logging 
     *     objects to configure themselves. 
     *
     * The global LogManager object can be retrieved using 
     * LogManager.getLogManager(). The LogManager object is created during 
     * class initialization and cannot subsequently be changed.
     *
     * ***TODO****
     * By default, the LogManager reads its initial configuration from a 
     * properties file "lib/logging.properties" in the JRE directory. If 
     * you edit that property file you can change the default logging 
     * configuration for all uses of that JRE.
     *
     * In addition, the LogManager uses two optional system properties that 
     * allow more control over reading the initial configuration:
     *
     *    * "decaf.logger.config.class"
     *    * "decaf.logger.config.file" 
     *
     * These two properties may be set via the Preferences API, or as 
     * command line property definitions to the "java" command, or as 
     * system property definitions passed to JNI_CreateJavaVM.
     *
     * If the "java.util.logging.config.class" property is set, then the 
     * property value is treated as a class name. The given class will be 
     * loaded, an object will be instantiated, and that object's constructor 
     * is responsible for reading in the initial configuration. (That object 
     * may use other system properties to control its configuration.) The 
     * alternate configuration class can use readConfiguration(InputStream) 
     * to define properties in the LogManager.
     *
     * If "java.util.logging.config.class" property is not set, then the 
     * "java.util.logging.config.file" system property can be used to specify
     * a properties file (in java.util.Properties format). The initial
     * logging configuration will be read from this file.
     *
     * If neither of these properties is defined then, as described above, 
     * the LogManager will read its initial configuration from a properties 
     * file "lib/logging.properties" in the JRE directory.
     *
     * The properties for loggers and Handlers will have names starting with
     * the dot-separated name for the handler or logger.
     * ***TODO****
     *
     * The global logging properties may include:
     *
     *    * A property "handlers". This defines a whitespace separated 
     *      list of class names for handler classes to load and register as 
     *      handlers on the root Logger (the Logger named ""). Each class
     *      name must be for a Handler class which has a default constructor.
     *      Note that these Handlers may be created lazily, when they are 
     *      first used.
     *    * A property "<logger>.handlers". This defines a whitespace or 
     *      comma separated list of class names for handlers classes to load
     *      and register as handlers to the specified logger. Each class name 
     *      must be for a Handler class which has a default constructor. Note 
     *      that these Handlers may be created lazily, when they are first 
     *      used.
     *    * A property "<logger>.useParentHandlers". This defines a boolean
     *      value. By default every logger calls its parent in addition to 
     *      handling the logging message itself, this often result in 
     *      messages being handled by the root logger as well. When setting
     *      this property to false a Handler needs to be configured for this
     *      logger otherwise no logging messages are delivered.
     *    * A property "config". This property is intended to allow arbitrary
     *      configuration code to be run. The property defines a whitespace 
     *      separated list of class names. A new instance will be created for 
     *      each named class. The default constructor of each class may 
     *      execute arbitrary code to update the logging configuration, such 
     *      as setting logger levels, adding handlers, adding filters, etc. 
     *
     * Loggers are organized into a naming hierarchy based on their dot 
     * separated names. Thus "a.b.c" is a child of "a.b", but "a.b1" and 
     * a.b2" are peers.
     *
     * All properties whose names end with ".level" are assumed to define
     * log levels for Loggers. Thus "foo.level" defines a log level for
     * the logger called "foo" and (recursively) for any of its children 
     * in the naming hierarchy. Log Levels are applied in the order they
     * are defined in the properties file. Thus level settings for child 
     * nodes in the tree should come after settings for their parents. The 
     * property name ".level" can be used to set the level for the root of 
     * the tree.
     *
     * All methods on the LogManager object are multi-thread safe. 
     */
    class LogManager
    {
    private:
    
        // Change listener on this class's Properties
        std::list<PropertyChangeListener*> listeners;
        
        // Properties of the Log Manager
        util::SimpleProperties properties;

    public:

        /**
         * Destructor
         */
        virtual ~LogManager();

        /**
         * Sets the Properties this LogManager should use to configure
         * its loggers.  Once set a properties change event is fired.
         * @param Properties Pointer to read the configuration from
         */
        virtual void setProperties( const util::Properties* properties );

        /**
         * Gets a reference to the Logging Properties used by this
         * logger.
         * @returns The Logger Properties Pointer
         */
        virtual const util::Properties& getProperties( void ) const {
            return properties;
        }

        /**
         * Gets the value of a named property of this LogManager
         * @param Name of the Property to retrieve
         * @return the value of the property
         */
        virtual std::string getProperty( const std::string& name ) {
            return properties.getProperty( name );
        }

        /**
         * Adds a change listener for LogManager Properties, adding the same
         * instance of a change event listener does nothing.
         * @param PropertyChangeListener
         */
        virtual void addPropertyChangeListener( 
            PropertyChangeListener* listener );

        /**
         * Removes a properties change listener from the LogManager.
         */
        virtual void removePropertyChangeListener(
            PropertyChangeListener* listener );

        /**
         * Retrieves or creates a new Logger using the name specified
         * a new logger inherits the configuration of the logger's 
         * parent if there is no configuration data for the logger.
         * @param The name of the Logger.
         */
        virtual Logger* getLogger( const std::string& name );

        /**
         * Gets a list of known Logger Names from this Manager
         * @param STL Vector to hold string logger names
         * @return count of how many loggers were inserted
         */
        virtual int getLoggerNames( const std::vector<std::string>& names );

    public:     // Static Singleton Methods.

        /**
         * Get the singleton instance
         * @return Pointer to an instance of the Log Manager
         */
        static LogManager* getInstance( void );

        /**
         * Returns a Checked out instance of this Manager
         */
        static void returnInstance( void );

        /**
         * Forcefully Delete the Instance of this LogManager
         * even if there are outstanding references.
         */
        static void destroy( void );

    protected:

        /**
         * Constructor, hidden to protect against direct instantiation
         */
        LogManager( void )
        {}

        /**
         * Copy Constructo
         */
        LogManager( const LogManager& manager );

        /**
         * Assignment operator
         */
        void operator=( const LogManager& manager );

    private:

        // Static mutex to protect the singleton methods
        static concurrent::Mutex mutex;

        // Static pointer to the one and only instance.
        static LogManager* instance;

        // Static counter for number of outstanding references
        static unsigned int refCount;

    };

}}

#endif /*_ACTIVEMQ_LOGGER_LOGMANAGER_H_*/
