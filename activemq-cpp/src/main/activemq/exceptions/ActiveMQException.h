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
#ifndef ACTIVEMQ_EXCEPTIONS_ACTIVEMQEXCEPTION_H
#define ACTIVEMQ_EXCEPTIONS_ACTIVEMQEXCEPTION_H

#include <cms/CMSException.h>
#include <activemq/exceptions/ExceptionDefines.h>
#include <stdarg.h>
#include <sstream>

namespace activemq{
namespace exceptions{

   /*
    * Base class for all exceptions.
    */
   class ActiveMQException : public cms::CMSException
   {
   private:
    
      /**
       * The cause of this exception.
       */
      std::string message;
        
      /**
       * The stack trace.
       */
      std::vector< std::pair< std::string, int> > stackTrace;
   
   public:
    
      /**
       * Default Constructor
       */
      ActiveMQException(void) {}
       
      /**
       * Copy Constructor
       */
      ActiveMQException( const ActiveMQException& ex ){
           *this = ex;
      }
       
      /**
       * Constructor - Initializes the file name and line number where
       * this message occured.  Sets the message to report, using an 
       * optional list of arguments to parse into the message
       * @param file name where exception occurs
       * @param line number where the exception occurred.
       * @param message to report
       * @param list of primitives that are formatted into the message
       */
      ActiveMQException(const char* file, const int lineNumber, 
           const char* msg, ...)
      {
         va_list vargs ;
         va_start(vargs, msg) ;
         buildMessage(msg, vargs) ;
            
         // Set the first mark for this exception.
         setMark( file, lineNumber );
      }

      /**
       * Destructor
       */
      virtual ~ActiveMQException(){}
   
      /**
       * Gets the message for this exception.
       */
      virtual const char* getMessage() const{ return message.c_str(); }
   
      /**
       * Sets the cause for this exception.
       * @param msg the format string for the msg.
       */
      virtual void setMessage( const char* msg, ... ){
          va_list vargs ;
          va_start(vargs, msg) ;
          buildMessage(msg, vargs) ;
      }
        
      /**
       * Adds a file/line number to the stack trace.
       * @param file The name of the file calling this method (use __FILE__).
       * @param lineNumber The line number in the calling file (use __LINE__).
       */
      virtual void setMark( const char* file, const int lineNumber );
        
      /**
       * Clones this exception.  This is useful for cases where you need
       * to preserve the type of the original exception as well as the message.
       * All subclasses should override.
       */
      virtual ActiveMQException* clone() const{
          return new ActiveMQException( *this );
      }
        
      /**
       * Provides the stack trace for every point where
       * this exception was caught, marked, and rethrown.  The first
       * item in the returned vector is the first point where the mark
       * was set (e.g. where the exception was created).  
       * @return the stack trace.
       */
      virtual std::vector< std::pair< std::string, int> > getStackTrace() const{ 
          return stackTrace; 
      }
        
      /**
       * Prints the stack trace to std::err
       */
      virtual void printStackTrace() const{
          printStackTrace( std::cerr );
      }
        
      /**
       * Prints the stack trace to the given output stream.
       * @param stream the target output stream.
       */
      virtual void printStackTrace( std::ostream& stream ) const{
          stream << getStackTraceString();
      }
        
      /**
       * Gets the stack trace as one contiguous string.
       */
      virtual std::string getStackTraceString() const{
            
         // Create the output stream.
         std::ostringstream stream;
            
         // Write the message and each stack entry.
         stream << message << std::endl;
         for( unsigned int ix=0; ix<stackTrace.size(); ++ix ){
             stream << "\tFILE: " << stackTrace[ix].first;
             stream << ", LINE: " << stackTrace[ix].second;
             stream << std::endl;                    
         }
            
         // Return the string from the output stream.
         return stream.str();
      }
        
      /**
       * Assignment operator.
       */
      virtual ActiveMQException& operator =( const ActiveMQException& ex ){
          this->message = ex.message;
          this->stackTrace = ex.stackTrace;
          return *this;
      }
        
   protected:
   
      virtual void buildMessage(const char* format, va_list& vargs);

   };

}}

#endif /*ACTIVEMQ_EXCEPTIONS_ACTIVEMQEXCEPTION_H*/
