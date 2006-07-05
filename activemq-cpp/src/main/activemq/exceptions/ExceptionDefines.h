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
#ifndef _ACTIVEMQ_EXCEPTIONS_EXCEPTIONDEFINES_H_
#define _ACTIVEMQ_EXCEPTIONS_EXCEPTIONDEFINES_H_

/**
 * Macro for catching and rethrowing an exception of
 * a given type.
 * @param type The type of the exception to throw 
 * (e.g. ActiveMQException ).
 */
#define AMQ_CATCH_RETHROW( type ) \
    catch( type& ex ){ \
        ex.setMark( __FILE__, __LINE__ ); \
        throw ex; \
    }
    
/**
 * Macro for catching an exception of one type and then rethrowing
 * as another type.
 * @param sourceType the type of the exception to be caught.
 * @param targetType the type of the exception to be thrown.
 */
#define AMQ_CATCH_EXCEPTION_CONVERT( sourceType, targetType ) \
    catch( sourceType& ex ){ \
        targetType target( ex ); \
        target.setMark( __FILE__, __LINE__ ); \
        throw target; \
    }

/**
 * A catch-all that throws a known exception.
 * @param type the type of exception to be thrown.
 */
#define AMQ_CATCHALL_THROW( type ) \
    catch( ... ){ \
        type ex( __FILE__, __LINE__, \
            "caught unknown exception" ); \
        throw ex; \
    }

/**
 * A catch-all that does not throw an exception, one use would
 * be to catch any exception in a destructor and mark it, but not
 * throw so that cleanup would continue as normal.
 */
#define AMQ_CATCHALL_NOTHROW( ) \
    catch( ... ){ \
        exceptions::ActiveMQException ex( __FILE__, __LINE__, \
            "caught unknown exception, not rethrowing" ); \
    }

/**
 * Macro for catching and rethrowing an exception of
 * a given type.
 * @param type The type of the exception to throw 
 * (e.g. ActiveMQException ).
 */
#define AMQ_CATCH_NOTHROW( type ) \
    catch( type& ex ){ \
        ex.setMark( __FILE__, __LINE__ ); \
    }

#endif /*_ACTIVEMQ_EXCEPTIONS_EXCEPTIONDEFINES_H_*/
