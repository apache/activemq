/**
 *
 * Copyright 2005-2006 The Apache Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.activemq.store.jdbc.adapter;

import org.apache.activemq.store.jdbc.Statements;

/**
 * Provides JDBCAdapter since that uses
 * IMAGE datatype to hold binary data.
 * 
 * The databases/JDBC drivers that use this adapter are:
 * <ul>
 * <li>Sybase</li>
 * <li>MS SQL</li>
 * </ul>
 * 
 * @org.apache.xbean.XBean element="imageBasedJDBCAdaptor"
 */
public class ImageBasedJDBCAdaptor extends DefaultJDBCAdapter {

    public void setStatements(Statements statements) {
        statements.setBinaryDataType("IMAGE");
        super.setStatements(statements);
    }
    
}