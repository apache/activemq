/**
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.activemq.broker.jmx;

/**
 * @version $Revision: 1.1 $
 */
public interface CompositeDataConstants {
    String PROPERTIES = "PropertiesText";
    String JMSXGROUP_SEQ = "JMSXGroupSeq";
    String JMSXGROUP_ID = "JMSXGroupID";
    String BODY_LENGTH = "BodyLength";
    String BODY_PREVIEW = "BodyPreview";
    String CONTENT_MAP = "ContentMap";
    String MESSAGE_TEXT = "Text";
    String MESSAGE_URL = "Url";

    String ORIGINAL_DESTINATION = "OriginalDestination";

    // User properties
    String STRING_PROPERTIES = "StringProperties";
    String BOOLEAN_PROPERTIES = "BooleanProperties";
    String BYTE_PROPERTIES = "ByteProperties";
    String SHORT_PROPERTIES = "ShortProperties";
    String INT_PROPERTIES = "IntProperties";
    String LONG_PROPERTIES = "LongProperties";
    String FLOAT_PROPERTIES = "FloatProperties";
    String DOUBLE_PROPERTIES = "DoubleProperties";
}
