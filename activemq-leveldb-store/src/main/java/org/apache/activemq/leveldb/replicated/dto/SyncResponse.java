/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.activemq.leveldb.replicated.dto;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlRootElement;
import java.util.HashSet;
import java.util.Set;

/**
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
@XmlRootElement(name="sync_response")
@XmlAccessorType(XmlAccessType.FIELD)
public class SyncResponse {

    @XmlAttribute(name = "snapshot_position")
    public long snapshot_position;

    @XmlAttribute(name = "wal_append_position")
    public long wal_append_position;

    @XmlAttribute(name = "index_files")
    public Set<FileInfo> index_files = new HashSet<FileInfo>();

    @XmlAttribute(name = "log_files")
    public Set<FileInfo> log_files = new HashSet<FileInfo>();

    @XmlAttribute(name = "append_log")
    public String append_log;
}
