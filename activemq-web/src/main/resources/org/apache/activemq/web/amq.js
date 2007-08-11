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

// Technique borrowed from scriptaculous to do includes.

var _AMQ_INCLUDE = {
  Version: 'AMQ JS',
  script: function(libraryName) {
    document.write('<script type="text/javascript" src="'+libraryName+'"></script>');
  },
  load: function() {
    var scriptTags = document.getElementsByTagName("script");
    for(var i=0;i<scriptTags.length;i++) {
      if(scriptTags[i].src && scriptTags[i].src.match(/amq\.js$/)) {
        var path = scriptTags[i].src.replace(/amq\.js$/,'');
        this.script(path + 'prototype.js');
        this.script(path + 'behaviour.js');
        this.script(path + '_amq.js');
        // this.script(path + 'scriptaculous.js');
        break;
      }
    }
  }
}

_AMQ_INCLUDE.load();

