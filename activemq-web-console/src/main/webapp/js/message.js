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

function sortSelect(selElem) {
    const tmpAry = [];
    for (var i=0;i<selElem.options.length;i++) {
        tmpAry[i] = [];
        tmpAry[i][0] = selElem.options[i].text;
        tmpAry[i][1] = selElem.options[i].value;
    }
    tmpAry.sort();
    while (selElem.options.length > 0) {
        selElem.options[0] = null;
    }
    for (var i=0;i<tmpAry.length;i++) {
        selElem.options[i] = new Option(tmpAry[i][0], tmpAry[i][1]);
    }
}

function selectOptionByText (selElem, selText) {
    var iter = 0;
    while ( iter < selElem.options.length ) {
        if ( selElem.options[iter].text === selText ) {
            selElem.selectedIndex = iter;
            break;
        }
        iter++;
    }
}

function confirmAction(id, action, params) {
    //TODO i18n messages
    const select = document.getElementById(id);
    const selectedIndex = select.selectedIndex;
    if (select.selectedIndex === 0) {
        alert("Please select a value");
        return;
    }
    const value = select.options[selectedIndex].value;
    const url = action + params + "&destination=" + encodeURIComponent(value);
    console.log(url);

    if (confirm("Are you sure?"))
        location.href=url;
}

window.onload=function() {
    sortSelect( document.getElementById('queue') );
    selectOptionByText( document.getElementById('queue'), "-- Please select --" );

    const params = document.getElementById('actionParams').value;

    document.getElementById('actionCopy').onclick = function(e) {
        confirmAction('queue', 'copyMessage', params);
    }

    document.getElementById('actionMove').onclick = function(e) {
        confirmAction('queue', 'moveMessage', params);
    }

    document.getElementById('actionDelete').onclick = function(e) {
        return confirm('Are you sure you want to delete the message?');
    }
}