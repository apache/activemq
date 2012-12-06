<%--
    Licensed to the Apache Software Foundation (ASF) under one or more
    contributor license agreements.  See the NOTICE file distributed with
    this work for additional information regarding copyright ownership.
    The ASF licenses this file to You under the Apache License, Version 2.0
    (the "License"); you may not use this file except in compliance with
    the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
--%>
<%@ attribute name="text" type="java.lang.String" required="true"  %>
<%@ attribute name="length" type="java.lang.Integer" required="false" %>
<%
 text = org.apache.commons.lang.StringEscapeUtils.escapeHtml(text);
 text = org.apache.commons.lang.StringEscapeUtils.escapeJavaScript(text);
 if (length == null || length < 20)
    length = 20;
 if (text.length() <= length) {
     out.print(text);
 } else {
     out.println(text.substring(0, (length-10)) + "..." + text.substring(text.length() - 5));
 }
%>