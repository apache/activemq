<!DOCTYPE html PUBLIC "-//W3C//DTD XHTML 1.0 Transitional//EN" "http://www.w3.org/TR/xhtml1/DTD/xhtml1-transitional.dtd">
<%@ page contentType="text/html;charset=UTF-8" language="java" %>
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

    <title><c:out value="${requestContext.brokerQuery.brokerAdmin.brokerName} : ${pageTitle}" /></title>

    <meta http-equiv="Content-Type" content="text/html; charset=iso-8859-1" />
    <style type="text/css" media="screen">
        @import url('${pageContext.request.contextPath}/styles/sorttable.css');
        @import url('${pageContext.request.contextPath}/styles/type-settings.css');
        @import url('${pageContext.request.contextPath}/styles/site.css');
        @import url('${pageContext.request.contextPath}/styles/prettify.css');
    </style>
    <c:if test="${!disableJavaScript}">
        <script type='text/javascript' src='${pageContext.request.contextPath}/js/common.js'></script>
        <script type='text/javascript' src='${pageContext.request.contextPath}/js/css.js'></script>
        <script type='text/javascript' src='${pageContext.request.contextPath}/js/standardista-table-sorting.js'></script>
        <script type='text/javascript' src='${pageContext.request.contextPath}/js/prettify.js'></script>
        <script>addEvent(window, 'load', prettyPrint)</script>
    </c:if>

