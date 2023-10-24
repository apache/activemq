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
<html>
<head>
<c:set var="row" value="${requestContext.messageQuery.message}"/>
<c:set var="pageTitle" value="Message ${requestContext.messageQuery.id}"/>

<%@include file="decorators/head.jsp" %>
</head>
<body>

<%@include file="decorators/header.jsp" %>

<c:choose>
<c:when test="${empty row}">

<div>
No message could be found for ID <c:out value="${requestContext.messageQuery.id}"/>
</div>

</c:when>

<c:otherwise>

<table class="layout">
    <tr>
        <td class="layout"  valign="top">
            <table id="header" class="sortable autostripe">
                <thead>
                    <tr>
                        <th colspan="2">
                            Headers
                        </th>
                    </tr>
                </thead>
                <tbody>
                    <tr>
                        <td class="label" title="Unique Message ID for this message">Message ID</td>
                        <td><c:out value="${row.JMSMessageID}"/></td>
                    </tr>
                    <tr>
                        <td class="label">Destination</td>
                        <td><form:tooltip text="${row.JMSDestination}" length="50"/></td>
                    </tr>
                    <tr>
                        <td class="label" title="The ID used to correlate messages together in a conversation">Correlation ID</td>
                        <td><c:out value="${row.JMSCorrelationID}"/></td>
                    </tr>
                    <tr>
                        <td class="label" title="Message Group Identifier">Group</td>
                        <td><c:out value="${row.groupID}"/></td>
                    </tr>
                    <tr>
                        <td class="label" title="Message Group Sequence Number">Sequence</td>
                        <td><c:out value="${row.groupSequence}"/></td>
                    </tr>
                    <tr>
                        <td class="label">Expiration</td>
                        <td><c:out value="${row.JMSExpiration}"/></td>
                    </tr>
                    <tr>
                        <td class="label">Persistence</td>
                        <td><jms:persistent message="${row}"/></td>
                    </tr>
                    <tr>
                        <td class="label">Priority</td>
                        <td><c:out value="${row.JMSPriority}"/></td>
                    </tr>
                    <tr>
                        <td class="label">Redelivered</td>
                        <td><c:out value="${row.JMSRedelivered}"/></td>
                    </tr>
                    <tr>
                        <td class="label">Reply To</td>
                        <td><c:out value="${row.JMSReplyTo}"/></td>
                    </tr>
                    <tr>
                        <td class="label">Timestamp</td>
                        <td><jms:formatTimestamp timestamp="${row.JMSTimestamp}"/></td>
                    </tr>
                    <tr>
                        <td class="label">Type</td>
                        <td><c:out value="${row.JMSType}"/></td>
                    </tr>
                </tbody>
            </table>
        </td>

        <td  class="layout" valign="top">
            <table id="properties" class="sortable autostripe">
                <thead>
                    <tr>
                        <th colspan="2">
                            Properties
                        </th>
                    </tr>
                </thead>
                <tbody>
                   <form:forEachMapEntry items="${requestContext.messageQuery.propertiesMap}" var="prop">
                        <tr>
                            <td class="label"><c:out value="${prop.key}"/></td>
                            <td><c:out value="${prop.value}"/></td>
                        </tr>
                    </form:forEachMapEntry>
                </tbody>
            </table>
        </td>
    </tr>
    <tr>
        <td class="layout" colspan="2">
            <table id="body" width="100%">
                <thead>
                    <tr>
                        <th colspan="2">
                            Message Actions
                        </th>
                    </tr>
                </thead>
                <tbody>
                    <tr>
                        <td colspan="2"><a href="<c:out value='deleteMessage.action?JMSDestination=${requestContext.messageQuery.JMSDestination}&messageId=${row.JMSMessageID}&secret=${sessionScope["secret"]}' />" onclick="return confirm('Are you sure you want to delete the message?')" >Delete</a></td>
                    </tr>
                    <c:if test="${requestContext.messageQuery.isDLQ() || requestContext.messageQuery.JMSDestination eq 'ActiveMQ.DLQ'}">
                    	<tr>
                    		<td>
                    	 		<a href="<c:url value="retryMessage.action">
                                          <c:param name="JMSDestination" value="${requestContext.messageQuery.JMSDestination}" />
                                          <c:param name="messageId" value="${row.JMSMessageID}" />
                                          <c:param name="JMSDestinationType" value="queue" />
                                          <c:param name="secret" value='${sessionScope["secret"]}' />
                                      </c:url>"
                                      onclick="return confirm('Are you sure you want to retry this message?')"
                                     title="Retry - attempt reprocessing on original destination">Retry</a>
                            </td>
                       </tr>
                    </c:if>
                    <tr class="odd">
                    <td><a href="<c:out value="javascript:confirmAction('queue', 'copyMessage"/>')">Copy</a></td>
                        <td rowspan="2">
                            <select id="queue">
                                <option value=""> -- Please select --</option>
                                <c:forEach items="${requestContext.brokerQuery.queues}" var="queues">
                                    <c:if test="${queues.name != requestContext.messageQuery.JMSDestination}">
                                    <option value="<c:out value="${queues.name}" />"><form:short text="${queues.name}" length="80"/></option>
                                    </c:if>
                                </c:forEach>
                            </select>
                        </td>

                    </tr>
                    <tr class="odd">
                        <td><a href="<c:out value="javascript:confirmAction('queue', 'moveMessage"/>')"
                            >Move</a></td>
                    </tr>
                </tbody>
            </table>
        </td>
    </tr>
    <tr>
        <td class="layout" colspan="2">
            <table id="body" width="100%">
                <thead>
                    <tr>
                        <th>
                            Message Details
                        </th>
                    </tr>
                </thead>
                <tbody>
                    <tr>
                        <td><div class="message"><pre class="prettyprint"><c:out value="${requestContext.messageQuery.body}"/></pre></div></td>
                    </tr>
                </tbody>
            </table>
        </td>
    </tr>
</table>

</c:otherwise>
</c:choose>

<script type="text/javascript">
function sortSelect(selElem) {
        var tmpAry = new Array();
        for (var i=0;i<selElem.options.length;i++) {
            tmpAry[i] = new Array();
            tmpAry[i][0] = selElem.options[i].text;
            tmpAry[i][1] = selElem.options[i].value;
        }
        tmpAry.sort();
        while (selElem.options.length > 0) {
            selElem.options[0] = null;
        }
        for (var i=0;i<tmpAry.length;i++) {
            var op = new Option(tmpAry[i][0], tmpAry[i][1]);
            selElem.options[i] = op;
        }
        return;
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

function confirmAction(id, action) {
	//TODO i18n messages
	var select = document.getElementById(id);
	var selectedIndex = select.selectedIndex; 
	if (select.selectedIndex == 0) {
		alert("Please select a value");
		return;
	}
	var value = select.options[selectedIndex].value;
	var url = action + ".action?destination=" + encodeURIComponent(value);

	var url = action +
		"<c:url value=".action">
                     <c:param name="JMSDestination" value="${requestContext.messageQuery.JMSDestination}" />
                     <c:param name="messageId" value="${row.JMSMessageID}" />
                     <c:param name="JMSDestinationType" value="queue" />
                     <c:param name="secret" value='${sessionScope["secret"]}' />
                 </c:url>";
	url = url + "&destination=" + encodeURIComponent(value);

	if (confirm("Are you sure?"))
	  location.href=url;
}

window.onload=function() {
	sortSelect( document.getElementById('queue') );
	selectOptionByText( document.getElementById('queue'), "-- Please select --" );
}
</script>


<%@include file="decorators/footer.jsp" %>


</body>
</html>

