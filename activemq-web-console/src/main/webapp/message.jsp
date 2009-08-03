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
<title>Message ${requestContext.messageQuery.id}</title>
</head>
<body>


<c:choose>
<c:when test="${empty row}">

<div>
No message could be found for ID ${requestContext.messageQuery.id}
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
						<td>${row.JMSMessageID}</td>
					</tr>
					<tr>
						<td class="label">Destination</td>
						<td><form:tooltip text="${row.JMSDestination}" length="50"/></td>
					</tr>
					<tr>
						<td class="label" title="The ID used to correlate messages together in a conversation">Correlation ID</td>
						<td>${row.JMSCorrelationID}</td>
					</tr>
					<tr>
						<td class="label" title="Message Group Identifier">Group</td>
						<td>${row.groupID}</td>
					</tr>
					<tr>
						<td class="label" title="Message Group Sequence Number">Sequence</td>
						<td>${row.groupSequence}</td>
					</tr>
					<tr>
						<td class="label">Expiration</td>
						<td>${row.JMSExpiration}</td>
					</tr>
					<tr>
						<td class="label">Persistence</td>
						<td><jms:persistent message="${row}"/></td>
					</tr>
					<tr>
						<td class="label">Priority</td>
						<td>${row.JMSPriority}</td>
					</tr>
					<tr>
						<td class="label">Redelivered</td>
					    <td>${row.JMSRedelivered}</td>
					</tr>
					<tr>
						<td class="label">Reply To</td>
						<td>${row.JMSReplyTo}</td>
					</tr>
					<tr>
						<td class="label">Timestamp</td>
						<td><jms:formatTimestamp timestamp="${row.JMSTimestamp}"/></td>
					</tr>
					<tr>
						<td class="label">Type</td>
						<td>${row.JMSType}</td>
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
							<td class="label">${prop.key}</td>
							<td>${prop.value}</td>
						</tr>
						<tr>
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
                        <td colspan="2"><a href="deleteMessage.action?JMSDestination=${row.JMSDestination}&messageId=${row.JMSMessageID}">Delete</a></td>
                    </tr>
                    <tr class="odd">
                    <td><a href="javascript:confirmAction('queue', 'copyMessage.action?destination=%target%&JMSDestination=${row.JMSDestination}&messageId=${row.JMSMessageID}&JMSDestinationType=queue')">Copy</a></td>
                        <td rowspan="2">
                            <select id="queue">
                                <option value=""> -- Please select --</option>
                                <c:forEach items="${requestContext.brokerQuery.queues}" var="queues">
                                    <c:if test="${queues.name != requestContext.messageQuery.JMSDestination}">
                                    <option value="${queues.name}"><form:short text="${queues.name}"/></option>
                                    </c:if>
                                </c:forEach>
                            </select>
                        </td>
                        
                    </tr>
                    <tr class="odd">
                        <td><a href="javascript:confirmAction('queue', 'moveMessage.action?destination=%target%&JMSDestination=${row.JMSDestination}&messageId=${row.JMSMessageID}&JMSDestinationType=queue')">Move</a></td>
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





</body>
</html>
	
