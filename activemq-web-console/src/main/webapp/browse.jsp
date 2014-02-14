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
<c:set var="pageTitle" value="Browse ${requestContext.queueBrowser.JMSDestination}"/>

<%@include file="decorators/head.jsp" %>
<title>Browse <form:short text="${requestContext.queueBrowser.JMSDestination}"/></title>

<script type="text/javascript" src="js/jmsQueueBrowser.js"></script>

<style type="text/css">

// This will prevent a bug that exist in firefox for pagination
.pagination {
	margin: 0px !important;
}

</style>
<!-- BOOTSTRAP CSS LIBRARY -->
<!-- TODO: REMOVE @import url('/admin/styles/sorttable.css'); -->
<!-- GOOGLE FONTS -->
 

<%@include file="decorators/header.jsp" %>


</head>
<body>

<input type="hidden" name="secret" id="secret" value='${sessionScope["secret"]}' />

<!-- Start Of Panel -->
<div class="panel" style="width:800px;">

<blockquote class="bs-callout"><h3>JMS Messages in Queue: <form:tooltip text="${requestContext.queueBrowser.JMSDestination}"/>
<small> 1 - 100 of 10,000 messages</small></h3></blockquote>
<div id="pageIndicator"></div>


</div>
<!-- End Of Panel -->

<table id="messages" class="table table-striped" styles="width:20%;">
    <div class="progress" style="width: 400px;">
    <div class="progress-bar progress-bar-danger" role="progressbar" aria-valuenow="60" aria-valuemin="0" aria-valuemax="100" style="width:80%">
        <span class="sr-only">80% Complete</span>
    </div>
</div>

</table>
<!-- PAGINATION -->

<div class="pagination" style="width:100%;">
                    <div class="pages" id="page" style="padding-bottom: 90px;">
                        <ul class="pagination" >
                            <li>
                            	<a href="#" onclick="prev()" id="prev" class="prev off" data-original-title="">Prev</a>
                            </li>
                            <li>
                                <a class="active" onclick="fetchResults(1)" href="#1" data-original-title="">1</a>
                            </li>
                            <li>
                                <a onclick="fetchResults(2)" href="#2" data-original-title="">2</a>
                            </li>
                            <li>
                                <a onclick="fetchResults(3)" href="#3" data-original-title="">3</a>
                            </li>
                            <li>
                                <a onclick="fetchResults(4)" href="#4" data-original-title="">4</a>
                            </li>
                            <li>
                                <a onclick="fetchResults(5)" href="#5" data-original-title="">5</a>
                            </li>
                            <li>
                                <a onclick="fetchResults(6)" href="#6" data-original-title="">6</a>
                            </li>
                            <li>
                                <a onclick="fetchResults(7)" href="#7" data-original-title="">7</a>
                            </li>
                            <li>
                                <a onclick="fetchResults(8)" href="#8" data-original-title="">8</a>
                            </li>
                            <li>
                                <a onclick="fetchResults(9)" href="#9" data-original-title="">9</a>
                            </li>
                            <li>
                                <a onclick="fetchResults(10)" href="#10" data-original-title="">10</a>
                            </li>
                            <li>
                                <a href="#" onclick="next()" id="next" class="next" data-original-title="">Next</a>
                            </li>
                        </ul>

                    </div>


                </div>
<!-- PAGINATION -->
<div>
<a href="queueConsumers.jsp?JMSDestination=<c:out value="${requestContext.queueBrowser.JMSDestination}"/>">View Consumers</a>
</div>


<script type="text/javascript">
    (function(_){

	_.sec='${sessionScope["secret"]}';

    	
        if( !!! QueryStringHelper.toJSON().page){
             location.replace(location.origin + location.pathname +"?JMSDestination="+QueryStringHelper.toJSON().JMSDestination+"&page=" + 1 );
        }

       $(".progress").hide();
           var jmx = new JMXJSON(null,null,function(data){
           try{
                   if (!!QueryStringHelper.toJSON().page && !!QueryStringHelper.toJSON().JMSDestination){
                       $(".progress").show();
                       $(".progress-bar").css({"width":"50%"});

                       var e = Number(QueryStringHelper.toJSON().page);
                       var last = e * 100;
                       var start = last - 100;
                      
                       var total = data.value.TotalMessageCount;
                       _.totalPages = data.value.TotalMessageCount;

   						// Checking if we have < 100 messags
   						 if( total == 0 ){
                            throw "Error: No messages in the queue";
                     	} else if(total < 100){
							start =0;
							last = total;
   	   					} else if ( start >= total &&  last > total ){
                            throw "Error: No messages on this page ";
                     	} else if ( start < total && last > total ) {
                            start = ( last - total ) + start ;
                     	}
	   					

                       
                       if ( PaginQueue.isHead() ) {
                              PaginQueue.moveCurLine(1);
                              PaginQueue.setupNav();
                       } else {
                              PaginQueue.moveCurLine( PaginQueue.getPos( PaginQueue.getPageNum()) );
                              PaginQueue.setupNav();
                       }

						
                      if(total===1){
                    	  $(".bs-callout small").html("1 of 1 messages of 1");
                      } else {
                    	  $(".bs-callout small").html(start+" of "+last +" messages of "+ total);
                      }
                     
                       var z = new JMSQueueBrowser(QueryStringHelper.toJSON().JMSDestination, Number(QueryStringHelper.toJSON().page), data.value.TotalMessageCount, function (data) {
                           z.empty();
                           $(".progress").hide();
                           return data;
                       });
                       return z;
                   }else {
                        throw "End of pages.. can not select next";
                   }
                } catch(err){
                   $(".progress").hide();
                   $("#page").hide();

                    $(".bs-callout small").html("");
                    showError("<h1>"+err+"</h1>");
                }
               console.log(data.value.TotalMessageCount);
           });

        // ... clean up code
        if( PaginQueue.isLast() ) {
              $("#next").parent().addClass("disabled");
        } else if( PaginQueue.isFirst() ){
              $("#prev").parent().addClass("disabled");
        }



    })(this);
    </script>
<%@include file="decorators/footer.jsp" %>

</body>
</html>

