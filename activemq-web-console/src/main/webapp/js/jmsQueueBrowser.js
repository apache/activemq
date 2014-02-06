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

(function (_) {

"use strict";

    _.JMSQueueBrowser = function (destination, page, total, _callback) {
        // Constructing initial AJAX Request to get XML data from ActiveMQ
        this.last = page * 100;
        this.first = this.last - 100;


        this.d = {};
        this.destination = destination;
        this.page = page;
        //this.totalPages = t;
        var that = this;
        this.url = "/admin/queueBrowse/" + this.destination + "?view=rss&feedType=atom_1.0&start=" + this.first + "&end=" + this.last;
        // DEBUG OUTPUT ...
        console.log(" last:  " + this.last);
        console.log(" start: " + this.first);
        console.log("Url: " + this.url);

        return {
            req: $.ajax(this.url).
            done(_callback),
            totalPages: total,
            isFirst: function () {
                return (this.getPageNum() == 1);
            },
            empty: function () {
                $("#messages").empty();
                $("#messages").append("<thead>" + "<tr>" +
                  "<th>Message Id</th>" + "<th>Updated Date</th>" +
                  "<th>Published Date</th>" + "<th>Summary</th>" +
                  "<th>Delete</th>" + "</tr></thead><tbody>" + this.getMQList() + "</tbody>");
            },
            totalItems: function () {
                return this.req.responseXML.firstChild.childElementCount;
            },
            colum: QueryStringHelper.toJSON().page || 1 ,
            getMQList: function () {
                var d = this.req.responseXML;
                var mqitems = [];
                for (var i = 3; i < this.req.responseXML.firstChild.childElementCount; i++) {
                    mqitems.push("<tr>" + "<td>" + "<a href='" + d.firstChild.children[i].children[1].attributes[1].textContent + "'>" + d.firstChild.children[i].children[0].textContent + "</a></td>" + "<td>" + d.firstChild.children[i].children[3].textContent + "</td>" + "<td>" + d.firstChild.children[i].children[4].textContent + "</td>" + "<td>" + d.firstChild.children[i].children[5].textContent + "</td>" + "<td><a href='" + "http://localhost:8161/admin/deleteMessage.action?JMSDestination=TEST&messageId=" + d.firstChild.children[3].children[0].textContent + "'>" + "DELETE</a></td>" + "</tr>");
                }
                return mqitems;
            },
            isLast: function () {
                return (this.getPageNum() == this.getTotalPages());
            },
            isHead: function () {
                return (Number($("#page a")[0].innerHTML) == this.getPageNum());
            },
            isTail: function () {
                return (Number($("#page a")[$("#page a").length - 1].innerHTML) == this.getPageNum());
            },
            isMin: function () {
                return (Number($("#page a")[0].innerHTML) == 1);
            },
            isMax: function () {
                return (Number($("#page a")[$("#page a").length - 1].innerHTML) == this.getTotalPages());
            },
            getPos: function (num) {
                return ((num % 10) === 0) ? 10 : (num % 10);
            },
            setupNav: function () {
                try {
                    var start = 1;
                    var end = this.getTotalPages();
                    $("#page table").empty();
                    for (var i = start; i <= end; i++) {
                        this.getPageLink(i);
                    }
                    this.moveCurLine(0);
                } catch (err) {}

            },
            moveCurLine: function (num) {
                if ($.find("#page a[class='current-page']").length) {
                    $("#page a[class='current-page']")[0].className = "";
                }
                $("#page a")[num].className = "current-page";
            },
            getPageNum: function () {
                return (!this.colum) ? 1 : this.colum;
            },
            BackPaginate: function () {

                try {

                    if (this.isFirst()) {
                        throw "Error: You can not go back when in the first position";
                    }
                    var start = (this.getPageNum() - 10) + 1;
                    var end = this.getPageNum();

                    if (this.isHead() && !this.isMin()) {
                        start--;
                        end--;
                    }

                    $("#page ul").empty();
                    for (var i = start; i <= end; i++) {
                        this.getPageLink(i);
                    }
                    // Set current page:
                    this.moveCurLine($("#page a").length - 1);

                } catch (err) {}

                $("body").animate({
                    scrollTop: '0px'
                }, 800);
            },
            getPageLink: function (num) {
                $("#page table").append("<td><a onclick='fetchResults(" + num + ")' >" + num + "</a></td>");
            },
            Paginate: function () {
                try {
                    if (this.isMax()) {
                        throw "This is the last item in the list sorry…";
                    }
                    $("#page table").empty();
                    var start = this.getPageNum();
                    var end = this.getPageNum() + 10;
                    start++;
                    if (end >= this.getTotalPages()) {
                        end = this.getTotalPages();
                    }

                    for (var i = start; i < end + 1; i++) {
                        this.getPageLink(i);
                    }

                    this.moveCurLine(0);

                } catch (err) {}

                //This used to scroll up the page when a user click the button...
                $("body").animate({
                    scrollTop: '0px'
                }, 800);

            },
            getTotalPages: function () {
                return (! this.totalPages ) ? 0 : this.totalPages;
            },
            pagin_prev: function () {
                try {

                    if (this.isFirst()) throw "Can not go previous at the first location";

                    if (this.isHead()) {
                        this.BackPaginate();
                    }
                    colum--;
                    fetchResults(colum);
                } catch (err) {}
            },
            pagin_next: function () {
                try {
                    if (this.isLast()) // return ( this.getPageNum() == this.getTotalPages() ); TODO: Check for more records...
                    throw "End of pages.. can not select next";

                    if (this.isTail()) {
                        this.Paginate();
                    }
                    colum++;
                    fetchResults(colum);
                    //Catching exception
                } catch (err) {
                    return;
                }
            }
        };
    };

    _.next = function () {
        PaginQueue.pagin_next();
    };

    _.prev = function () {
        PaginQueue.pagin_prev();
    };

    _.showError = function (msg) {
        $("#messages").empty();
        $("#messages").append(msg);
    };

    _.fetchResults = function (e) {
        var last = e * 100;
        var start = last - 100;
        console.log('start :' + start);
        console.log('last :' + last);
        if (QueryStringHelper.toJSON().page && QueryStringHelper.toJSON().JMSDestination) {
            var z = new JMSQueueBrowser(QueryStringHelper.toJSON().JMSDestination, Number(QueryStringHelper.toJSON().page), 10, function (data) {
                return data;
            });
            z.empty();
            z.setupNav();
            return z;
        } else {
            showError("<h1>ERROR: You need to set the JMSDestination and Page to view the JMS Messages in Queue</h1>");
        }


    };


    _.QueryStringHelper = {

        getCurrentUrl: function () {
            return window.location.href;
        },
        isQueryStr: function () {
            return this.getCurrentUrl().indexOf("?") != -1;
        },
        getQueryString: function () {
            return this.getCurrentUrl().split("?")[1];
        },
        toJSON: function () {
            var json = {};
            try {
                var keys = this.getQueryString().split("&");
                for (var i = 0, len = keys.length; i < len; i++) {
                    var pair = keys[i].split("=");
                    var pairval = decodeURIComponent(pair[1]);
                    pairval = pairval.replace("+", " ");
                    json[pair[0]] = pairval;
                }
            } catch (err) {}
            return json;
        }
    };

    if (QueryStringHelper.toJSON().page) {
        Number(QueryStringHelper.toJSON().page);
    };

//  JSON.parse(str).value.TotalMessageCount

_.JMXJSON=function(brokertype, brokername,callback){
    this.bType = brokertype || 'Broker';
    this.bName = brokername || 'localhost';
    this.calli = callback || function(data){
                                    console.log("Total Count of Messages: " + data.value.TotalMessageCount);
                                    data.value.TotalMessageCount;
                                }
    this.url="/api/jolokia/read/org.apache.activemq:type="+this.bType+",brokerName="+this.bName;
    return {
            req: $.getJSON(this.url).done(callback) ,
            getBrokerType: this.bType ,
            getBrokerName: this.bName,
            getMessageCount: 0
    }
}


//Script to run on browser load:
$(".progress").hide();


var jmx = new JMXJSON(null,null,function(data){
        if (!!QueryStringHelper.toJSON().page && !!QueryStringHelper.toJSON().JMSDestination){
            $(".progress").show();
            $(".progress-bar").css({"width":"50%"})
            var z = new JMSQueueBrowser(QueryStringHelper.toJSON().JMSDestination, Number(QueryStringHelper.toJSON().page), data.value.TotalMessageCount, function (data) {
                z.empty();
                z.setupNav();
                $(".progress").hide();
                return data;
            });
            return z;
        } else {
            showError("<h1>ERROR: You need to set the JMSDestination and Page to view the JMS Messages in Queue</h1>");
        }
    console.log(data.value.TotalMessageCount);
});


})(this);
/**

var jmx = new JMXJSON(null,null,function(data){
        if (!!QueryStringHelper.toJSON().page && !!QueryStringHelper.toJSON().JMSDestination){
            var z = new JMSQueueBrowser(QueryStringHelper.toJSON().JMSDestination, Number(QueryStringHelper.toJSON().page), data.value.TotalMessageCount, function (data) {
                return data;
            });
            z.empty();
            z.setupNav();
            return z;
        } else {
            showError("<h1>ERROR: You need to set the JMSDestination and Page to view the JMS Messages in Queue</h1>");
        }


console.log(data.value.TotalMessageCount); });

**/