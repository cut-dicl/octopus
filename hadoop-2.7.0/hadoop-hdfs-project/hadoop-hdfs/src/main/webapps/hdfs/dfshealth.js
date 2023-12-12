/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * 
 * EDITED BY: Nicolas Karmiris
 * For the purpose of extending the existing WebInterface of HadoopFS to a new
 * inteface that implements the functionalities of the new OctapusFS.
 * Cyprus University of Technology 
 */
(function () {
  "use strict";

  dust.loadSource(dust.compile($('#tmpl-dfshealth').html(), 'dfshealth'));
  dust.loadSource(dust.compile($('#tmpl-startup-progress').html(), 'startup-progress'));
  dust.loadSource(dust.compile($('#tmpl-datanode').html(), 'datanode-info'));
  dust.loadSource(dust.compile($('#tmpl-datanode-volume-failures').html(), 'datanode-volume-failures'));
  dust.loadSource(dust.compile($('#tmpl-snapshot').html(), 'snapshot-info'));
  dust.loadSource(dust.compile($('#tmpl-storage-tiers').html(),'storage-tiers'));
  dust.loadSource(dust.compile($('#tmpl-caching').html(),'caching'));

  function load_overview() {
    var BEANS = [
      {"name": "nn",      "url": "/jmx?qry=Hadoop:service=NameNode,name=NameNodeInfo"},
      {"name": "nnstat",  "url": "/jmx?qry=Hadoop:service=NameNode,name=NameNodeStatus"},
      {"name": "fs",      "url": "/jmx?qry=Hadoop:service=NameNode,name=FSNamesystemState"},
      {"name": "mem",     "url": "/jmx?qry=java.lang:type=Memory"}
    ];

    var HELPERS = {
      'helper_fs_max_objects': function (chunk, ctx, bodies, params) {
        var o = ctx.current();
        if (o.MaxObjects > 0) {
          chunk.write('(' + Math.round((o.FilesTotal + o.BlockTotal) / o.MaxObjects * 100) * 100 + ')%');
        }
      },

      'helper_dir_status': function (chunk, ctx, bodies, params) {
        var j = ctx.current();
        for (var i in j) {
          chunk.write('<tr><td>' + i + '</td><td>' + j[i] + '</td><td>' + params.type + '</td></tr>');
        }
      },

      'helper_date_tostring' : function (chunk, ctx, bodies, params) {
        var value = dust.helpers.tap(params.value, chunk, ctx);
        return chunk.write('' + new Date(Number(value)).toLocaleString());
      }
    };

    var data = {};

    $.ajax({'url': '/conf', 'dataType': 'xml', 'async': false}).done(
      function(d) {
        var $xml = $(d);
        var namespace, nnId;
        $xml.find('property').each(function(idx,v) {
          if ($(v).find('name').text() === 'dfs.nameservice.id') {
            namespace = $(v).find('value').text();
          }
          if ($(v).find('name').text() === 'dfs.ha.namenode.id') {
            nnId = $(v).find('value').text();
          }
        });
        if (namespace && nnId) {
          data['HAInfo'] = {"Namespace": namespace, "NamenodeID": nnId};
        }
    });

    // Workarounds for the fact that JMXJsonServlet returns non-standard JSON strings
    function workaround(nn) {
      nn.JournalTransactionInfo = JSON.parse(nn.JournalTransactionInfo);
      nn.NameJournalStatus = JSON.parse(nn.NameJournalStatus);
      nn.NameDirStatuses = JSON.parse(nn.NameDirStatuses);
      nn.NodeUsage = JSON.parse(nn.NodeUsage);
      nn.CorruptFiles = JSON.parse(nn.CorruptFiles);
      return nn;
    }

    load_json(
      BEANS,
      guard_with_startup_progress(function(d) {
        for (var k in d) {
          data[k] = k === 'nn' ? workaround(d[k].beans[0]) : d[k].beans[0];
        }
        render();
      }),
      function (url, jqxhr, text, err) {
        show_err_msg('<p>Failed to retrieve data from ' + url + ', cause: ' + err + '</p>');
      });

    function render() {
      var base = dust.makeBase(HELPERS);
      dust.render('dfshealth', base.push(data), function(err, out) {
        $('#tab-overview').html(out);
        $('#ui-tabs a[href="#tab-overview"]').tab('show');
      });
    }
  }

  function show_err_msg(msg) {
    $('#alert-panel-body').html(msg);
    $('#alert-panel').show();
  }

  function ajax_error_handler(url, jqxhr, text, err) {
    show_err_msg('<p>Failed to retrieve data from ' + url + ', cause: ' + err + '</p>');
  }

  function guard_with_startup_progress(fn) {
    return function() {
      try {
        fn.apply(this, arguments);
      } catch (err) {
        if (err instanceof TypeError) {
          show_err_msg('NameNode is still loading. Redirecting to the Startup Progress page.');
          load_startup_progress();
        }
      }
    };
  }

  function load_startup_progress() {
    function workaround(r) {
      function rename_property(o, s, d) {
        if (o[s] !== undefined) {
          o[d] = o[s];
          delete o[s];
        }
      }
      r.percentComplete *= 100;
      $.each(r.phases, function (idx, p) {
        p.percentComplete *= 100;
        $.each(p.steps, function (idx2, s) {
          s.percentComplete *= 100;
          // dust.js is confused by these optional keys in nested
          // structure, rename them
          rename_property(s, "desc", "stepDesc");
          rename_property(s, "file", "stepFile");
          rename_property(s, "size", "stepSize");
        });
      });
      return r;
    }
    $.get('/startupProgress', function (resp) {
      var data = workaround(resp);
      dust.render('startup-progress', data, function(err, out) {
        $('#tab-startup-progress').html(out);
        $('#ui-tabs a[href="#tab-startup-progress"]').tab('show');
      });
    }).error(ajax_error_handler);
  }

  function load_datanode_info() {

    var HELPERS = {
      'helper_lastcontact_tostring' : function (chunk, ctx, bodies, params) {
        var value = dust.helpers.tap(params.value, chunk, ctx);
        return chunk.write('' + new Date(Date.now()-1000*Number(value)));
      }
    };

    function workaround(r) {
      function node_map_to_array(nodes) {
        var res = [];
        for (var n in nodes) {
          var p = nodes[n];
          p.name = n;
          res.push(p);
        }
        return res;
      }

      r.LiveNodes = node_map_to_array(JSON.parse(r.LiveNodes));
      r.DeadNodes = node_map_to_array(JSON.parse(r.DeadNodes));
      r.DecomNodes = node_map_to_array(JSON.parse(r.DecomNodes));
      return r;
    }

    $.get(
      '/jmx?qry=Hadoop:service=NameNode,name=NameNodeInfo',
      guard_with_startup_progress(function (resp) {
        var data = workaround(resp.beans[0]);
        var base = dust.makeBase(HELPERS);
        dust.render('datanode-info', base.push(data), function(err, out) {
          $('#tab-datanode').html(out);
          $('#ui-tabs a[href="#tab-datanode"]').tab('show');
        });
      })).error(ajax_error_handler);
  }

  function load_datanode_volume_failures() {

    var HELPERS = {
      'helper_date_tostring' : function (chunk, ctx, bodies, params) {
        var value = dust.helpers.tap(params.value, chunk, ctx);
        return chunk.write('' + new Date(Number(value)).toLocaleString());
      }
    };

    function workaround(r) {
      function node_map_to_array(nodes) {
        var res = [];
        for (var n in nodes) {
          var p = nodes[n];
          // Filter the display to only datanodes with volume failures.
          if (p.volfails > 0) {
            p.name = n;
            res.push(p);
          }
        }
        return res;
      }

      r.LiveNodes = node_map_to_array(JSON.parse(r.LiveNodes));
      return r;
    }

    $.get(
      '/jmx?qry=Hadoop:service=NameNode,name=NameNodeInfo',
      guard_with_startup_progress(function (resp) {
        var data = workaround(resp.beans[0]);
        var base = dust.makeBase(HELPERS);
        dust.render('datanode-volume-failures', base.push(data), function(err, out) {
          $('#tab-datanode-volume-failures').html(out);
          $('#ui-tabs a[href="#tab-datanode-volume-failures"]').tab('show');
        });
      })).error(ajax_error_handler);
  }

  function load_snapshot_info() {
    $.get(
      '/jmx?qry=Hadoop:service=NameNode,name=SnapshotInfo',
      guard_with_startup_progress(function (resp) {
      dust.render('snapshot-info', resp.beans[0], function(err, out) {
          $('#tab-snapshot').html(out);
          $('#ui-tabs a[href="#tab-snapshot"]').tab('show');
        });
      })).error(ajax_error_handler);
  }
  
  function load_storage_tiers(){
  	dust.render('storage-tiers', { world: "Betelgeuse" }, function(err, out) {
	   $('#tab-storage-tiers').html(out);
	   $('#ui-tabs a[href="#tab-storage-tiers"]').tab('show');
	});
	
  }
  function load_caching(){
  	dust.render('caching', { world: "Something" }, function(err, out) {
	   $('#tab-caching').html(out);
	   $('#ui-tabs a[href="#tab-caching"]').tab('show');
	});
	
  }
  function load_page() {
    var hash = window.location.hash;
    switch(hash) {
      case "#tab-datanode":
        load_datanode_info();
        break;
      case "#tab-datanode-volume-failures":
        load_datanode_volume_failures();
        break;
      case "#tab-snapshot":
        load_snapshot_info();
        break;
      case "#tab-startup-progress":
        load_startup_progress();
        break;
      case "#tab-overview":      
        load_overview();
        break;
      case "#tab-storage-tiers":        
    	  load_storage_tiers(); // Load storage_tiers tab 
    	  // Request to server to retrieve information
    	  $.getJSON( "/jmx?qry=Hadoop:service=NameNode,name=NameNodeInfo", function( data ) {
    	  	// Initiate nodes as array variable
  			var nodes = []; 
  			var memory = false, ram_disk = false, ssd_h = false, ssd = false, disk_h = false, disk = false, archive = false, remote = false;
  			var new_data=workaround(data.beans[0]);
  			//we need to access the objects inside the JSON retrieved and find our key (= LiveNodes)
  			//use of .each method to iterate through the items of JMX which are presented inside the new_data variable
  			$.each(new_data, function( key, val ) {
  				//Find our key which is LiveNodes
  				if (key=="LiveNodes"){
  					// val = val[0];  //This is what we first had. Only works where is 1 node. Where many nobjects (nodes), this takes only the first object (node)
  					// Each val represents a JSON schema for a node, so basically val is an array with JSON objects aka Nodes
  					// alert(val) returns [object Object], [object Object] which means there are two objects as the value of key LiveNodes 
  					// and so we have 2 nodes. We need the size of val in order to be able to loop through the nodes
  					// Use of ObjectLength method to return the size of an object - this case our object is -> val
  					var size = ObjectLength(val); 
  					var i;  					
  					// Iterate through all objects aka Nodes inside val
  					for(i=0;i<size;i++){
  						// For each Node we find
  					    $.each( val[i], function( key2, value2 ) {  					       
                            // Find name of node and append it to dropdown for nodes
                            if(key2 == "name"){
                                var options='<option value="' + value2 + '">' + value2 + '</option>';
                                $(options).appendTo("#dropdown");
                            }
                        });
  					}
  					for(i=0;i<size;i++){
  					    $.each(val[i], function( key2, value2 ) {
  					        // Find storage tiers available and mark their variable as true if found
                            // Use of .includes() because string is: storageType[..] and right now we only care about storage type                            
  					        if(key2.includes("storageType")){  	
  					             if(value2 == "MEMORY"){
  					                 memory = true;
  					             }
  					             else if(value2 == "RAM_DISK"){
  					                 ram_disk = true;
  					             }			         
  					             else if(value2 == "SSD_H"){
  					                 ssd_h = true;
  					             }     				
  					             else if(value2 == "SSD"){
  					                 ssd = true;
  					             }	       
  					             else if(value2 == "DISK_H"){
  					                 disk_h = true;
  					             }
  					             else if(value2 == "DISK"){
  					                 disk = true;
  					             }
  					             else if(value2 == "ARCHIVE"){
  					                 archive = true;
  					             }
  					             else if(value2 == "REMOTE"){
  					                 remote = true;
  					             }  					              					                                                                 
                            }        
  					    });  					    
  					}
  					// remove from dropdown those tiers whose variable is false, therefore are not available
  					var select = document.getElementById("storage-dropdown");                    
                    if (memory == false){
                        for(var x = 0; x<select.length; x++){
                            if(select.options[x].value == "MEMORY"){
                                select.remove(x);
                            }
                        }
                    }      
                    if(ram_disk == false){
                        for(var x = 0; x<select.length; x++){
                            if(select.options[x].value == "RAM_DISK"){
                                select.remove(x);
                            }
                        }
                    }
                    if(ssd_h == false){
                        for(var x = 0; x<select.length; x++){
                            if(select.options[x].value == "SSD_H"){
                                select.remove(x);
                            }
                        }
                    }
                    if(ssd == false){
                        for(var x = 0; x<select.length; x++){
                            if(select.options[x].value == "SSD"){
                                select.remove(x);
                            }
                        }
                    }  
                    if(disk_h == false){
                        for(var x = 0; x<select.length; x++){
                            if(select.options[x].value == "DISK_H"){
                                select.remove(x);
                            }
                        }
                    }
                    if(disk == false){
                        for(var x = 0; x<select.length; x++){
                            if(select.options[x].value == "DISK"){
                                select.remove(x);
                            }
                        }
                    }
                    if(archive == false){
                        for(var x = 0; x<select.length; x++){
                            if(select.options[x].value == "ARCHIVE"){
                                select.remove(x);
                            }
                        }
                    }
                    if(remote == false){
                        for(var x = 0; x<select.length; x++){
                            if(select.options[x].value == "REMOTE"){
                                select.remove(x);
                            }
                        }
                    }
  				}    		
  			}); 	
  			$("#dropdown").html($("#dropdown option").sort(function (a, b) {
    			return a.text == b.text ? 0 : a.text < b.text ? -1 : 1;
			}));
  			// set second child of storage-dropdown as selected by default
  			$('#storage-dropdown option:nth-child(2)').attr("selected", true); 
  			
  			var selectedNode = $("dropdown").val();
  			if(!(selectedNode == "Select All")){
  				$(".clear").html("");
  					$.each(new_data, function( key, val ) {
					if (key=="LiveNodes"){
						// Again, same method as before. Get size of object, and iterate to find details about objects aka Nodes
						var size = ObjectLength(val);
						var i;
						// Iterate through each Node						
						for(i=0;i<size;i++){
						    $.each( val[i], function( key2, value2 ) {
  						    if((key2 == "name")&&(value2 == selectedNode)){
  						    	// The selected Node from dropdown is found, now show the metrics		
  							   var capacity = val[i]['capacity'];
  							   capacity = formatBytes(capacity);			
  							   $("#capacity").html(capacity); 	
  							   var used = val[i]['usedSpace'];
  							   used = formatBytes(used);
  							   $("#used").html(used);
  							   var nonUsed = val[i]['nonDfsUsedSpace'];
  							   nonUsed = formatBytes(nonUsed);
  							   $("#nonUsed").html(nonUsed);
  							   var remaining = val[i]['remaining'];
  							   remaining = formatBytes(remaining);
  							   $("#dfsRemaining").html(remaining);
  							   var confCache = val[i]["configuredCache"];
  							   confCache = formatBytes(confCache);  							   
  							   $("#confCache").html(confCache);
  							   var cacheUsed = val[i]["cacheUsed"];
  							   cacheUsed = formatBytes(cacheUsed);
  							   $("#cacheUsed").html(cacheUsed + " (" + val[i]["cacheUsedPercent"] + "%)");
  							   var cacheRemain = val[i]["cacheRemaining"];
  							   cacheRemain = formatBytes(cacheRemain);
  							   $("#cacheRemaining").html(cacheRemain + " (" + val[i]["cacheRemainingPercent"] + "%)");
  							   // Find storages and their information
  							   // Show it
  							   var obj_stor;
  							   var count=1;
                               for(obj_stor in val[i]){
                                   if(obj_stor.includes("storageType")){
                                        var temp = obj_stor;
                                        var matches = temp.match(/\[(.*?)\]/);
                                        if (matches) {
                                            var position = matches[1];
                                        }
                                        // Getting the values
                                        var type = val[i]['storageType[' + position + ']'];
                                        var stor_id = val[i]['storageId[' + position + ']'];
                                        var stor_cap = val[i]['storageConfiguredCapacity[' + position + ']'];
                                        stor_cap=formatBytes(stor_cap);
                                        var stor_cap_used = val[i]['storageDFSUsed[' + position + ']'];
                                        stor_cap_used = formatBytes(stor_cap_used);
                                        var stor_cap_rem = val[i]['storageDFSRemaining[' + position + ']'];
                                        stor_cap_rem = formatBytes(stor_cap_rem);
                                        var stor_cache = val[i]['storageConfiguredCacheCapacity[' + position + ']'];
                                        stor_cache = formatBytes(stor_cache);
                                        var stor_cache_used = val[i]['storageCacheUsed[' + position + ']'];
                                        stor_cache_used = formatBytes(stor_cache_used);
                                        var stor_cache_rem = val[i]['storageCacheRemaining[' + position + ']'];
                                        stor_cache_rem = formatBytes(stor_cache_rem);
                                        
                                        // Creating the schema with each different Storage Tier
                                        if(document.getElementById("tier-" + type)==null){
                                            // means that the storage tier is not already created, append it
                                            var info='<tr class="tiers-info clear"><td><div class="tier-header"><h3><span class="tier-header-link" id="tier-' + type + '"><img class="arrow" id="tier-' + type + '-img" src="images/right-chevron.png" />' + type + '</span></h3></div><div class="tier-container" id="tier-' + type + '-cont"><div class="inside_container"><div class="stats-content"><h4>Statistics:</h4><table class="table table-bordered table-striped" id="storage-' + type + '"><tr><th align="center">StorageID</th><th align="center">Configured Capacity</th><th align="center"> DFS Used </th><th align="center">DFS Remaining</th><th align="center">Configured Cache Capacity</th><th align="center">Cache Used</th><th align="center">Cache Remaining</th></tr></table></div></div></div></td></tr>';
                                            $(info).appendTo("#table-for-showing-tiers");  
                                            count++;  
                                        }                                        
                                        var storage_stats = '<tr><td>' + stor_id + '</td><td>' + stor_cap + '</td><td>' + stor_cap_used + '</td><td>' + stor_cap_rem + '</td><td>' + stor_cache + '</td><td>' + stor_cache_used + '</td><td>' + stor_cache_rem + '</td></tr>';
                                        $(storage_stats).appendTo("#storage-" + type);                                        
                                    }
                                }
  						   }
					       });
					   }					   
					}				
				});
  			}
  			 
  			// On change of dropdown for nodes
  			$( "#dropdown" ).change(function() {
  				// Get selected value
  				var selected = $("#dropdown").val();
  				if (selected=="Select One"){
  					// Clear what is shown on table
  					$(".clear").html("");
  				}  				
  				else{
  				    $(".clear").html("");
  					$.each(new_data, function( key, val ) {
					if (key=="LiveNodes"){
						// Again, same method as before. Get size of object, and iterate to find details about objects aka Nodes
						var size = ObjectLength(val);
						var i;
						// Iterate through each Node						
						for(i=0;i<size;i++){
						    $.each( val[i], function( key2, value2 ) {
  						    if((key2 == "name")&&(value2 == selected)){
  						    	// The selected Node from dropdown is found, now show the metrics		
  							   var capacity = val[i]['capacity'];
  							   capacity = formatBytes(capacity);			
  							   $("#capacity").html(capacity); 	
  							   var used = val[i]['usedSpace'];
  							   used = formatBytes(used);
  							   $("#used").html(used);
  							   var nonUsed = val[i]['nonDfsUsedSpace'];
  							   nonUsed = formatBytes(nonUsed);
  							   $("#nonUsed").html(nonUsed);
  							   var remaining = val[i]['remaining'];
  							   remaining = formatBytes(remaining);
  							   $("#dfsRemaining").html(remaining);
  							   var confCache = val[i]["configuredCache"];
  							   confCache = formatBytes(confCache);  							   
  							   $("#confCache").html(confCache);
  							   var cacheUsed = val[i]["cacheUsed"];
  							   cacheUsed = formatBytes(cacheUsed);
  							   $("#cacheUsed").html(cacheUsed + " (" + val[i]["cacheUsedPercent"] + "%)");
  							   var cacheRemain = val[i]["cacheRemaining"];
  							   cacheRemain = formatBytes(cacheRemain);
  							   $("#cacheRemaining").html(cacheRemain + " (" + val[i]["cacheRemainingPercent"] + "%)");
  							   // Find storages and their information
  							   // Show it
  							   var obj_stor;
  							   var count=1;
                               for(obj_stor in val[i]){
                                   if(obj_stor.includes("storageType")){
                                        var temp = obj_stor;
                                        var matches = temp.match(/\[(.*?)\]/);
                                        if (matches) {
                                            var position = matches[1];
                                        }
                                        // Getting the values
                                        var type = val[i]['storageType[' + position + ']'];
                                        var stor_id = val[i]['storageId[' + position + ']'];
                                        var stor_cap = val[i]['storageConfiguredCapacity[' + position + ']'];
                                        stor_cap=formatBytes(stor_cap);
                                        var stor_cap_used = val[i]['storageDFSUsed[' + position + ']'];
                                        stor_cap_used = formatBytes(stor_cap_used);
                                        var stor_cap_rem = val[i]['storageDFSRemaining[' + position + ']'];
                                        stor_cap_rem = formatBytes(stor_cap_rem);
                                        var stor_cache = val[i]['storageConfiguredCacheCapacity[' + position + ']'];
                                        stor_cache = formatBytes(stor_cache);
                                        var stor_cache_used = val[i]['storageCacheUsed[' + position + ']'];
                                        stor_cache_used = formatBytes(stor_cache_used);
                                        var stor_cache_rem = val[i]['storageCacheRemaining[' + position + ']'];
                                        stor_cache_rem = formatBytes(stor_cache_rem);
                                        
                                        // Creating the schema with each different Storage Tier
                                        if(document.getElementById("tier-" + type)==null){
                                            // means that the storage tier is not already created, append it
                                            var info='<tr class="tiers-info clear"><td><div class="tier-header"><h3><span class="tier-header-link" id="tier-' + type + '"><img class="arrow" id="tier-' + type + '-img" src="images/right-chevron.png" />' + type + '</span></h3></div><div class="tier-container" id="tier-' + type + '-cont"><div class="inside_container"><div class="stats-content"><h4>Statistics:</h4><table class="table table-bordered table-striped" id="storage-' + type + '"><tr><th align="center">StorageID</th><th align="center">Configured Capacity</th><th align="center"> DFS Used </th><th align="center">DFS Remaining</th><th align="center">Configured Cache Capacity</th><th align="center">Cache Used</th><th align="center">Cache Remaining</th></tr></table></div></div></div></td></tr>';
                                            $(info).appendTo("#table-for-showing-tiers");  
                                            count++;  
                                        }                                        
                                        var storage_stats = '<tr><td>' + stor_id + '</td><td>' + stor_cap + '</td><td>' + stor_cap_used + '</td><td>' + stor_cap_rem + '</td><td>' + stor_cache + '</td><td>' + stor_cache_used + '</td><td>' + stor_cache_rem + '</td></tr>';
                                        $(storage_stats).appendTo("#storage-" + type);
                                        
                                    }
                                }
  						   }
					       });
					   }					   
					}				
				}); 
  				}
			});
			// Show details for second element of storage-dropdown by default so the page is not empy
			// first element is Select One so we chose the 2nd, whichever it is
		    var item = document.getElementById("storage-dropdown").options[1];
		    if((item!=null) && (item.selected)){
		          var default_item = document.getElementById("storage-dropdown").options[1].value;
		          var total_cap=0; var total_cap_used=0; var total_cap_rem=0;
                  var total_cache=0; var total_cache_used=0; var total_cache_remain=0;
		          $.each(new_data, function( key, val ) {
                    if (key=="LiveNodes"){
                        var size = ObjectLength(val);
                        var i;      
                        var start;
                        var end;    
                        for(i=0;i<size;i++){
                         $.each( val[i], function( key2, value2 ) {
                            if((key2.includes("storageType"))&&(value2 == default_item)){
                                var temp = key2; 
                                // get the index                                                               
                                var matches = temp.match(/\[(.*?)\]/);
                                if (matches) {
                                    var position = matches[1];
                                }
                                // Get metrics for Nodes that contain the selected tier
                                // Total Capacity - Total Capacity Used - Total Capacity Remaining                             
                                var tier_capacity = val[i]['storageConfiguredCapacity[' + position + ']']; //taking a number that is the actual bytes
                                total_cap=total_cap + tier_capacity;  //add with total as it is in bytes
                                var tier_used = val[i]['storageDFSUsed[' + position + ']'];
                                total_cap_used = total_cap_used + tier_used;                                
                                var tier_remaining = val[i]['storageDFSRemaining[' + position + ']'];
                                total_cap_rem = total_cap_rem + tier_remaining;                                                                
                                // Total Cache - Total Cache Used - Total Cache Remaining
                                var tier_cache = val[i]['storageConfiguredCacheCapacity[' + position + ']'];
                                total_cache = total_cache + tier_cache;
                                var tier_cache_used = val[i]['storageCacheUsed[' + position + ']'];
                                total_cache_used = total_cache_used + tier_cache_used;
                                var tier_cache_remain = val[i]['storageCacheRemaining[' + position + ']'];
                                total_cache_remain = total_cache_remain + tier_cache_remain;
                                var node_name = val[i]['name'];
                                // Search if node is already added in list. If it is already added then we must add its metrics as a total
                                if(document.getElementById(node_name)!=null){
                                    // Node is on the list. Get the capacities from HTML
                                    var tr = document.getElementById(node_name);
                                    var cells = tr.getElementsByTagName("td");
                                    // capacity
                                    var tmp_cap = tr.cells[1].innerHTML;
                                    if(tmp_cap.includes("TB")){
                                        tmp_cap = TBtoBytes(tmp_cap);
                                    }
                                    if(tmp_cap.includes("GB")){
                                        tmp_cap = GBtoBytes(parseFloat(tmp_cap));
                                    }
                                    else if(tmp_cap.includes("MB")){
                                        tmp_cap = MBtoBytes(parseFloat(tmp_cap));
                                    }
                                    else if(tmp_cap.includes("KB")){
                                        tmp_cap = KBtoBytes(parseFloat(tmp_cap));
                                    }
                                    else{
                                        tmp_cap = parseFloat(tmp_cap);
                                    }                                   
                                    tier_capacity = tier_capacity + tmp_cap; // bytes + bytes
                                    tier_capacity = formatBytes(tier_capacity); // now convert bytes to greater unit
                                    // capacity used
                                    var tmp_used = tr.cells[2].innerHTML;
                                    if(tmp_used.includes("TB")){
                                        tmp_used = TBtoBytes(tmp_used);
                                    }
                                    if(tmp_used.includes("GB")){
                                        tmp_used = GBtoBytes(parseFloat(tmp_used));
                                    }
                                    else if(tmp_used.includes("MB")){
                                        tmp_used = MBtoBytes(parseFloat(tmp_used));
                                    }
                                    else if(tmp_used.includes("KB")){
                                        tmp_used = KBtoBytes(parseFloat(tmp_used));
                                    }
                                    else{
                                        tmp_used = parseFloat(tmp_used);
                                    }       
                                    tier_used = tier_used + tmp_used; // bytes + bytes
                                    tier_used = formatBytes(tier_used);
                                    // capacity remaining                                                                        
                                    var tmp_rem = tr.cells[3].innerHTML;
                                    if(tmp_rem.includes("TB")){
                                        tmp_rem = TBtoBytes(tmp_rem);
                                    }
                                    if(tmp_rem.includes("GB")){
                                        tmp_rem = GBtoBytes(parseFloat(tmp_rem));
                                    }
                                    else if(tmp_rem.includes("MB")){
                                        tmp_rem = MBtoBytes(parseFloat(tmp_rem));
                                    }
                                    else if(tmp_rem.includes("KB")){
                                        tmp_rem = KBtoBytes(parseFloat(tmp_rem));
                                    }
                                    else{
                                        tmp_rem = parseFloat(tmp_rem);
                                    }  
                                    tier_remaining = tier_remaining + tmp_rem; // bytes + bytes
                                    tier_remaining = formatBytes(tier_remaining);
                                    // cache capacity
                                    var tmp_cache = tr.cells[4].innerHTML;
                                    if(tmp_cache.includes("TB")){
                                        tmp_cache = TBtoBytes(tmp_cache);
                                    }
                                    if(tmp_cache.includes("GB")){
                                        tmp_cache = GBtoBytes(parseFloat(tmp_cache));
                                    }                                    
                                    else if(tmp_cache.includes("MB")){
                                        tmp_cache = MBtoBytes(parseFloat(tmp_cache));
                                    }
                                    else if(tmp_cache.includes("KB")){
                                        tmp_cache = KBtoBytes(parseFloat(tmp_cache));
                                    }
                                    else{
                                        tmp_cache = parseFloat(tmp_cache);
                                    }  
                                    tier_cache = tier_cache + tmp_cache;
                                    tier_cache = formatBytes(tier_cache);
                                    // cache used
                                    var tmp_cache_used = tr.cells[5].innerHTML;
                                    if(tmp_cache_used.includes("TB")){
                                        tmp_cache_used = TBtoBytes(tmp_cache_used);
                                    }
                                    if(tmp_cache_used.includes("GB")){
                                        tmp_cache_used = GBtoBytes(parseFloat(tmp_cache_used));
                                    }
                                    else if(tmp_cache_used.includes("MB")){
                                        tmp_cache_used = MBtoBytes(parseFloat(tmp_cache_used));
                                    }
                                    else if(tmp_cache_used.includes("KB")){
                                        tmp_cache_used = KBtoBytes(parseFloat(tmp_cache_used));
                                    }
                                    else{
                                        tmp_cache_used = parseFloat(tmp_cache_used);
                                    } 
                                    tier_cache_used = tier_cache_used + tmp_cache_used;
                                    tier_cache_used = formatBytes(tier_cache_used);
                                    // cache remaining
                                    var tmp_cache_rem = tr.cells[6].innerHTML;
                                    if(tmp_cache_rem.includes("TB")){
                                        tmp_cache_rem = TBtoBytes(tmp_cache_rem);
                                    }
                                    if(tmp_cache_rem.includes("GB")){
                                        tmp_cache_rem = GBtoBytes(parseFloat(tmp_cache_rem));
                                    }
                                    else if(tmp_cache_rem.includes("MB")){
                                        tmp_cache_rem = MBtoBytes(parseFloat(tmp_cache_rem));
                                    }
                                    else if(tmp_cache_rem.includes("KB")){
                                        tmp_cache_rem = KBtoBytes(parseFloat(tmp_cache_rem));
                                    }
                                    else{
                                        tmp_cache_rem = parseFloat(tmp_cache_rem);
                                    }       
                                    tier_cache_remain = tier_cache_remain + tmp_cache_rem;
                                    tier_cache_remain = formatBytes(tier_cache_remain);                                                                         
                                    // change values inside table                                       
                                    tr.cells[1].innerHTML = tier_capacity;
                                    tr.cells[2].innerHTML = tier_used;
                                    tr.cells[3].innerHTML = tier_remaining;
                                    tr.cells[4].innerHTML = tier_cache;                                     
                                    tr.cells[5].innerHTML = tier_cache_used;
                                    tr.cells[6].innerHTML = tier_cache_remain;
                                }
                                else{
                                    // Node is not on list
                                    tier_capacity = formatBytes(tier_capacity);
                                    tier_used = formatBytes(tier_used);
                                    tier_remaining = formatBytes(tier_remaining);
                                    tier_cache = formatBytes(tier_cache);
                                    tier_cache_used = formatBytes(tier_cache_used);
                                    tier_cache_remain = formatBytes(tier_cache_remain);
                                    var node_info='<tr id="' + node_name + '" class="tier-nodes-table-row clear-table"><td>' + node_name + '</td>' + '<td>' + tier_capacity + '</td>' + '<td>' + tier_used + '</td>' +'<td>' + tier_remaining + '</td>' +'<td>' + 
                                    tier_cache + '</td>' + '<td>' + tier_cache_used + '</td>' + '<td>' + tier_cache_remain + '</td>' + 
                                            '<td><button class="details-link">Details</button></td><td class="stor-type">' + default_item + '</td></tr>';
                                   $(node_info).appendTo("#tier-nodes-table");  
                                }
                                // Append to table
                            }
                           });
                          }                          
                          //Print Total Tier Capacities
                          total_cap = formatBytes(total_cap);
                          $("#storage-capacity").html(total_cap);
                          total_cap_used = formatBytes(total_cap_used);
                          $("#storage-used").html(total_cap_used);
                          total_cap_rem = formatBytes(total_cap_rem);
                          $("#storage-dfsRemaining").html(total_cap_rem);
                          //Print Cache Capacities
                          total_cache = formatBytes(total_cache);
                          $("#storage-cache").html(total_cache);
                          total_cache_used = formatBytes(total_cache_used)
                          $("#storage-cacheUsed").html(total_cache_used);
                          total_cache_remain = formatBytes(total_cache_remain);
                          $("#storage-cacheRemaining").html(total_cache_remain);                    
                    }               
                });
		    }
			//On change of dropdown for storage tiers
			$( "#storage-dropdown" ).change(function() {
				//get selected value from dropdown
				var selected_tier = $("#storage-dropdown").val();
				//Initiate variables for Total Metrics
				var total_cap=0; var total_cap_used=0; var total_cap_rem=0;
				var total_cache=0; var total_cache_used=0; var total_cache_remain=0;
				if (selected_tier=="Select One"){
					// Clear what metrics are shown
  					$(".clear-tier").html("");
  					$(".clear-table").html("");
  				}
  				else{
  					$(".clear-table").remove();
  					$(".clear-tier").html("");
  					$(".node-clear").remove();  					
  					$.each(new_data, function( key, val ) {
					if (key=="LiveNodes"){
						var size = ObjectLength(val);
						var i;		
						var start;
						var end;	
						for(i=0;i<size;i++){
  					     $.each( val[i], function( key2, value2 ) {
  					         // Again use of .includes()  						    
  						    if((key2.includes("storageType"))&&(value2 == selected_tier)){
  						        var temp = key2; 
  						        // temp string is: storageType[..] and because based on whats in the brackets, we will be able to show information
  						        // of the correct Node and its' storage type, we need to get whats inside the brackets
  						        // basically we get the index 						                                        
                                var matches = temp.match(/\[(.*?)\]/);
                                if (matches) {
                                    var position = matches[1];
                                }
                                // Get metrics for Nodes that contain the selected tier
  						    	// Because we also need the Total of the metrics for selected tier
  						    	// We first iterate through all val[i] who represent the JSON schema
  						    	// If that val[i] contains selected storage tier, then we add its metrics to the total variables
  						    	// Total Capacity - Total Capacity Used - Total Capacity Remaining							   
  								var tier_capacity = val[i]['storageConfiguredCapacity[' + position + ']']; //taking a number that is the actual bytes
  								total_cap=total_cap + tier_capacity;  //add with total as it is in bytes
  								
  							   	var tier_used = val[i]['storageDFSUsed[' + position + ']'];
  							   	total_cap_used = total_cap_used + tier_used;  							   	
  							   	
  							   	var tier_remaining = val[i]['storageDFSRemaining[' + position + ']'];
  							   	total_cap_rem = total_cap_rem + tier_remaining;	  							   	  							   
  							   	
  							   	// Total Cache - Total Cache Used - Total Cache Remaining
  							   	var tier_cache = val[i]['storageConfiguredCacheCapacity[' + position + ']'];
  							   	total_cache = total_cache + tier_cache;
  							   	
  							   	var tier_cache_used = val[i]['storageCacheUsed[' + position + ']'];
  							   	total_cache_used = total_cache_used + tier_cache_used;
  							   	
  							   	var tier_cache_remain = val[i]['storageCacheRemaining[' + position + ']'];
  							   	total_cache_remain = total_cache_remain + tier_cache_remain;
  							   	
  							   	var node_name = val[i]['name'];
  							   	// Search if node is already added in list. If it is already added then we must add its metrics as a total
  							   	if(document.getElementById(node_name)!=null){
  							   		// Node is on the list. Get the capacities from HTML
  							   		var tr = document.getElementById(node_name);
  							   		var cells = tr.getElementsByTagName("td");
  							   		// capacity
  							   		var tmp_cap = tr.cells[1].innerHTML;
  							   		if(tmp_cap.includes("TB")){
  							   		    tmp_cap = TBtoBytes(tmp_cap);
  							   		}
  							   		if(tmp_cap.includes("GB")){
  							   		    tmp_cap = GBtoBytes(parseFloat(tmp_cap));
  							   		}
  							   		else if(tmp_cap.includes("MB")){
  							   		    tmp_cap = MBtoBytes(parseFloat(tmp_cap));
  							   		}
  							   		else if(tmp_cap.includes("KB")){
  							   		    tmp_cap = KBtoBytes(parseFloat(tmp_cap));
  							   		}
  							   		else{
  							   		    tmp_cap = parseFloat(tmp_cap);
  							   		}  							   		
  							   		tier_capacity = tier_capacity + tmp_cap; // bytes + bytes
  							   		tier_capacity = formatBytes(tier_capacity); // now convert bytes to greater unit
  							   		// capacity used
  							   		var tmp_used = tr.cells[2].innerHTML;
  							   		if(tmp_used.includes("TB")){
                                        tmp_used = TBtoBytes(tmp_used);
                                    }
                                    if(tmp_used.includes("GB")){
                                        tmp_used = GBtoBytes(parseFloat(tmp_used));
                                    }
                                    else if(tmp_used.includes("MB")){
                                        tmp_used = MBtoBytes(parseFloat(tmp_used));
                                    }
                                    else if(tmp_used.includes("KB")){
                                        tmp_used = KBtoBytes(parseFloat(tmp_used));
                                    }
                                    else{
                                        tmp_used = parseFloat(tmp_used);
                                    }       
  							   	    tier_used = tier_used + tmp_used; // bytes + bytes
                                    tier_used = formatBytes(tier_used);
                                    // capacity remaining                                                                        
  							   		var tmp_rem = tr.cells[3].innerHTML;
  							   		if(tmp_rem.includes("TB")){
                                        tmp_rem = TBtoBytes(tmp_rem);
                                    }
                                    if(tmp_rem.includes("GB")){
                                        tmp_rem = GBtoBytes(parseFloat(tmp_rem));
                                    }
                                    else if(tmp_rem.includes("MB")){
                                        tmp_rem = MBtoBytes(parseFloat(tmp_rem));
                                    }
                                    else if(tmp_rem.includes("KB")){
                                        tmp_rem = KBtoBytes(parseFloat(tmp_rem));
                                    }
                                    else{
                                        tmp_rem = parseFloat(tmp_rem);
                                    }  
  							   		tier_remaining = tier_remaining + tmp_rem; // bytes + bytes
  							   		tier_remaining = formatBytes(tier_remaining);
  							   		// cache capacity
  							   		var tmp_cache = tr.cells[4].innerHTML;
  							   		if(tmp_cache.includes("TB")){
                                        tmp_cache = TBtoBytes(tmp_cache);
                                    }
                                    if(tmp_cache.includes("GB")){
                                        tmp_cache = GBtoBytes(parseFloat(tmp_cache));
                                    }                                    
                                    else if(tmp_cache.includes("MB")){
                                        tmp_cache = MBtoBytes(parseFloat(tmp_cache));
                                    }
                                    else if(tmp_cache.includes("KB")){
                                        tmp_cache = KBtoBytes(parseFloat(tmp_cache));
                                    }
                                    else{
                                        tmp_cache = parseFloat(tmp_cache);
                                    }  
  							   		tier_cache = tier_cache + tmp_cache;
                                    tier_cache = formatBytes(tier_cache);
                                    // cache used
  							   		var tmp_cache_used = tr.cells[5].innerHTML;
  							   		if(tmp_cache_used.includes("TB")){
                                        tmp_cache_used = TBtoBytes(tmp_cache_used);
                                    }
                                    if(tmp_cache_used.includes("GB")){
                                        tmp_cache_used = GBtoBytes(parseFloat(tmp_cache_used));
                                    }
                                    else if(tmp_cache_used.includes("MB")){
                                        tmp_cache_used = MBtoBytes(parseFloat(tmp_cache_used));
                                    }
                                    else if(tmp_cache_used.includes("KB")){
                                        tmp_cache_used = KBtoBytes(parseFloat(tmp_cache_used));
                                    }
                                    else{
                                        tmp_cache_used = parseFloat(tmp_cache_used);
                                    } 
  							   		tier_cache_used = tier_cache_used + tmp_cache_used;
                                    tier_cache_used = formatBytes(tier_cache_used);
  							   		// cache remaining
  							   		var tmp_cache_rem = tr.cells[6].innerHTML;
  							   		if(tmp_cache_rem.includes("TB")){
                                        tmp_cache_rem = TBtoBytes(tmp_cache_rem);
                                    }
                                    if(tmp_cache_rem.includes("GB")){
                                        tmp_cache_rem = GBtoBytes(parseFloat(tmp_cache_rem));
                                    }
                                    else if(tmp_cache_rem.includes("MB")){
                                        tmp_cache_rem = MBtoBytes(parseFloat(tmp_cache_rem));
                                    }
                                    else if(tmp_cache_rem.includes("KB")){
                                        tmp_cache_rem = KBtoBytes(parseFloat(tmp_cache_rem));
                                    }
                                    else{
                                        tmp_cache_rem = parseFloat(tmp_cache_rem);
                                    }   	
  							   		tier_cache_remain = tier_cache_remain + tmp_cache_rem;
  							   		tier_cache_remain = formatBytes(tier_cache_remain);  							   		  							   		
                                    // change values inside table  							   			
  							   		tr.cells[1].innerHTML = tier_capacity;
  							   		tr.cells[2].innerHTML = tier_used;
  							   		tr.cells[3].innerHTML = tier_remaining;
  							   		tr.cells[4].innerHTML = tier_cache;  							   		
  							   		tr.cells[5].innerHTML = tier_cache_used;
  							   		tr.cells[6].innerHTML =	tier_cache_remain;
  							   	}
  							   	else{
  							   		// Node is not on list
  							   		tier_capacity = formatBytes(tier_capacity);
  							   		tier_used = formatBytes(tier_used);
  							   		tier_remaining = formatBytes(tier_remaining);
  							   		tier_cache = formatBytes(tier_cache);
  							   		tier_cache_used = formatBytes(tier_cache_used);
  							   		tier_cache_remain = formatBytes(tier_cache_remain);
  							   		var node_info='<tr id="' + node_name + '" class="tier-nodes-table-row clear-table"><td>' + node_name + '</td>' + '<td>' + tier_capacity + '</td>' + '<td>' + tier_used + '</td>' +'<td>' + tier_remaining + '</td>' +'<td>' + 
  							   		tier_cache + '</td>' + '<td>' + tier_cache_used + '</td>' + '<td>' + tier_cache_remain + '</td>' + 
  							   				'<td><button class="details-link">Details</button></td><td class="stor-type">' + selected_tier + '</td></tr>';
  							   	   $(node_info).appendTo("#tier-nodes-table");  
  							   	}
                            	                             							   	
  						    }
					       });
					      }
					      //After for loop finishes, it means we have iterated through all LiveNodes
					      //who may contain the selected storage tier. So now we print the total metrics
					      //Print Tier Capacities
					      total_cap = formatBytes(total_cap);
					      $("#storage-capacity").html(total_cap);
					      total_cap_used = formatBytes(total_cap_used);
					      $("#storage-used").html(total_cap_used);
					      total_cap_rem = formatBytes(total_cap_rem);
					      $("#storage-dfsRemaining").html(total_cap_rem);
					      //Print Cache Capacities
					      total_cache = formatBytes(total_cache);
					      $("#storage-cache").html(total_cache);
					      total_cache_used = formatBytes(total_cache_used)
					      $("#storage-cacheUsed").html(total_cache_used);
					      total_cache_remain = formatBytes(total_cache_remain);
  						  $("#storage-cacheRemaining").html(total_cache_remain);					
					}				
				}); 
  				} 	
			});
			
			
		});				
		// Use of .on() helps us get events on dynamically created elements such as our tier-containers
	   $("table").on('click', '#tier-DISK', function(){
           	$("#tier-DISK-cont").fadeToggle("fast", function(){
               	if($("#tier-DISK-cont").is(":visible")){
                   	document.getElementById("tier-DISK-img").src="images/down-chevron.png";
               	} else{
                   	document.getElementById("tier-DISK-img").src="images/right-chevron.png";
               	}
           	});         
       	});
        $("table").on('click', '#tier-MEMORY', function(){
           	$("#tier-MEMORY-cont").fadeToggle("fast", function(){
               	if($("#tier-MEMORY-cont").is(":visible")){
                   	document.getElementById("tier-MEMORY-img").src="images/down-chevron.png";
               	} else{
                   	document.getElementById("tier-MEMORY-img").src="images/right-chevron.png";
               	}
           	});    
        });
        $("table").on('click', '#tier-SSD', function(){
           	$("#tier-SSD-cont").fadeToggle("fast", function(){
               	if($("#tier-SSD-cont").is(":visible")){
                   	document.getElementById("tier-SSD-img").src="images/down-chevron.png";
               	} else{
               	 	document.getElementById("tier-SSD-img").src="images/right-chevron.png";
               	}
           	});         
        });
        $("table").on('click', '#tier-ARCHIVE', function(){
            $("#tier-ARCHIVE-cont").fadeToggle("fast", function(){
                if($("#tier-ARCHIVE-cont").is(":visible")){
                    document.getElementById("tier-ARCHIVE-img").src="images/down-chevron.png";
                } else{
                    document.getElementById("tier-ARCHIVE-img").src="images/right-chevron.png";
                }
            });         
        });
        $("table").on('click', '#tier-RAM_DISK', function(){
            $("#tier-RAM_DISK-cont").fadeToggle("fast", function(){
                if($("#tier-RAM_DISK-cont").is(":visible")){
                    document.getElementById("tier-RAM_DISK-img").src="images/down-chevron.png";
                } else{
                    document.getElementById("tier-RAM_DISK-img").src="images/right-chevron.png";
                }
            });         
        });
        $("table").on('click', '#tier-DISK_H', function(){
            $("#tier-DISK_H-cont").fadeToggle("fast", function(){
                if($("#tier-DISK_H-cont").is(":visible")){
                    document.getElementById("tier-DISK_H-img").src="images/down-chevron.png";
                }else{
                    document.getElementById("tier-DISK_HK-img").src="images/right-chevron.png";
                }
            });         
        });
        $("table").on('click', '#tier-SSD_H', function(){
            $("#tier-SSD_H-cont").fadeToggle("fast", function(){
                if($("#tier-SSD_H-cont").is(":visible")){
                    document.getElementById("tier-SSD_H-img").src="images/down-chevron.png";
                } else{
                    document.getElementById("tier-SSD_H-img").src="images/right-chevron.png";
                }
            });         
        });
        $("table").on('click', '#tier-REMOTE', function(){
            $("#tier-REMOTE-cont").fadeToggle("fast", function(){
                if($("#tier-REMOTE-cont").is(":visible")){
                    document.getElementById("tier-REMOTE-img").src="images/down-chevron.png";
                }else{
                    document.getElementById("tier-REMOTE-img").src="images/right-chevron.png";
                }
            });         
        });
        // On click of details button show popup window with details about the selected node
        $("table").on('click', '.details-link', function(){        	
        	$(".node-clear").remove(); 
        	$(".node-clear").html("");        	
			var $row = $(this).closest("tr");        // Finds the closest row <tr> 
    		var	_node = $row.find("td:nth-child(1)").text();	// Gets first child td element of that row
    		var _type = $row.find("td:nth-child(9)").text();	// Gets ninth child td element of that row
    		$("#node").empty().append(_node);  
    		$("#type").empty().append("Showing details for: " + _type + " tier");  		
    		$.getJSON( "/jmx?qry=Hadoop:service=NameNode,name=NameNodeInfo", function( data ) {
    			var nodes = []; 
  				var new_data=workaround(data.beans[0]);
  				var total_cap=0; var total_cap_used=0; var total_cap_rem=0;
                var total_cache=0; var total_cache_used=0; var total_cache_remain=0;
  				$.each(new_data, function( key, val ){
  					//Find our key which is LiveNodes
  					if (key=="LiveNodes"){
  					var size = ObjectLength(val); 
  					var i;  					
  					for(i=0;i<size;i++){
  						// For each Node we find
  					    $.each( val[i], function( key2, value2 ) {  					       
                            if((key2 == "name")&&(value2 == _node)){
                            	var obj_stor;
  							   	var count=0;
                               	for(obj_stor in val[i]){
									if(obj_stor.includes("storageType")){
                                        count++;
                                        var temp = obj_stor;
                                        var matches = temp.match(/\[(.*?)\]/);
                                        if (matches) {
                                            var position = matches[1];
                                        }
                                        // Getting the values
                                        var type = val[i]['storageType[' + position + ']'];
                                        if(type == _type){
                                        	var node_stor_id = val[i]['storageId[' + position + ']'];
                                        	var node_stor_cap = val[i]['storageConfiguredCapacity[' + position + ']'];
                                        	node_stor_cap=formatBytes(node_stor_cap);
                                        	var node_stor_cap_used = val[i]['storageDFSUsed[' + position + ']'];
                                        	node_stor_cap_used = formatBytes(node_stor_cap_used);
                                        	var node_stor_cap_rem = val[i]['storageDFSRemaining[' + position + ']'];
                                        	node_stor_cap_rem = formatBytes(node_stor_cap_rem);
                                        	var node_stor_cache = val[i]['storageConfiguredCacheCapacity[' + position + ']'];
                                        	node_stor_cache = formatBytes(node_stor_cache);
                                        	var node_stor_cache_used = val[i]['storageCacheUsed[' + position + ']'];
                                        	node_stor_cache_used = formatBytes(node_stor_cache_used);
                                        	var node_stor_cache_rem = val[i]['storageCacheRemaining[' + position + ']'];                                        	
                                        	node_stor_cache_rem = formatBytes(node_stor_cache_rem);                                        	
                                        	// Creating the schema 
                                        	if(document.getElementById(node_stor_id)==null){
                                        	    var storage_stats = '<tr class="node-clear"><td id=' + node_stor_id + '>' + node_stor_id + '</td><td>' + node_stor_cap + '</td><td>' + node_stor_cap_used + '</td><td>' + node_stor_cap_rem + '</td><td>' + node_stor_cache + '</td><td>' + node_stor_cache_used + '</td><td>' + node_stor_cache_rem + '</td></tr>'
                                                $(storage_stats).appendTo("#node-type-table");
                                        	}                                   	                                        	
                                        }
                                        
                                    }
                                }
                            }
                        });
  					}
  					for(i=0;i<size;i++){
                        // For each Node we find
                        $.each( val[i], function( key2, value2 ) {  
                            if((key2.includes("storageType"))&&(value2 == _type)){
                                var temp = key2;                                
                                var matches = temp.match(/\[(.*?)\]/);
                                if (matches) {
                                    var position = matches[1];
                                }
                                // Total Capacity - Total Capacity Used - Total Capacity Remaining                             
                                var tier_capacity = val[i]['storageConfiguredCapacity[' + position + ']']; //taking a number that is the actual bytes
                                total_cap=total_cap + tier_capacity;  //add with total as it is in bytes
                               
                                var tier_used = val[i]['storageDFSUsed[' + position + ']'];
                                total_cap_used = total_cap_used + tier_used;                                
                                
                                var tier_remaining = val[i]['storageDFSRemaining[' + position + ']'];
                                total_cap_rem = total_cap_rem + tier_remaining;                                                                
                                
                                // Total Cache - Total Cache Used - Total Cache Remaining
                                var tier_cache = val[i]['storageConfiguredCacheCapacity[' + position + ']'];
                                total_cache = total_cache + tier_cache;
                                
                                var tier_cache_used = val[i]['storageCacheUsed[' + position + ']'];
                                total_cache_used = total_cache_used + tier_cache_used;
                                
                                var tier_cache_remain = val[i]['storageCacheRemaining[' + position + ']'];
                                total_cache_remain = total_cache_remain + tier_cache_remain;
                                
                                
                            }                            
                        });
                    }
                    total_cap = formatBytes(total_cap);
                    total_cap_used = formatBytes(total_cap_used);
                    total_cap_rem = formatBytes(total_cap_rem);
                    total_cache = formatBytes(total_cache);
                    total_cache_remain = formatBytes(total_cache_remain);
                    total_cache_used = formatBytes(total_cache_used);
                    $("#tot_cap_tier").empty().append(total_cap);
                    $("#tot_dfs_used").empty().append(total_cap_used);
                    $("#tot_dfs_rem").empty().append(total_cap_rem);
                    $("#tot_cache_cap").empty().append(total_cache);
                    $("#tot_cache_used").empty().append(total_cache_used);
                    $("#tot_cache_rem").empty().append(total_cache_remain);
  				}    		
  				});
    		});
			$( "#myPopup" ).toggle( "fast", function() {});
        });		        
        // Close popup when user clicks close button
        $(document).on('click', '.close', function(){
            $(".node-clear").remove();
        	var popup = document.getElementById('myPopup');
			$( "#myPopup" ).hide( "fast", function() {
    			$(".node-clear").remove();
    			
  			});
        });        
        function TBtoBytes(unit){
           return unit * 1099511627776;
        };
        function GBtoBytes(unit){
          return unit * 1024 * 1024 * 1024;           
        };
        function MBtoBytes(unit){
            return unit * 1024 * 1024;
        }
        function KBtoBytes(unit){
            return unit * 1024;
        }
        // Function to return size - length - how many objects the key LiveNodes containes
		function ObjectLength( object ) {
            var length = 0;
            for( var key in object ) {
                if( object.hasOwnProperty(key) ) {
                    ++length;
                }
            }
            return length;
        };
        // Function to convert Bytes - MBytes - GBytes etc
        function formatBytes(bytes,decimals) {
   			if(bytes == 0) return '0 Bytes';
   			var k = 1024;
   			var dm = decimals + 1 || 3;
   			var sizes = ['Bytes', 'KB', 'MB', 'GB', 'TB', 'PB', 'EB', 'ZB', 'YB'];
   			var i = Math.floor(Math.log(bytes) / Math.log(k));
   			return parseFloat((bytes / Math.pow(k, i)).toFixed(dm)) + ' ' + sizes[i];
		}		
		function formatTtl(ttl){
		    if(ttl == "never") return "never";
		    var result = "";
		    ttl = ttl / 1000; // convert from miliseconds to seconds first
		    if((ttl % 86400) == 0){
		        result = ttl/86400;
		        if(result == 1){
		            result = result + " Day";
		        }		        
		        else{
		            result = result + " Days";
		        }
		    }
		    else if((ttl % 3600) == 0){
		        result = ttl/3600;
		        if(result == 1){
                    result = result + " Hour";
                }               
                else{
                    result = result + " Hours";
                }
		    }
		    else if((ttl % 60) == 0){
		        result = ttl/60;
		        if(result == 1){
                    result = result + " Minute";
                }               
                else{
                    result = result + " Minutes";
                }
		    }
		    else{
		        result = ttl;
		        if(result == 1){
                    result = result + " Second";
                }               
                else{
                    result = result + " Seconds";
                }
		    }
		    return result;
		}
   		function workaround(r) {
			function node_map_to_array(nodes) {
		        var res = [];
		        for (var n in nodes) {
		          var p = nodes[n];
		          p.name = n;
		          res.push(p);
		        }
		        return res;
		      }		  
		    r.LiveNodes = node_map_to_array(JSON.parse(r.LiveNodes));
			return r;
		};
		
    	  break;
   	  case "#tab-caching":
   	    $(".node-clear").remove();   	    	    
      	load_caching();    
      	var directive_ID=0;
      	$.getJSON( "/jmx?qry=Hadoop:service=NameNode,name=NameNodeInfo", function( data ) {
      		var new_data=workaroundCachePools(data.beans[0]);
      		var directives_data=workaroundCacheDirectives(data.beans[0]);
      		// populate dropdown for pools
      		$.each(new_data, function(key,val){
      			if (key=="CachePools"){
  					var i;
  					var size = ObjectLength(val);  					
					for(i=0;i<size;i++){
						$.each(val[i], function(key2,value2){
								if(key2 == "name"){
								    var pool='<option class="clear-selections" value="' + value2 + '">' + value2 + '</option>';
                                    $(pool).appendTo("#pools-dropdown");
                                    $(pool).appendTo("#add-pool-directive-dropdown");
                                    $(pool).appendTo("#edit-pool-directive-dropdown");
								}
							});	
					}
				}
      		});
      		var selected_val = sessionStorage.getItem("selected"); // get from local storage
      		if(selected_val!=null){
      			$("#pools-dropdown").val(selected_val);
      			if(selected_val=="All"){
      				populateAll();
      			}
      			else{
      				populateSpecific(selected_val);
      			}
      		}
      		else{
      			$("#pools-dropdown").val("All");
      			populateAll();
      		}      		    		      	    
  			$("#pools-dropdown").change(function() {
				//get selected value from dropdown
				var selected_pool = $("#pools-dropdown").val();
				sessionStorage.setItem("selected", selected_pool);	//save to local storage
				if(selected_pool == "All"){
					//call function to populate all
					$(".clear-pool").remove();
					$(".clear-directive").remove();
					populateAll();
				}
				else{
					$(".clear-pool").remove();
					$(".clear-directive").remove();
					populateSpecific(selected_pool);
				}
			});
      	});      	
      	// on add pool click, show popup
      	$("#add-pool-button").click(function(){ 
      		var mem = false, ram_disk = false, ssd_h = false, ssd = false, disk_h = false, disk = false, archive = false, remote = false;
      		$.getJSON( "/jmx?qry=Hadoop:service=NameNode,name=NameNodeInfo", function( data ) {
      			var new_data=workaroundCachePools(data.beans[0]);
      			$.each(new_data, function( key, val ) {      				
  					if (key=="CachePools"){
  						if(!((val == "")||(val == null))){
  							val=val[0];
  							var poolOwner = val['owner'];
                        	var group = val['group'];
                        	var mode = val['mode'];
                        	var owner_text = $('#pool-owner');
                        	owner_text.val(""+poolOwner);  
                        	var group_text = $('#pool-group');
                        	group_text.val(""+group);
                        	var mode_text = $('#pool-mode');
                        	mode_text.val(""+mode);
  						}					
  					}
  				});
  				var new_data2=workaround(data.beans[0]);  				
  				$.each(new_data2, function( key, val ) {
  					if (key=="LiveNodes"){
  						var size = ObjectLength(val);
                       	var i;                                
                       	for(i=0;i<size;i++){
                        	$.each( val[i], function( key2, value2 ) {
                            	if(key2.includes("storageType")){
                                	var temp = key2; 
                                	// get the index                                                               
                                	var matches = temp.match(/\[(.*?)\]/);
                                	if (matches) {
                                    	var position = matches[1];
                                	}
                                	var type_stor = val[i]['storageType[' + position + ']'];
                                	if(type_stor == "MEMORY"){
                                	    mem = true;
                                	}
                                	else if(type_stor == "RAM_DISK"){
                                	    ram_disk = true;
                                	}
                                	else if(type_stor =="SSD_H"){
                                	    ssd_h = true;
                                	}
                                	else if(type_stor == "SSD"){
                                	    ssd = true;
                                	}
                                	else if(type_stor == "DISK_H"){
                                	    disk_h = true;
                                	}
                                	else if(type_stor == "DISK"){
                                	    disk = true;
                                	}
                                	else if(type_stor == "ARCHIVE"){
                                	    archive = true;
                                	}
                                	else if(type_stor == "REMOTE"){
                                	    remote = true;
                                	}
                                                   
                           		}
                         	});
                        }
                        if(mem == false){
                            $("#memory-line-add-pool").remove();                            
                        }
                        if(ram_disk == false){
                            $("#ram-disk-line-add-pool").remove();
                        }
                        if(ssd_h == false){
                            $("#ssd-h-line-add-pool").remove();
                        }
                        if(ssd == false){
                            $("#ssd-line-add-pool").remove();
                        }
                        if(disk_h == false){
                            $("#disk-h-line-add-pool").remove();
                        }
                        if(disk == false){
                            $("#disk-line-add-pool").remove();
                        }
                        if(archive == false){
                            $("#archive-line-add-pool").remove();
                        }
                        if(remote == false){
                            $("#remote-line-add-pool").remove();
                        }
  					}  			
  				});
  			});
			$( "#add-pool-popup" ).toggle("fast");
        });	
        // close add pool popup
        $(document).on('click', '#close-add-pool', function(){
        	$("#add-pool-name").val('');
            $("#MEMORY-bytes").val('');
            $("#RAM_DISK-bytes").val('');
            $("#SSD_H-bytes").val('');
            $("#SSD-bytes").val('');
            $("#DISK_H-bytes").val('');
            $("#DISK-bytes").val('');
            $("#ARCHIVE-bytes").val('');
            $("#REMOTE-bytes").val(''); 
            $("#error-field").empty();
            $("#error-name").empty();
            $("#add-pool-ttl").val('');
            $("#add-pool-ttl-dropdown").val('h');
            $(".add-pool-bytes-dropdown").val("MB");
			$( "#add-pool-popup" ).hide( "fast", function() {});
        }); 

        // do the actual creation of new pool
        $("#ok-add-pool").click(function(){
           	var typesArr="";
           	var limitsArr=""; 
			var poolName = $("#add-pool-name").val();
           	var poolOwner = $("#pool-owner").val();
           	var poolGroup = $("#pool-group").val();
           	var poolMode = $("#pool-mode").val();
           	var maxttl = $("#add-pool-ttl").val();           	
           	var time = $("#add-pool-ttl-dropdown").val();
           	var bytesIn = '-bytes';               	
           	var diskBytes,memBytes,ssdBytes,ssd,archiveBytes,ram_diskBytes,
           		disk_hBytes,ssd_hBytes,remoteBytes;
           	var chars = false;       
               
            // check if maxttl is empty - implying never should be used
            if(!((maxttl=='')||(maxttl==null))){                
                if(!(maxttl == '0')){
                    maxttl = maxttl + time;
                }            
                else{
                    maxttl = '';
                }    
            }
            	           	
           	if(document.getElementById('DISK'+bytesIn)!=null){  
           		diskBytes = $("#DISK" + bytesIn).val();           		
           		if(!((diskBytes == ""))){
           			// not empty
           			if(diskBytes.match(/^\d+$/)){
           				if(diskBytes > 0){
           				    var diskChoice="";
                            if(document.getElementById('DISK-mb').selected){
                                diskChoice="m";
                            }
                            else if(document.getElementById('DISK-gb').selected){
                                diskChoice="g";
                            }    
                            else if(document.getElementById('DISK-kb').selected){
                                diskChoice="k";
                            }                                                  
           					if(typesArr==""){
           						typesArr = typesArr + 'D';
           					}
           					else{
           						typesArr = typesArr + '-D';
           					}
           					if(limitsArr==""){
           						limitsArr = limitsArr + diskBytes + diskChoice;
           					}         
           					else{
           						limitsArr = limitsArr + '-' + diskBytes + diskChoice;
           					}
           				}           				
           			}
           			else{
           				chars = true;
           			}           			
           		}           		           		           		
           	}
           	if(document.getElementById('MEMORY'+bytesIn)!=null){           		
           		memBytes = $("#MEMORY" + bytesIn).val();           		
           		if(!((memBytes == ""))){
           			// not empty
           			if(memBytes.match(/^\d+$/)){
           				if(memBytes>0){
           				    var memChoice="";
                            if(document.getElementById('MEMORY-mb').selected){
                                memChoice="m";
                            }
                            else if(document.getElementById('MEMORY-gb').selected){
                                memChoice="g";
                            }            
                            else if(document.getElementById('MEMORY-kb').selected){
                                memChoice="k";
                            }                
           					if(typesArr==""){
           						typesArr = typesArr + 'M';
           					}
           					else{
           						typesArr = typesArr + '-M';
           					}
           					if(limitsArr==""){
           						limitsArr = limitsArr + memBytes + memChoice;
           					}         
           					else{
           						limitsArr = limitsArr + '-' + memBytes + memChoice;
           					}
           				}           				
           			}
           			else{
           				chars = true;
           			}           			           			
           		}         			           	
           	}
           	if(document.getElementById('SSD'+bytesIn)!=null){           		           		
           		ssdBytes = $("#SSD" + bytesIn).val();           		
           		if(!((ssdBytes == ""))){
           			// not empty
           			if(ssdBytes.match(/^\d+$/)){
           				if(ssdBytes>0){
           				    var ssdChoice="";
                            if(document.getElementById('SSD-mb').selected){
                                ssdChoice="m";
                            }
                            else if(document.getElementById('SSD-gb').selected){
                                ssdChoice="g";
                            }  
                            else if(document.getElementById('SSD-kb').selected){
                                ssdChoice="k";
                            }                          
           					if(typesArr==""){
           						typesArr = typesArr + 'S';
           					}
           					else{
           						typesArr = typesArr + '-S';
           					}
           					if(limitsArr==""){
           						limitsArr = limitsArr + ssdBytes + ssdChoice;
           					}         
           					else{
           						limitsArr = limitsArr + '-' + ssdBytes + ssdChoice;
           					}
           				}           				
           			}
           			else{
           				chars = true;
           			}           			
           		}           					           		
           	}
           	if(document.getElementById('ARCHIVE'+bytesIn)!=null){           		
           		archiveBytes=$("#ARCHIVE" + bytesIn).val();           		
           		if(!((archiveBytes == ""))){
           			// not empty
           			if(archiveBytes.match(/^\d+$/)){
           				if(archiveBytes>0){
           				    var archiveChoice="";
                            if(document.getElementById('ARCHIVE-mb').selected){
                                archiveChoice="m";
                            }
                            else if(document.getElementById('ARCHIVE-gb').selected){
                                archiveChoice="g";
                            }
                            else if(document.getElementById('ARCHIVE-kb').selected){
                                archiveChoice="k";
                            }
           					if(typesArr==""){
           						typesArr = typesArr + 'A';
           					}
           					else{
           						typesArr = typesArr + '-A';
           					}
           					if(limitsArr==""){
           						limitsArr = limitsArr + archiveBytes + archiveChoice;
           					}         
           					else{
           						limitsArr = limitsArr + '-' + archiveBytes + archiveChoice;
           					}
           				}           				
           			}
           			else{
           				chars = true;
           			}           			
           		}           		
           	}
           	if(document.getElementById('RAM_DISK'+bytesIn)!=null){           		
           		ram_diskBytes = $("#RAM_DISK" + bytesIn).val();           		
           		if(!((ram_diskBytes == ""))){
           			// not empty
           			if(ram_diskBytes.match(/^\d+$/)){
           				if(ram_diskBytes>0){
           				    var ramdiskChoice="";
                            if(document.getElementById('RAM_DISK-mb').selected){
                                ramdiskChoice="m";
                            }
                            else if(document.getElementById('RAM_DISK-gb').selected){
                                ramdiskChoice="g";
                            }
                            else if(document.getElementById('RAM_DISK-kb').selected){
                                ramdiskChoice='k';
                            }
           					if(typesArr==""){
           					    typesArr = typesArr + 'RD';
           					}
           					else{
           						typesArr = typesArr + '-RD';
           					}
           					if(limitsArr==""){
           						limitsArr = limitsArr + ram_diskBytes + ramdiskChoice;
           					}         
           					else{
           						limitsArr = limitsArr + '-' + ram_diskBytes + ramdiskChoice;
           					}	
           				}           				
           			}
           			else{
           				chars = true;
           			}           			           			
           		}   
           	}
           	if(document.getElementById('DISK_H'+bytesIn)!=null){           		
				disk_hBytes = $("#DISK_H" + bytesIn).val();				
				if(!(disk_hBytes == "")){
					// not empty
					if(disk_hBytes.match(/^\d+$/)){
						if(disk_hBytes>0){
						    var diskhChoice="";
                            if(document.getElementById('DISK_H-mb').selected){
                                diskhChoice="m";
                            }
                            else if(document.getElementById('DISK_H-gb').selected){
                                diskhChoice="g";
                            }
                            else if(document.getElementById('DISK_H-kb').selected){
                                diskhChoice="k";
                            }
							if(typesArr==""){
           						typesArr = typesArr + 'DH';
           					}
           					else{
           						typesArr = typesArr + '-DH';
           					}
           					if(limitsArr==""){
           						limitsArr = limitsArr + disk_hBytes + diskChoice;
           					}         
           					else{
           						limitsArr = limitsArr + '-' + disk_hBytes + diskChoice;
           					}
						}						
					}
					else{
						chars = true;
					}           			
				}
           	}
           	if(document.getElementById('SSD_H'+bytesIn)!=null){           		
           		ssd_hBytes = $("#SSD_H" + bytesIn).val();           		
           		if(!(ssd_hBytes == "")){
           			// not empty
           			if(ssd_hBytes.match(/^\d+$/)){
           				if(ssd_hBytes>0){
           				    var ssdhChoice="";
                            if(document.getElementById('SSD_H-mb').selected){
                                ssdhChoice="m";
                            }
                            else if(document.getElementById('SSD_H-gb').selected){
                                ssdhChoice="g";
                            }
                            else if(document.getElementById('SSD_H-kb').selected){
                                ssdhChoice="k";
                            }
           					if(typesArr==""){
           						typesArr = typesArr + 'SSH';
           					}
           					else{
           						typesArr = typesArr + '-SSH';
           					}
           					if(limitsArr==""){
           						limitsArr = limitsArr + ssd_hBytes + ssdChoice;
           					}         
           					else{
           						limitsArr = limitsArr + '-' + ssd_hBytes + ssdChoice;
           					}	
           				}           				
           			}
           			else{
           				chars = true;
           			}			           			
           		}
           	}
           	if(document.getElementById('REMOTE'+bytesIn)!=null){           		
           		remoteBytes = $("#REMOTE" + bytesIn).val();           		
           		if(!(remoteBytes == "")){
           			// not empty
           			if(remoteBytes.match(/^\d+$/)){
           				if(remoteBytes>0){
           				    var remoteChoice="";
                            if(document.getElementById('REMOTE-mb').selected){
                                remoteChoice="m";
                            }
                            else if(document.getElementById('REMOTE-gb').selected){
                                remoteChoice="g";
                            }
                            else if(document.getElementById('REMOTE-kb').selected){
                                remoteChoice="k";
                            }
           					if(typesArr==""){
           						typesArr = typesArr + 'R';
           					}
           					else{
           						typesArr = typesArr + '-R';
           					}
           					if(limitsArr==""){
           						limitsArr = limitsArr + remoteBytes + remoteChoice;
           					}         
           					else{
           						limitsArr = limitsArr + '-' + remoteBytes + remoteChoice;
           					}
           				}           				
           			}
           			else{
           				chars = true;
           			}
           		}
           	}
           	if((poolName=="") || (poolName == null)){
        		var errorMsg = "<p>You must provide a name for the new Cache Pool</p>";
        		$("#error-name").empty().html(errorMsg);
        		document.getElementById("add-pool-name").focus();
        	}        	              	
        	else if((typesArr == "")||(limitsArr == "")){
        		var errorMsg = '<p>You must select at least one storage type and limit. Negative values and special characters are not allowed.</p>';
        		$("#error-field").html(errorMsg);
        	}
        	else if(chars == true){
        		var errorMsg = '<p>Values must not contain alphabetical characters or negative numbers</p>';
        		$("#error-field").html(errorMsg);
        	}              	   
        	else{        		
        		// do add cachepool
        		$("#error-field").empty();
        		$("#error-name").empty();
                var path_url = '/webhdfs/v1/' + encode_path(poolName) + '?op=MAKEPOOL&storage=' + typesArr+ '&limit=' + limitsArr + '&maxttl=' + maxttl;
           		$.ajax({
    				url: path_url,
    				type: 'PUT',
    				success: function(result) {
    					alert("Cache Pool is successfully added!");
    					var typesArr="";
           				var limitsArr=""; 
        				$( "#add-pool-popup" ).hide( "fast", function() {});	// hide popup
        				location.reload();	//refresh page
    				}
				}).error(network_error_handler(path_url, "add-pool"));	// call error handler
        	}
           	
        });
        // show modify pool popup
        $('table').on("click","#edit-pool-img", function(){
        	var $row = $(this).closest("tr");        			// Finds the closest row <tr> 
    		var	name = $(this).closest('tr').children('td:first').text();	// Gets first child td element of that row
    		var owner = $row.find("td:nth-child(2)").text(); 	// Gets nth child td element of that row
    		var group = $row.find("td:nth-child(3)").text();
    		var mode = $row.find("td:nth-child(4)").text();
    		var maxttl = $row.find("td:nth-child(5)").text();
    		var pool_name = $("#edit-pool-name");
    		pool_name.val(name);
    		var owner_text = $('#edit-pool-owner');
            owner_text.val(owner);  
            var group_text = $('#edit-pool-group');
            group_text.val(group);
            var mode_text = $('#edit-pool-mode');
            mode_text.val(mode);
            var ttl_text = $("#edit-pool-ttl");
            //convert time from miliseconds to seconds an then to hours since hours is our default value                          
            if(maxttl == "never"){
                $("#edit-pool-ttl-dropdown").val("h");
            }
            else{
                maxttl = maxttl.split(" ");      
                ttl_text.val(maxttl[0]);
                if(maxttl[1].includes("Day")){
                    $("#edit-pool-ttl-dropdown").val("d");
                }   
                else if(maxttl[1].includes("Hour")){
                    $("#edit-pool-ttl-dropdown").val("h");
                } 
                else if(maxttl[1].includes("Minute")){
                    $("#edit-pool-ttl-dropdown").val("m");
                }
                else{
                    $("#edit-pool-ttl-dropdown").val("s"); // set dropdown value to hours 
                }
            }
            	
					
            var mem = false, ram_disk = false, ssd_h = false, ssd = false, disk_h = false, disk = false, archive = false, remote = false;					
			$.getJSON( "/jmx?qry=Hadoop:service=NameNode,name=NameNodeInfo", function( data ) {
      			var new_data=workaroundCachePools(data.beans[0]);
      			$.each(new_data, function( key, val ) {
      				if(key =="CachePools"){
      					var i;
  						var size = ObjectLength(val);  					
						for(i=0;i<size;i++){
  							// for each cache pool
							$.each( val[i], function( key2, value2 ) {
								if((key2 == "name") && (value2==name)){
									var obj_pool;										
									// iterate through cache pool	
									for(obj_pool in val[i]){
										if(obj_pool.includes("type")){
											var temp = obj_pool;
											var position;
                                    		var matches = temp.match(/\[(.*?)\]/);
                                    		if (matches) {
                                    			position = matches[1];
                                    		}
                                    		var typePool = val[i]['type[' + position + ']'];
                                    		var limit = val[i]['limit[' + position + ']'];
                                    		limit = formatBytes(limit);
                                    		limit = limit.split(" ");                                    		                                    		
                                    		if(typePool == "MEMORY"){
                                    		    $("#limit-MEMORY").val('' + limit[0]);
                                    		    if(limit[1]=="MB"){
                                    		        document.getElementById('MEMORY-mb-mod').selected = true;
                                    		    }
                                    		    else if(limit[1]=="GB"){
                                    		        document.getElementById('MEMORY-gb-mod').selected = true;
                                    		    }
                                    		    else if(limit[1]=="KB"){
                                    		        document.getElementById('MEMORY-kb-mod').selected = true;
                                    		    }
                                                mem = true;
                                            }
                                            else if(typePool == "RAM_DISK"){
                                                $("#limit-RAM_DISK").val('' + limit[0]);
                                                if(limit[1]=="MB"){
                                                    document.getElementById('RAM_DISK-mb-mod').selected = true;
                                                }
                                                else if(limit[1]=="GB"){
                                                    document.getElementById('RAM_DISK-gb-mod').selected = true;
                                                }
                                                else if(limit[1]=="KB"){
                                                    document.getElementById('RAM_DISK-kb-mod').selected = true;
                                                }
                                                ram_disk = true;
                                            }
                                            else if(typePool =="SSD_H"){
                                                $("#limit-SSD_H").val('' + limit[0]);
                                                if(limit[1]=="MB"){
                                                    document.getElementById('SSD_H-mb-mod').selected = true;
                                                }
                                                else if(limit[1]=="GB"){
                                                    document.getElementById('SSD_H-gb-mod').selected = true;
                                                }
                                                else if(limit[1]=="KB"){
                                                    document.getElementById('SSD_H-kb-mod').selected = true;
                                                }
                                                ssd_h = true;
                                            }
                                            else if(typePool == "SSD"){
                                                $("#limit-SSD").val('' + limit[0]);
                                                if(limit[1]=="MB"){
                                                    document.getElementById('SSD-mb-mod').selected = true;
                                                }
                                                else if(limit[1]=="GB"){
                                                    document.getElementById('SSD-gb-mod').selected = true;
                                                }
                                                else if(limit[1]=="KB"){
                                                    document.getElementById('SSD-kb-mod').selected = true;
                                                }
                                                ssd = true;
                                            }
                                            else if(typePool == "DISK_H"){
                                                $("#limit-DISK_H").val('' + limit[0]);
                                                if(limit[1]=="MB"){
                                                    document.getElementById('DISK_H-mb-mod').selected = true;
                                                }
                                                else if(limit[1]=="GB"){
                                                    document.getElementById('DISK_H-gb-mod').selected = true;
                                                }
                                                else if(limit[1]=="KB"){
                                                    document.getElementById('DISK_H-kb-mod').selected = true;
                                                }
                                                disk_h = true;
                                            }
                                            else if(typePool == "DISK"){
                                                $("#limit-DISK").val('' + limit[0]);
                                                if(limit[1]=="MB"){
                                                    document.getElementById('DISK-mb-mod').selected = true;
                                                }
                                                else if(limit[1]=="GB"){
                                                    document.getElementById('DISK-gb-mod').selected = true;
                                                }
                                                else if(limit[1]=="KB"){
                                                    document.getElementById('DISK-kb-mod').selected = true;
                                                }
                                                disk = true;
                                            }
                                            else if(typePool == "ARCHIVE"){
                                                $("#limit-ARCHIVE").val('' + limit[0]);
                                                if(limit[1]=="MB"){
                                                    document.getElementById('ARCHIVE-mb-mod').selected = true;
                                                }
                                                else if(limit[1]=="GB"){
                                                    document.getElementById('ARCHIVE-gb-mod').selected = true;
                                                }
                                                else if(limit[1]=="KB"){
                                                    document.getElementById('ARCHIVE-kb-mod').selected = true;
                                                }
                                                archive = true;
                                            }
                                            else if(typePool == "REMOTE"){
                                                $("#limit-REMOTE").val('' + limit[0]);
                                                if(limit[1]=="MB"){
                                                    document.getElementById('REMOTE-mb-mod').selected = true;
                                                }
                                                else if(limit[1]=="GB"){
                                                    document.getElementById('REMOTE-gb-mod').selected = true;
                                                }
                                                else if(limit[1]=="KB"){
                                                    document.getElementById('REMOTE-kb-mod').selected = true;
                                                }
                                                remote = true;
                                            }
                                    		
                                    	}
                                    }
                                }
                             });
                         }
      				}      					
      			});
      			new_data=workaround(data.beans[0]);  				
  				$.each(new_data, function( key, val ) {
  					if (key=="LiveNodes"){
  						var size = ObjectLength(val);
                       	var i;                                
                       	for(i=0;i<size;i++){
                        	$.each( val[i], function( key2, value2 ) {
                            	if(key2.includes("storageType")){
                                	var temp = key2; 
                                	// get the index                                                               
                                	var matches = temp.match(/\[(.*?)\]/);
                                	if (matches) {
                                    	var position = matches[1];
                                	}
                                	var type_stor = val[i]['storageType[' + position + ']'];
                                	if(type_stor == "MEMORY"){
                                        mem = true;
                                    }
                                    else if(type_stor == "RAM_DISK"){
                                        ram_disk = true;
                                    }
                                    else if(type_stor =="SSD_H"){
                                        ssd_h = true;
                                    }
                                    else if(type_stor == "SSD"){
                                        ssd = true;
                                    }
                                    else if(type_stor == "DISK_H"){
                                        disk_h = true;
                                    }
                                    else if(type_stor == "DISK"){
                                        disk = true;
                                    }
                                    else if(type_stor == "ARCHIVE"){
                                        archive = true;
                                    }
                                    else if(type_stor == "REMOTE"){
                                        remote = true;
                                    }
                                	                                                                  
                           		}
                         	});
                        }
                        if(mem == false){
                            $("#memory-line-mod-pool").remove();                            
                        }
                        if(ram_disk == false){
                            $("#ram-disk-line-mod-pool").remove();
                        }
                        if(ssd_h == false){
                            $("#ssd-h-line-mod-pool").remove();
                        }
                        if(ssd == false){
                            $("#ssd-line-mod-pool").remove();
                        }
                        if(disk_h == false){
                            $("#disk-h-line-mod-pool").remove();
                        }
                        if(disk == false){
                            $("#disk-line-mod-pool").remove();
                        }
                        if(archive == false){
                            $("#archive-line-mod-pool").remove();
                        }
                        if(remote == false){
                            $("#remote-line-mod-pool").remove();
                        }
  					}  			
  				});
      			
      		});
			$( "#modify-pool-popup" ).toggle("fast");
        });	
        $(document).on('click', '#close-modify-pool', function(){   
            $("#limit-MEMORY").val('');
            $("#limit-RAM_DISK").val('');
            $("#limit-SSD_H").val('');
            $("#limit-SSD").val('');
            $("#limit-DISK_H").val('');
            $("#limit-DISK").val('');
            $("#limit-ARCHIVE").val('');
            $("#limit-REMOTE").val(''); 
            $("#edit-pool-ttl").val('');
            $("#edit-pool-ttl-dropdown").val("h");
            $(".mod-pool-bytes-dropdown").val("MB");                 
            $("#error-field-mod-pool").empty();
			$( "#modify-pool-popup" ).hide( "fast", function() {});
        }); 
        $("#ok-modify-pool").click(function(){
           	var typesArr="";
           	var limitsArr=""; 
			var poolName = $("#edit-pool-name").val();
           	var poolOwner = $("#edit-pool-owner").val();
           	var poolGroup = $("#edit-pool-group").val();
           	var poolMode = $("#edit-pool-mode").val();
           	var maxttl = $("#edit-pool-ttl").val();
           	var time = $("#edit-pool-ttl-dropdown").val();
           	var chars = false;
           	var diskBytes,memBytes,ssdBytes,ssd,archiveBytes,ram_diskBytes,
           		disk_hBytes,ssd_hBytes,remoteBytes;
                   
           	// check if maxttl is empty - implying never should be used
            if(!((maxttl == '')||(maxttl == null))){                
                if(!(maxttl == '0')){
                    maxttl = maxttl + time;
                }            
                else{
                    maxttl = '';
                }    
            }

           	if(document.getElementById('limit-DISK')!=null){  
           		diskBytes = $("#limit-DISK").val();
           		if(!((diskBytes == ""))){
           			// not empty
           			if(diskBytes.match(/^\d+$/)){
           				if(diskBytes > 0){
           				    var diskchoice = "";
           				    if(document.getElementById('DISK-mb-mod').selected){
           				        diskchoice = "m";
           				    }
           				    else if(document.getElementById('DISK-gb-mod').selected){
           				        diskchoice = "g";
           				    }
           				    else if(document.getElementById('DISK-kb_mod').selected){
           				        diskchoice = "k";
           				    }           				    
           					if(typesArr==""){
           						typesArr = typesArr + 'D';
           					}
           					else{
           						typesArr = typesArr + '-D';
           					}
           					if(limitsArr==""){
           						limitsArr = limitsArr + diskBytes + diskchoice;
           					}         
           					else{
           						limitsArr = limitsArr + '-' + diskBytes + diskchoice;
           					}
           				}           				
           			}
           			else{
           				chars = true;
           			}           			
           		}           		           		           		
           	}
           	if(document.getElementById('limit-MEMORY')!=null){           		
           		memBytes = $("#limit-MEMORY").val();
           		if(!((memBytes == ""))){
           			// not empty
           			if(memBytes.match(/^\d+$/)){
           				if(memBytes>0){
           				    var memchoice = "";
                            if(document.getElementById('MEMORY-mb-mod').selected){
                                memchoice = "m";
                            }
                            else if(document.getElementById('MEMORY-gb-mod').selected){
                                memchoice = "g";
                            }
                            else if(document.getElementById('MEMORY-kb_mod').selected){
                                memchoice = "k";
                            }
           					if(typesArr==""){
           						typesArr = typesArr + 'M';
           					}
           					else{
           						typesArr = typesArr + '-M';
           					}
           					if(limitsArr==""){
           						limitsArr = limitsArr + memBytes + memchoice;
           					}         
           					else{
           						limitsArr = limitsArr + '-' + memBytes + memchoice;
           					}
           				}           				
           			}
           			else{
           				chars = true;
           			}           			
           		}         			           	
           	}
           	if(document.getElementById('limit-SSD')!=null){           		           		
           		ssdBytes = $("#limit-SSD").val();
           		if(!((ssdBytes == ""))){
           			// not empty
           			if(ssdBytes.match(/^\d+$/)){
           				if(ssdBytes>0){
           				    var ssdchoice = "";
                            if(document.getElementById('SSD-mb-mod').selected){
                                ssdchoice = "m";
                            }
                            else if(document.getElementById('SSD-gb-mod').selected){
                                ssdchoice = "g";
                            }
                            else if(document.getElementById('SSD-kb-mod').selected){
                                ssdchoice = "k";
                            }
           					if(typesArr==""){
           						typesArr = typesArr + 'S';
           					}
           					else{
           						typesArr = typesArr + '-S';
           					}
           					if(limitsArr==""){
           						limitsArr = limitsArr + ssdBytes + ssdchoice;
           					}         
           					else{
           						limitsArr = limitsArr + '-' + ssdBytes + ssdchoice;
           					}
           				}           				
           			}
           			else{
           				chars = true;
           			}           			
           		}           					           		
           	}
           	if(document.getElementById('limit-ARCHIVE')!=null){           		
           		archiveBytes=$("#limit-ARCHIVE").val();
           		if(!((archiveBytes == ""))){
           			// not empty
           			if(archiveBytes.match(/^\d+$/)){
           				if(archiveBytes>0){
           				    var archivechoice = "";
                            if(document.getElementById('ARCHIVE-mb-mod').selected){
                                archivechoice = "m";
                            }
                            else if(document.getElementById('ARCHIVE-gb-mod').selected){
                                archivechoice = "g";
                            }
                            else if(document.getElementById('ARCHIVE-kb-mod').selected){
                                archivechoice = "k";
                            }
           					if(typesArr==""){
           						typesArr = typesArr + 'A';
           					}
           					else{
           						typesArr = typesArr + '-A';
           					}
           					if(limitsArr==""){
           						limitsArr = limitsArr + archiveBytes + archivechoice;
           					}         
           					else{
           						limitsArr = limitsArr + '-' + archiveBytes + archivechoice;
           					}
           				}           				
           			}
           			else{
           				chars = true;
           			}           			
           		}           		
           	}
           	if(document.getElementById('limit-RAM_DISK')!=null){           		
           		ram_diskBytes = $("#limit-RAM_DISK").val();
           		if(!((ram_diskBytes == ""))){
           			// not empty
           			if(ram_diskBytes.match(/^\d+$/)){
           				if(ram_diskBytes>0){
           				    var ramDiskchoice = "";
                            if(document.getElementById('RAM_DISK-mb-mod').selected){
                                ramDiskchoice = "m";
                            }
                            else if(document.getElementById('RAM_DISK-gb-mod').selected){
                                ramDiskchoice = "g";
                            }
                            else if(document.getElementById("RAM_DISK-kb-mod").selected){
                                ramDiskchoice = "k"
                            }
           					if(typesArr==""){
           						typesArr = typesArr + 'RD';
           					}
           					else{
           						typesArr = typesArr + '-RD';
           					}
           					if(limitsArr==""){
           						limitsArr = limitsArr + ram_diskBytes + ramDiskchoice;
           					}         
           					else{
           						limitsArr = limitsArr + '-' + ram_diskBytes + ramDiskchoice;
           					}	
           				}           				
           			}
           			else{
           				chars = true;
           			}
		 		}   
           	}
           	if(document.getElementById('limit-DISK_H')!=null){           		
				disk_hBytes = $("#limit-DISK_H").val();
				if(!(disk_hBytes == "")){
					// not empty
					if(disk_hBytes.match(/^\d+$/)){
						if(disk_hBytes>0){
						    var disk_hchoice = "";
                            if(document.getElementById('DISK_H-mb-mod').selected){
                                disk_hchoice = "m";
                            }
                            else if(document.getElementById('DISK_H-gb-mod').selected){
                                disk_hchoice = "g";
                            }
                            else if(document.getElementById('DISK_H-kb-mod').selected){
                                disk_hchoice = "k";
                            }
							if(typesArr==""){
           					typesArr = typesArr + 'DH';
           					}
           					else{
           						typesArr = typesArr + '-DH';
           					}
           					if(limitsArr==""){
           						limitsArr = limitsArr + disk_hBytes + disk_hchoice;
           					}         
           					else{
           						limitsArr = limitsArr + '-' + disk_hBytes + disk_hchoice;
           					}
						}						
					}
					else{
						chars = true;
					}
				}
           	}
           	if(document.getElementById('limit-SSD_H')!=null){           		
           		ssd_hBytes = $("#limit-SSD_H").val();
           		if(!(ssd_hBytes == "")){
           			// not empty
           			if(ssd_hBytes.match(/^\d+$/)){
           				if(ssd_hBytes>0){
           				    var ssd_hchoice = "";
                            if(document.getElementById('SSD_H-mb-mod').selected){
                                ssd_hchoice = "m";
                            }
                            else if(document.getElementById('SSD_H-gb-mod').selected){
                                ssd_hchoice = "g";
                            }
                            else if(document.getElementById('SSD_H-kb-mod').selected){
                                ssd_hchoice = "k";
                            }
           					if(typesArr==""){
           						typesArr = typesArr + 'SSH';
           					}
           					else{
           						typesArr = typesArr + '-SSH';
           					}
           					if(limitsArr==""){
           						limitsArr = limitsArr + ssd_hBytes + ssd_hchoice;
           					}         
           					else{
           						limitsArr = limitsArr + '-' + ssd_hBytes + ssd_hchoice;
           					}	
           				}           				
           			}
           			else{
           				chars = true;
           			}           			
           		}
           	}
           	if(document.getElementById('limit-REMOTE')!=null){           		
           		remoteBytes = $("#limit-REMOTE").val();
           		if(!(remoteBytes == "")){
           			// not empty
           			if(remoteBytes.match(/^\d+$/)){
           				if(remoteBytes>0){
           				    var remchoice = "";
                            if(document.getElementById('REMOTE-mb-mod').selected){
                                remchoice = "m";
                            }
                            else if(document.getElementById('REMOTE-gb-mod').selected){
                                remchoice = "g";
                            }
                            else if(document.getElementById('REMOTE-kb-mod').selected){
                                remchoice = "k";
                            }
           					if(typesArr==""){
           						typesArr = typesArr + 'R';
           					}
           					else{
           						typesArr = typesArr + '-R';
           					}
           					if(limitsArr==""){
           						limitsArr = limitsArr + remoteBytes + remchoice;
           					}         
           					else{
           						limitsArr = limitsArr + '-' + remoteBytes + remchoice;
           					}
           				}           				
           			}
           			else{
           				chars = true;
           			}
           		}
           	}         
        	if((typesArr == "")||(limitsArr == "")){
        		var errorMsg = '<p>You must select at least one storage type and limit. Negative values and special characters are not allowed.</p>';
        		$("#error-field-mod-pool").empty().html(errorMsg);
        	}
        	else if(chars == true){
        		var errorMsg ='<p>Values must not contain alphabetical characters or negative numbers</p>';
        		$("#error-field-mod-pool").empty().html(errorMsg);        		
        	}          	   	       
        	else{        		
        		// do modify pool
        		$("#error-field-mod-pool").empty();
                var path_url = '/webhdfs/v1/' + encode_path(poolName) + '?op=MODIFYPOOL&storage=' + typesArr+ '&limit=' + limitsArr + '&maxttl=' + maxttl;

           		$.ajax({
    				url: path_url,
    				type: 'PUT',
    				success: function(result) {
    					alert("Cache Pool is successfully modified!");
    					var typesArr="";
           				var limitsArr=""; 
        				$( "#modify-pool-popup" ).hide( "fast", function() {});	//hide popup
        				location.reload();	//refresh page
    				}
				}).error(network_error_handler(path_url, "edit-pool"));	// call error handler
				}
        });            
        $("table").on("click","#delete-pool-img", function(){
        	var $row = $(this).closest("tr");        			// Finds the closest row <tr> 
    		var	poolName = $(this).closest('tr').children('td:first').text();	// Gets first child td element of that row
        	if (confirm("Are you sure you want to delete " + poolName + " ?")){
        		var path_url = '/webhdfs/v1/' + encode_path(poolName) + '?op=DELETEPOOL';
           		$.ajax({
    				url: path_url,
    				type: 'PUT',
    				success: function(result) {
    					alert("Cache Pool is successfully deleted!");
        				$( "#add-pool-popup" ).hide( "fast", function() {});	// hide popup
        				location.reload();	//refresh page
    				}
				}).error(network_error_handler(path_url, "delete-pool"));	// call error handler 
        	}
        });
        // handle add directive
        $("#add-directive-button").click(function(){
        	$("#alert-panel-body-add-dir").empty();
        	$("#alert-panel-add-dir").hide();
        	var mem = false, ram_disk = false, ssd_h = false, ssd = false, disk_h = false, disk = false, archive = false, remote = false;
            $.getJSON( "/jmx?qry=Hadoop:service=NameNode,name=NameNodeInfo", function( data ) {                
                var new_data=workaround(data.beans[0]);                 
                $.each(new_data, function( key, val ) {
                    if (key=="LiveNodes"){
                        var size = ObjectLength(val);
                        var i;        
                        var count=0;                        
                        for(i=0;i<size;i++){
                            $.each( val[i], function( key2, value2 ) {
                                if(key2.includes("storageType")){
                                    count++;
                                    var temp = key2; 
                                    // get the index                                                               
                                    var matches = temp.match(/\[(.*?)\]/);
                                    if (matches) {
                                        var position = matches[1];
                                    }
                                    var type_stor = val[i]['storageType[' + position + ']'];
                                    if(type_stor == "MEMORY"){
                                        mem = true;
                                    }
                                    else if(type_stor == "RAM_DISK"){
                                        ram_disk = true;
                                    }
                                    else if(type_stor =="SSD_H"){
                                        ssd_h = true;
                                    }
                                    else if(type_stor == "SSD"){
                                        ssd = true;
                                    }
                                    else if(type_stor == "DISK_H"){
                                        disk_h = true;
                                    }
                                    else if(type_stor == "DISK"){
                                        disk = true;
                                    }
                                    else if(type_stor == "ARCHIVE"){
                                        archive = true;
                                    }
                                    else if(type_stor == "REMOTE"){
                                        remote = true;
                                    }
                                                                                                     
                                }
                            });
                        }
                        if(mem == false){
                            $("#memory-line-add-dir").remove();                            
                        }
                        if(ram_disk == false){
                            $("#ram-disk-line-add-dir").remove();
                        }
                        if(ssd_h == false){
                            $("#ssd-h-line-add-dir").remove();
                        }
                        if(ssd == false){
                            $("#ssd-line-add-dir").remove();
                        }
                        if(disk_h == false){
                            $("#disk-h-line-add-dir").remove();
                        }
                        if(disk == false){
                            $("#disk-line-add-dir").remove();
                        }
                        if(archive == false){
                            $("#archive-line-add-dir").remove();
                        }
                        if(remote == false){
                            $("#remote-line-add-dir").remove();
                        }
                    }           
                });
            });
        	$("#add-directive-popup").toggle("fast");
        	
        });
        $("#add-pool-directive-dropdown").change(function(){
        	$("#MEMORY-replication").val("");
        	$("#DISK-replication").val("");
        	$("#RAM_DISK-replication").val("");
        	$("#SSD_H-replication").val("");
        	$("#SSD-replication").val("");
        	$("#DISK_H-replication").val("");
        	$("#ARCHIVE-replication").val("");
        	$("#REMOTE-replication").val("");        	
        	var choice_pool = $("#add-pool-directive-dropdown").val();	// get selected pool
        	var mem = false, ram_disk = false, ssd_h = false, ssd = false, disk_h = false, disk = false, archive = false, remote = false;
        	$.getJSON( "/jmx?qry=Hadoop:service=NameNode,name=NameNodeInfo", function( data ) { 
        		var new_data=workaroundCachePools(data.beans[0]);
                $.each(new_data, function( key, val ) {
                    if (key=="CachePools"){
                        var i;
                        var size = ObjectLength(val);                   
                        for(i=0;i<size;i++){
                            // for each cache pool
                            $.each( val[i], function( key2, value2 ) {
                                // at any given key, we can use it first and then iterate through the general object of cache pool
                                // we choose name because it is what seperates the pools                    
                                if((key2 == "name") && (value2==choice_pool)){
                                    var obj_pool;                                       
                                    // iterate through cache pool   
                                    for(obj_pool in val[i]){
                                        if(obj_pool.includes("type")){
                                            // get the index of type which has schema of: type[..]
                                            var temp = obj_pool;
                                            var position;
                                            var matches = temp.match(/\[(.*?)\]/);
                                            if (matches) {
                                                position = matches[1];
                                            }
                                            // after type's index is found, get the other details with the same index so
                                            // we guarantee that they are for the same type
                                            var typePool = val[i]['type[' + position + ']'];
                                            if(typePool == "MEMORY"){
                                        		mem = true;
                                        		$("#memory-line-add-dir").show();
                                    		}
                                    		else if(typePool == "RAM_DISK"){
                                        		ram_disk = true;
                                        		$("#ram-disk-line-add-dir").show();
                                    		}
                                    		else if(typePool =="SSD_H"){
                                        		ssd_h = true;
                                        		$("#ssd-h-line-add-dir").show();
                                    		}
                                    		else if(typePool == "SSD"){
                                        		ssd = true;
                                        		$("#ssd-line-add-dir").show();
                                    		}
                                    		else if(typePool == "DISK_H"){
                                        		disk_h = true;
                                        		$("#disk-h-line-add-dir").show();
                                    		}
                                    		else if(typePool == "DISK"){
                                        		disk = true;
                                        		$("#disk-line-add-dir").show();
                                    		}
                                    		else if(typePool == "ARCHIVE"){
                                        		archive = true;
                                        		$("#archive-line-add-dir").show();
                                    		}
                                    		else if(typePool == "REMOTE"){
                                        		remote = true;
                                        		$("#remote-line-add-dir").show();
                                    		}
                                            
                                        }
                                    }   
                                   
                                }                           
                            });                         
                        }  
                        if(mem == false){
                        	$("#memory-line-add-dir").hide();
                        }
                        if(ram_disk == false){
                        	$("#ram-disk-line-add-dir").hide();
                        }
                        if(ssd_h == false){
                        	$("#ssd-h-line-add-dir").hide();
                        }
                        if(ssd == false){
                        	$("#ssd-line-add-dir").hide();
                        }
                        if(disk_h == false){
                        	$("#disk-h-line-add-dir").hide();
                        }
                        if(disk == false){
                        	$("#disk-line-add-dir").hide();
                        }
                        if(archive == false){
                        	$("#archive-line-add-dir").hide();
                        }
                        if(remote == false){
                        	$("#remote-line-add-dir").hide();
                        }                      
                    }               
                });
        	});
        });
        $(document).on('click', '#close-add-directive', function(){
        	$("#add-directive-path").val('');     
        	$("#MEMORY-replication").val('');
            $("#RAM_DISK-replication").val('');
            $("#SSD_H-replication").val('');
            $("#SSD-replication").val('');
            $("#DISK_H-replication").val('');
            $("#DISK-replication").val('');
            $("#ARCHIVE-replication").val('');
            $("#REMOTE-replication").val(''); 
            $("#error-field-add-directive").empty();
            $("#error-dir-path").empty();
            $("#add-directive-ttl").val('');
            $("#add-directive-ttl-dropdown").val("h");
			$( "#add-directive-popup" ).hide( "fast");
        });

        $("#ok-add-directive").click(function(){
           	var pool = $("#add-pool-directive-dropdown").val();
        	var dirPath = $("#add-directive-path").val();
        	var index = dirPath.indexOf("/");
			// find if user has entered / in the beginning
        	if(index == 0 ){
        		dirPath = dirPath.slice(1);
        	}
            var ttl = $("#add-directive-ttl").val();
            var time = $("#add-directive-ttl-dropdown").val();

            // check if ttl is empty - implying never should be used
            if(!((ttl=='')||(ttl==null))){                
                if(!(ttl == '0')){
                    ttl = ttl + time;
                }            
                else{
                    ttl = '';
                }    
            }        	
        	
        	var replArr="";
        	var diskRepl,memRepl,ssdRepl, archiveRepl, ram_diskRepl,disk_hRepl,ssd_hRepl,remoteRepl;
        	var chars = false;
        	
        	if(document.getElementById('replication-D')!=null){  
                diskRepl = $("#DISK-replication").val();
                if(!((diskRepl == ""))){
                    // not empty
                    if(diskRepl.match(/^\d+$/)){
                    	if(diskRepl > 0){
                        	if(replArr==""){
                            	replArr = replArr + 'D-' + diskRepl;
                        	}
                        	else{
                            	replArr = replArr + '_D-' + diskRepl;
                        	}                        
                    	}
                    }
                    else{
                    	chars = true;
                    }                    
                }                                                   
            }
            if(document.getElementById('replication-M')!=null){  
                memRepl = $("#MEMORY-replication").val();
                if(!((memRepl == ""))){
                    // not empty
                    if(memRepl.match(/^\d+$/)){
                    	if(memRepl > 0){
                        	if(replArr==""){
                            	replArr = replArr + 'M-' + memRepl;
                        	}
                        	else{
                            	replArr = replArr + '_M-' + memRepl;
                        	}                        
                    	}
                    }
                    else{
                    	chars = true;
                    }
                }                                                   
            }
            if(document.getElementById('replication-S')!=null){  
                ssdRepl = $("#SSD-replication").val();
                if(!((ssdRepl == ""))){
                    // not empty
                    if(ssdRepl.match(/^\d+$/)){
                    	if(ssdRepl > 0){
                        	if(replArr==""){
                            	replArr = replArr + 'S-' + ssdRepl;
                        	}
                        	else{
                            	replArr = replArr + '_S-' + ssdRepl;
                        	}                        
                    	}
                    }
                    else{
                    	chars = true;
                    }
                }                                                   
            }
            if(document.getElementById('replication-A')!=null){           		
           		archiveBytes=$("#ARCHIVE-replication").val();
           		if(!((archiveRepl == ""))){
           			// not empty
           			if(archiveRepl.match(/^\d+$/)){
           				if(archiveRepl > 0){
                        	if(replArr==""){
                            	replArr = replArr + 'A-' + archiveRepl;
                        	}
                        	else{
                            	replArr = replArr + '_A-' + archiveRepl;
                        	}                        
                    	}
           			}
           			else{
           				chars = true;
           			}
           		}           		
           	}
           	if(document.getElementById('replication-RD')!=null){           		
           		ram_diskRepl = $("#RAM_DISK-replication").val();
           		if(!((ram_diskRepl == ""))){
           			// not empty
           			if(ram_diskRepl.match(/^\d+$/)){
           				if(ram_diskRepl > 0){
                        	if(replArr==""){
                            	replArr = replArr + 'RD-' + ram_diskRepl;
                        	}
                        	else{
                            	replArr = replArr + '_RD-' + ram_diskRepl;
                        	}                        
                    	}
           			}
           			else{
           				chars = true;
           			}
           		}   
           	}
           	if(document.getElementById('replication-DH')!=null){           		
				disk_hRepl = $("#DISK_H-replication").val();
				if(!(disk_hRepl == "")){
					// not empty
					if(disk_hRepl.match(/^\d+$/)){
						if(disk_hRepl>0){
							if(replArr==""){
                            	replArr = replArr + 'DH-' + disk_hRepl;
                        	}
                        	else{
                            	replArr = replArr + '_DH-' + disk_hRepl;
                        	} 
						}
					}
					else{
						chars = true;
					}
				}
           	}
           	if(document.getElementById('replication-SH')!=null){           		
           		ssd_hRepl = $("#SSD_H-replication").val();
           		if(!(ssd_hRepl == "")){
           			// not empty
           			if(ssd_hRepl.match(/^\d+$/)){
           				if(ssd_hRepl>0){
           					if(replArr==""){
                            	replArr = replArr + 'SH-' + ssd_hRepl;
                        	}
                        	else{
                            	replArr = replArr + '_SH-' + ssd_hRepl;
                        	}
           				}
           			}
           			else{
           				chars = true;
           			}
           		}
           	}
           	if(document.getElementById('replication-R')!=null){           		
           		remoteRepl = $("#REMOTE-replication").val();
           		if(!(remoteRepl == "")){
           			// not empty
           			if(remoteRepl.match(/^\d+$/)){
           				if(remoteRepl>0){
           					if(replArr==""){
                            	replArr = replArr + 'R-' + remoteRepl;
                        	}
                        	else{
                            	replArr = replArr + '_R-' + remoteRepl;
                        	}
           				}
           			}
           			else{
           				chars = true;
           			}
           			
           		}
           	}
           	if(dirPath == ""){
           		var er = '<p>You must provide directive path</p>';
           		$("#error-dir-path").empty().html(er);
           		document.getElementById("add-directive-path").focus();
           	}
           	else if(replArr==""){
           		var errorMsg ='<p>Please select replication vector</p>';
                $("#error-field-add-directive").empty().html(errorMsg);
           	}
           	else if(chars == true){
           		var errorMsg ='<p>Values must not contain alphabetical characters or negative numbers</p>';
                $("#error-field-add-directive").empty().html(errorMsg);
           	}
           	else{
                   var path_url = '/webhdfs/v1/' + dirPath + '?op=MAKECACHEDIRECTIVE&pool=' + pool + '&vector=' + replArr + '&maxttl=' + ttl;
           		$.ajax({
    				url: path_url,
    				type: 'PUT',
    				success: function(result) {
        				alert("Successfully added cache directive!");
        				$( "#add-directive-popup" ).hide( "fast");	// hide popup
        				location.reload();	//refresh page
    				}
				}).error(network_error_handler(path_url, "add-dir"));
           	}
        });
        // handle edit directive
        $("table").on("click","#edit-directive-img", function(){
        	$("#alert-panel-body-mod-dir").empty();
        	$("#alert-panel-mod-dir").hide();
        	var $row = $(this).closest("tr");	// Finds the closest row <tr>
        	var dir_id = $(this).closest('tr').children('td:first').text();
        	var dir_path = $row.find("td:nth-child(2)").text();
        	var dir_pool = $row.find("td:nth-child(3)").text();
        	var ttl = $row.find("td:nth-child(4)").text();
        	var dir_repl_v = $row.find("td:nth-child(5)").text();
        	var mem = false, ram_disk = false, ssd_h = false, ssd = false, disk_h = false, disk = false, archive = false, remote = false;
    		$("#edit-pool-directive-dropdown").val(dir_pool);
    		var path_text = $('#edit-directive-path');
            path_text.val(dir_path);  
            var id_text = $('#edit-directive-id');
            id_text.val(dir_id);
        	var replicationArray = dir_repl_v.split(/[[\],]/);
        	var memRep,diskRep,ssdRep,archRep,rdRep,dhRep,shRep,rRep;
        	var mem2 = false, ram_disk2 = false, ssd_h2 = false, ssd2 = false, disk_h2 = false, disk2 = false, archive2 = false, remote2 = false;
        	// get numbers from existing replication vector
        	for(var i=0; i<replicationArray.length;i++){               
                if(replicationArray[i].includes("M")){
                   var tmp = replicationArray[i].split(/[=]/);
                   memRep = tmp[1];                   
                }
                if(replicationArray[i].includes("D")){
                   var tmp = replicationArray[i].split(/[=]/);
                   if(tmp[0] == "D"){
                       diskRep = tmp[1]; 
                   }
                   else if(tmp[0]=="RD"){
                       rdRep = tmp[1];
                   }
                   else if(tmp[0]=="DH"){
                       dhRep = temp[1];
                   }
                                     
                }
                if(replicationArray[i].includes("S")){
                   var tmp = replicationArray[i].split(/[=]/);
                   if(tmp[0]=="S"){
                       ssdRep = tmp[1];  
                   }
                   else if(tmp[0]=="SH"){
                       shRep = temp[1];
                   }
                                      
                }
                if(replicationArray[i].includes("A")){
                   var tmp = replicationArray[i].split(/[=]/);
                   archRep = tmp[1];                   
                }
                if(replicationArray[i].includes("R")){
                   var tmp = replicationArray[i].split(/[=]/);
                   if(tmp[0]=="R"){
                       rRep = temp[1];
                   }
                }
            }
        	
        	$.getJSON( "/jmx?qry=Hadoop:service=NameNode,name=NameNodeInfo", function( data ) {                
                var new_data=workaround(data.beans[0]);
                var cache_data=workaroundCachePools(data.beans[0]);                 
                $.each(new_data, function( key, val ) {
                    if (key=="LiveNodes"){
                        var size = ObjectLength(val);
                        var i;        
                        var count=0;                        
                        for(i=0;i<size;i++){
                            $.each( val[i], function( key2, value2 ) {
                                if(key2.includes("storageType")){
                                    count++;
                                    var temp = key2; 
                                    // get the index                                                               
                                    var matches = temp.match(/\[(.*?)\]/);
                                    if (matches) {
                                        var position = matches[1];
                                    }
                                    var type_stor = val[i]['storageType[' + position + ']'];
                                    if(type_stor == "MEMORY"){                                        
                                        mem = true;
                                        if(memRep !=undefined){
                                            $("#MEMORY-replication-mod").val('' + memRep);
                                        }
                                    }
                                    else if(type_stor == "RAM_DISK"){
                                        ram_disk = true;
                                        if(rdRep != undefined){
                                            $("#RAM_DISK-replication-mod").val('' + rdRep);
                                        }
                                    }
                                    else if(type_stor =="SSD_H"){
                                        ssd_h = true;
                                        if(shRep != undefined){
                                            $("#SSD_H-replication-mod").val('' + shRep);
                                        }
                                    }
                                    else if(type_stor == "SSD"){
                                        ssd = true;
                                        if(ssdRep != undefined){
                                            $("#SSD-replication-mod").val('' + ssdRep);
                                        }
                                    }
                                    else if(type_stor == "DISK_H"){
                                        disk_h = true;
                                        if(dhRep != undefined){
                                            $("#DISK_H-replication-mod").val('' + dhRep);
                                        }
                                    }
                                    else if(type_stor == "DISK"){
                                        disk = true;
                                        if(diskRep != undefined){
                                            $("#DISK-replication-mod").val('' + diskRep);
                                        }
                                    }
                                    else if(type_stor == "ARCHIVE"){
                                        archive = true;
                                        if(archRep != undefined){
                                            $("#ARCHIVE-replication-mod").val('' + archRep);
                                        }
                                    }
                                    else if(type_stor == "REMOTE"){
                                        remote = true;
                                        if(rRep != undefined){
                                            $("#REMOTE-replication-mod").val('' + rRep); 
                                        }
                                    }
									}
                            });
                        }
                        if(mem == false){
                            $("#memory-line-mod-dir").remove();                            
                        }
                        if(ram_disk == false){
                            $("#ram-disk-line-mod-dir").remove();
                        }
                        if(ssd_h == false){
                            $("#ssd-h-line-mod-dir").remove();
                        }
                        if(ssd == false){
                            $("#ssd-line-mod-dir").remove();
                        }
                        if(disk_h == false){
                            $("#disk-h-line-mod-dir").remove();
                        }
                        if(disk == false){
                            $("#disk-line-mod-dir").remove();
                        }
                        if(archive == false){
                            $("#archive-line-mod-dir").remove();
                        }
                        if(remote == false){
                            $("#remote-line-mod-dir").remove();
                        }
                    }           
                });
                $.each(cache_data, function( key, val ) {
                    if (key=="CachePools"){
                        var i;
                        var size = ObjectLength(val);                   
                        for(i=0;i<size;i++){
                            // for each cache pool
                            $.each( val[i], function( key2, value2 ) {
                                if((key2 == "name") && (value2== dir_pool)){
                                    var obj_pool;                                       
                                    // iterate through cache pool   
                                    for(obj_pool in val[i]){
                                        if(obj_pool.includes("type")){
                                            // get the index of type which has schema of: type[..]
                                            var temp = obj_pool;
                                            var position;
                                            var matches = temp.match(/\[(.*?)\]/);
                                            if (matches) {
                                                position = matches[1];
                                            }
                                            var typePool = val[i]['type[' + position + ']'];
                                            if(typePool == "MEMORY"){
                                        		mem2 = true;
                                        		$("#memory-line-mod-dir").show();
                                    		}
                                    		else if(typePool == "RAM_DISK"){
                                        		ram_disk2 = true;
                                        		$("#ram-disk-line-mod-dir").show();
                                    		}
                                    		else if(typePool =="SSD_H"){
                                        		ssd_h2 = true;
                                        		$("#ssd-h-line-mod-dir").show();
                                    		}
                                    		else if(typePool == "SSD"){
                                        		ssd2 = true;
                                        		$("#ssd-line-mod-dir").show();
                                    		}
                                    		else if(typePool == "DISK_H"){
                                        		disk_h2 = true;
                                        		$("#disk-h-line-mod-dir").show();
                                    		}
                                    		else if(typePool == "DISK"){
                                        		disk2 = true;
                                        		$("#disk-line-mod-dir").show();
                                    		}
                                    		else if(typePool == "ARCHIVE"){
                                        		archive2 = true;
                                        		$("#archive-line-mod-dir").show();
                                    		}
                                    		else if(typePool == "REMOTE"){
                                        		remote2 = true;
                                        		$("#remote-line-mod-dir").show();
                                    		}
                                            
                                        }
                                    }   
                                   
                                }                           
                            });                         
                        }  
                        if(mem2 == false){
                        	$("#memory-line-mod-dir").hide();
                        }
                        if(ram_disk2 == false){
                        	$("#ram-disk-line-mod-dir").hide();
                        }
                        if(ssd_h2 == false){
                        	$("#ssd-h-line-mod-dir").hide();
                        }
                        if(ssd2 == false){
                        	$("#ssd-line-mod-dir").hide();
                        }
                        if(disk_h2 == false){
                        	$("#disk-h-line-mod-dir").hide();
                        }
                        if(disk2 == false){
                        	$("#disk-line-mod-dir").hide();
                        }
                        if(archive2 == false){
                        	$("#archive-line-mod-dir").hide();
                        }
                        if(remote2 == false){
                        	$("#remote-line-mod-dir").hide();
                        }                      
                    }               
                });
                
            });                                               
        	$("#edit-directive-popup").toggle("fast");    		
        });
        $("#edit-pool-directive-dropdown").change(function(){
        	$("#MEMORY-replication-mod").val('');
            $("#RAM_DISK-replication-mod").val('');
            $("#SSD_H-replication-mod").val('');
            $("#SSD-replication-mod").val('');
            $("#DISK_H-replication-mod").val('');
            $("#DISK-replication-mod").val('');
            $("#ARCHIVE-replication-mod").val('');
            $("#REMOTE-replication-mod").val('');              
        	var choice_pool = $("#edit-pool-directive-dropdown").val();	// get selected pool
        	var mem = false, ram_disk = false, ssd_h = false, ssd = false, disk_h = false, disk = false, archive = false, remote = false;
        	$.getJSON( "/jmx?qry=Hadoop:service=NameNode,name=NameNodeInfo", function( data ) { 
        		var new_data=workaroundCachePools(data.beans[0]);
                $.each(new_data, function( key, val ) {
                    if (key=="CachePools"){
                        var i;
                        var size = ObjectLength(val);                   
                        for(i=0;i<size;i++){
                            // for each cache pool
                            $.each( val[i], function( key2, value2 ) {
                                // at any given key, we can use it first and then iterate through the general object of cache pool
                                // we choose name because it is what seperates the pools                    
                                if((key2 == "name") && (value2==choice_pool)){
                                    var obj_pool;                                       
                                    // iterate through cache pool   
                                    for(obj_pool in val[i]){
                                        if(obj_pool.includes("type")){
                                            // get the index of type which has schema of: type[..]
                                            var temp = obj_pool;
                                            var position;
                                            var matches = temp.match(/\[(.*?)\]/);
                                            if (matches) {
                                                position = matches[1];
                                            }
                                            // after type's index is found, get the other details with the same index so
                                            // we guarantee that they are for the same type
                                            var typePool = val[i]['type[' + position + ']'];
                                            if(typePool == "MEMORY"){
                                        		mem = true;
                                        		$("#memory-line-mod-dir").show();
                                    		}
                                    		else if(typePool == "RAM_DISK"){
                                        		ram_disk = true;
                                        		$("#ram-disk-line-mod-dir").show();
                                    		}
                                    		else if(typePool =="SSD_H"){
                                        		ssd_h = true;
                                        		$("#ssd-h-line-mod-dir").show();
                                    		}
                                    		else if(typePool == "SSD"){
                                        		ssd = true;
                                        		$("#ssd-line-mod-dir").show();
                                    		}
                                    		else if(typePool == "DISK_H"){
                                        		disk_h = true;
                                        		$("#disk-h-line-mod-dir").show();
                                    		}
                                    		else if(typePool == "DISK"){
                                        		disk = true;
                                        		$("#disk-line-mod-dir").show();
                                    		}
                                    		else if(typePool == "ARCHIVE"){
                                        		archive = true;
                                        		$("#archive-line-mod-dir").show();
                                    		}
                                    		else if(typePool == "REMOTE"){
                                        		remote = true;
                                        		$("#remote-line-mod-dir").show();
                                    		}
                                            
                                        }
                                    }   
                                   
                                }                           
                            });                         
                        }  
                        if(mem == false){
                        	$("#memory-line-mod-dir").hide();
                        }
                        if(ram_disk == false){
                        	$("#ram-disk-line-mod-dir").hide();
                        }
                        if(ssd_h == false){
                        	$("#ssd-h-line-mod-dir").hide();
                        }
                        if(ssd == false){
                        	$("#ssd-line-mod-dir").hide();
                        }
                        if(disk_h == false){
                        	$("#disk-h-line-mod-dir").hide();
                        }
                        if(disk == false){
                        	$("#disk-line-mod-dir").hide();
                        }
                        if(archive == false){
                        	$("#archive-line-mod-dir").hide();
                        }
                        if(remote == false){
                        	$("#remote-line-mod-dir").hide();
                        }                      
                    }               
                });
        	});
        });
        $(document).on('click', '#close-edit-directive', function(){  
            $("#MEMORY-replication-mod").val('');
            $("#RAM_DISK-replication-mod").val('');
            $("#SSD_H-replication-mod").val('');
            $("#SSD-replication-mod").val('');
            $("#DISK_H-replication-mod").val('');
            $("#DISK-replication-mod").val('');
            $("#ARCHIVE-replication-mod").val('');
            $("#REMOTE-replication-mod").val('');    
            $("#error-mod-rep-vec").empty();     	     
            $("#mod-directive-ttl").val('');
            $("#mod-directive-ttl-dropdown").val('h');   
			$( "#edit-directive-popup" ).hide( "fast", function() {});
        });
        $("#ok-edit-directive").click(function(){
            var id = $("#edit-directive-id").val();
            var pool = $("#edit-pool-directive-dropdown").val();
            var dirPath = $("#edit-directive-path").val();
            var ttl = $("#mod-directive-ttl").val();
            var time = $("#mod-directive-ttl-dropdown").val();
            var replArr="";
            var diskRepl,memRepl,ssdRepl, archiveRepl, ram_diskRepl,disk_hRepl,ssd_hRepl,remoteRepl;
            var chars = false;
            var index = dirPath.indexOf("/"); // find if user has entered / in the beginning
        	if(index == 0 ){
        		// if exists / in the beginning, remove it because it causes problems in the request
        		dirPath = dirPath.slice(1);
            }
            
            // check if ttl is empty - implying never should be used
            if(!((ttl=='')||(ttl==null))){                
                if(!(ttl == '0')){
                    ttl = ttl + time;
                }                            
            }      

            if(document.getElementById('mod-replication-D')!=null){  
                diskRepl = $("#DISK-replication-mod").val();
                if(!((diskRepl == ""))){
                    // not empty
                    if(diskRepl.match(/^\d+$/)){
                    	if(diskRepl > 0){
                        	if(replArr==""){
                            	replArr = replArr + 'D-' + diskRepl;
                        	}
                        	else{
                            	replArr = replArr + '_D-' + diskRepl;
                        	}                        
                    	}                    	
                    }
                    else{
                    	chars = true;
                    }                    
                }                                                   
            }
            if(document.getElementById('mod-replication-M')!=null){  
                memRepl = $("#MEMORY-replication-mod").val();
                if(!((memRepl == ""))){
                    // not empty
                    if(memRepl.match(/^\d+$/)){
                    	if(memRepl > 0){
                        	if(replArr==""){
                            	replArr = replArr + 'M-' + memRepl;
                        	}
                        	else{
                            	replArr = replArr + '_M-' + memRepl;
                        	}                        
                    	}
                    }
                    else{
                    	chars = true;
                    }
                }                                                   
            }
            if(document.getElementById('mod-replication-S')!=null){  
                ssdRepl = $("#SSD-replication-mod").val();
                if(!((ssdRepl == ""))){
                    // not empty
                    if(ssdRepl.match(/^\d+$/)){
                    	if(ssdRepl > 0){
                        	if(replArr==""){
                            	replArr = replArr + 'S-' + ssdRepl;
                        	}
                        	else{
                            	replArr = replArr + '_S-' + ssdRepl;
                        	}                        
                    	}                    	
                    }
                    else{
                    	chars = true;
                    }                    
                }                                                   
            }
            if(document.getElementById('mod-replication-A')!=null){                 
                archiveBytes=$("#ARCHIVE-replication-mod").val();
                if(!((archiveRepl == ""))){
                    // not empty
                    if(archiveRepl.match(/^\d+$/)){
                    	if(archiveRepl > 0){
                        	if(replArr==""){
                            	replArr = replArr + 'A-' + archiveRepl;
                        	}
                        	else{
                            	replArr = replArr + '_A-' + archiveRepl;
                        	}                        
                    	}
                    }
                    else{
                    	chars = true;
                    }                    
                }                   
            }
            if(document.getElementById('mod-replication-RD')!=null){                
                ram_diskRepl = $("#RAM_DISK-replication-mod").val();
                if(!((ram_diskRepl == ""))){
                    // not empty
                    if(ram_diskRepl.match(/^\d+$/)){
                    	if(ram_diskRepl > 0){
                        	if(replArr==""){
                            	replArr = replArr + 'RD-' + ram_diskRepl;
                        	}
                        	else{
                            	replArr = replArr + '_RD-' + ram_diskRepl;
                        	}                        
                   		}
                    }
                    else{
                    	chars = true;
                    }
                }   
            }
            if(document.getElementById('mod-replication-DH')!=null){                
                disk_hRepl = $("#DISK_H-replication-mod").val();
                if(!(disk_hRepl == "")){
                    // not empty
                    if(disk_hRepl.match(/^\d+$/)){
                    	if(disk_hRepl>0){
                        	if(replArr==""){
                            	replArr = replArr + 'DH-' + disk_hRepl;
                        	}
                        	else{
                            	replArr = replArr + '_DH-' + disk_hRepl;
                        	}	
                    	}
                    }
                    else{
                    	chars = true;
                    }                    
                }
            }
            if(document.getElementById('mod-replication-SH')!=null){                
                ssd_hRepl = $("#SSD_H-replication-mod").val();
                if(!(ssd_hRepl == "")){
                    // not empty
                    if(ssd_hRepl.match(/^\d+$/)){
                    	if(ssd_hRepl>0){
                        	if(replArr==""){
                            	replArr = replArr + 'SH-' + ssd_hRepl;
                        	}
                        	else{
                           		replArr = replArr + '_SH-' + ssd_hRepl;
                        	}
                    	}
                    }
                    else{
                    	chars = true;
                    }                    
                }
            }if(document.getElementById('mod-replication-R')!=null){                
                remoteRepl = $("#REMOTE-replication-mod").val();
                if(!(remoteRepl == "")){
                    // not empty
                    if(remoteRepl.match(/^\d+$/)){
                    	if(remoteRepl>0){
                        	if(replArr==""){
                            	replArr = replArr + 'R-' + remoteRepl;
                        	}
                        	else{
                            	replArr = replArr + '_R-' + remoteRepl;
                        	}
                    	}
                    }
                    else{
                    	chars = true;
                    }
                }
            }
            if(dirPath == ""){
                var er = '<p>You must provide directive path</p>';
                $("#error-mod-dir-path").empty().html(er);
                document.getElementById("edit-directive-path").focus();
            }           
            else if(chars == true){
            	var er = '<p>Values must not contain alphabetical characters or negative numbers</p>';
                $("#error-mod-rep-vec").empty().html(er);
            }
            else if(replArr == ""){
                var er = '<p>No valid replication vector</p>';
                $("#error-mod-rep-vec").empty().html(er);
            }
            else{
                var path_url = '/webhdfs/v1/' + encode_path(dirPath) + '?op=MODIFYDIRECTIVE&id=' + id + '&pool=' + pool + '&vector=' + replArr + '&maxttl=' + ttl; 

                $.ajax({
                    url: path_url,
                    type: 'PUT',
                    success: function(result) {
                        alert("Successfully modified cache directive!");
                        $( "#edit-directive-popup" ).hide( "fast");		// hide popup
                        location.reload();	// reload page
                    }
                }).error(network_error_handler(path_url, "edit-dir"));	// call error handler
            }
            
                
        });
        $("table").on("click","#delete-directive-img", function(){
        	var $row = $(this).closest("tr");        			// Finds the closest row <tr> 
    		var	dirId = $(this).closest('tr').children('td:first').text();	// Gets first child td element of that row
    		var dir_path = $row.find("td:nth-child(2)").text();
    		var index = dir_path.indexOf("/");		// find if user has entered / in the beginning
        	if(index == 0 ){
        		// remove / from beginning of path
        		dir_path = dir_path.slice(1);
        	}
        	dir_path = dir_path.replace("/", "");
    		
        	if (confirm("Are you sure you want to delete cache directive " + dir_path + " with ID = " + dirId + " ?")){
        		var path_url = '/webhdfs/v1/' + encode_path(dir_path) + '?op=DELETEDIRECTIVE&id=' + dirId;
           		$.ajax({
    				url: path_url,
    				type: 'PUT',
    				success: function(result) {
    					alert("Cache Directive is successfully deleted!");	// hide popup
    					location.reload();	// reload page
    				}
				}).error(network_error_handler(path_url, "delete-directive"));	// call error handler
        	}
        });
        $(document).ready(function(){
    		$('[data-toggle="tooltip"]').tooltip();	// tooltip to show when hovering icons
		});
		function encode_path(abs_path) {
    		abs_path = encodeURIComponent(abs_path);
    		var re = /%2F/g;
    		return abs_path.replace(re, '/');
  		}
  		function network_error_handler(url, op) {
  			// function to handle error messages
            return function (jqxhr, text, err) {
                switch(jqxhr.status) {
                case 401:
                    var msg = '<p>Authentication failed when trying to open ' + url + ': Unauthorized.</p>';
                    break;
                case 403:
                    if(jqxhr.responseJSON !== undefined && jqxhr.responseJSON.RemoteException !== undefined) {
                        var msg = '<p>' + jqxhr.responseJSON.RemoteException.message + "</p>";
                        break;
                    }
                    var msg = '<p>Permission denied when trying to open ' + url + ': ' + err + '</p>';
                    break;
                case 404:
                    var msg = '<p>Path does not exist on HDFS or WebHDFS is disabled.  Please check your path or enable WebHDFS</p>';
                    break;
                case 400:
                    var msg = '<p>' + jqxhr.responseJSON.message + '</p><br>' + '<p>' + err + '</p>';               
                default:
                    var msg = '<p>Failed to retrieve data from ' + url + ': ' + err + '</p>';
                }
            //show_err_msg(msg);
                switch(op){
                    case "add-pool":
                    {
                        $("#alert-panel-body-add-pool").html(msg);
                        $("#alert-panel-add-pool").show();  
                        break;
                    }
                    case "edit-pool":
                    {
                        $("#alert-panel-body-mod-pool").html(msg);
                        $("#alert-panel-mod-pool").show();  
                        break;
                    }
                    case "add-dir":
                    {
                        $("#alert-panel-body-add-dir").html(msg);
                        $("#alert-panel-add-dir").show(); 
                        break;
                    }
                    case "edit-dir":
                    {
                        $("#alert-panel-body-mod-dir").html(msg);
                        $("#alert-panel-mod-dir").show();
                        break;
                    }
                    case "delete-pool":
                    {
                    	$("#alert-panel-cache-body").html(msg);
                    	$("#alert-panel-cache").show();
                    	break;
                    }
                    case "delete-directive":
                    {
                    	$("#alert-panel-cache-body").html(msg);
                    	$("#alert-panel-cache").show();
                    	break;
                    }
                
                }
           
            };
        }  		
      	function workaroundCachePools(r) {
      		// workaround to convert json to array for cache pools
			function node_map_to_array(pools) {
		        var res = [];
		        for (var n in pools) {
		          var p = pools[n];
		          p.name = n;
		          res.push(p);
		        }
		        return res;
		      }		  
		    r.CachePools = node_map_to_array(JSON.parse(r.CachePools));
			return r;
		};
		function workaroundCacheDirectives(r) {
			// workaround to convert json to array for cache directives
			function node_map_to_array(directives) {
		        var res = [];
		        for (var n in directives) {
		          var p = directives[n];
		          p.id = n;
		          res.push(p);
		        }
		        return res;
		      }		  
		    r.CacheDirectives = node_map_to_array(JSON.parse(r.CacheDirectives));
			return r;
		};						
		function populateAll(){
			// function to populate tables for cache pools and cache directives in case "All" is selected
			$.getJSON( "/jmx?qry=Hadoop:service=NameNode,name=NameNodeInfo", function( data ) {
      			var new_data=workaroundCachePools(data.beans[0]);
      			var directives_data=workaroundCacheDirectives(data.beans[0]);
				$.each(new_data, function( key, val ) {
  				if (key=="CachePools"){
  					var i;
  					var size = ObjectLength(val);  					
					for(i=0;i<size;i++){
						var array_types = [];
  						// for each cache pool
						$.each( val[i], function( key2, value2 ) {
							// at any given key, we can use it first and then iterate through the general object of cache pool
							// we choose name because it is what seperates the pools					
							if(key2 == "name"){
								var obj_pool;										
								// iterate through cache pool	
								for(obj_pool in val[i]){
									// we need something in order to stop the loop -> by checking if obj_pool includes type guarantees us that
									// only if type is found, the next commands will be executed
									if(obj_pool.includes("type")){
										// get the index of type which has schema of: type[..]
										var temp = obj_pool;
										var position;
                                    	var matches = temp.match(/\[(.*?)\]/);
                                    	if (matches) {
                                    		position = matches[1];
                                    	}
                                    	// after type's index is found, get the other details with the same index so
                                    	// we guarantee that they are for the same type
                                    	var typePool = val[i]['type[' + position + ']'];
                                    	var limit = val[i]['limit[' + position + ']'];
                                    	var bytesNeededPool = val[i]['bytesNeeded[' + position + ']'];
                                    	var bytesCachedPool = val[i]['bytesCached[' + position + ']'];
                                    	var bytesOverLimit = val[i]['bytesOverlimit[' + position + ']'];
                                    	var poolName = val[i]['name'];
                                    	var poolOwner = val[i]['owner'];
                                    	var group = val[i]['group'];
                                    	var mode = val[i]['mode'];
                                    	var maxTtl = val[i]['maxTTL'];
                                    	maxTtl = formatTtl(maxTtl); 
                                    	
                                    	//push to table
                                    	array_types.push(typePool + " " + limit + " " + bytesNeededPool + " " + bytesCachedPool + " " + bytesOverLimit);
									}
								}	
								// create the schema and append it to table							
								var poolInfo ='<tr class="clear-pool"><td>' + poolName + '</td><td>' + poolOwner + '</td><td>' + group + '</td><td>' + mode + '</td><td>' + maxTtl + '</td><td><table class="types-pool-table" id="type-' + poolName + '"></table></td><td><table class="limits-pool-table" id="limit-' + poolName + '"></table></td><td align="center"><table id="bn-pool-table-' + poolName + '"></table></td><td align="center"><table id="bc-pool-table-' + poolName + '"></table></td><td align="center"><table id="bo-pool-table-' + poolName + '"></table></td><td><div class="tools-cont"><img data-toggle="tooltip" title="Edit Pool" alt="Edit Pool" class="tools" id="edit-pool-img" src="images/pencil.png"/><img data-toggle="tooltip" title="Delete Pool" alt="Delete Pool" class="tools" id="delete-pool-img" src="images/cancel-button.png" /></div</td></tr>';
                                $(poolInfo).appendTo("#cache-pools-table");
                                for(var x=0;x<array_types.length;x++){
                                    var tempArr = array_types[x].split(/ /);                                    
                                    	var typesInfo = '<tr><td class="bottom-line" id="'+ tempArr[0] +'">' + tempArr[0] + '</td></tr>';                                    	
                                    	var limitInfo = '<tr><td class="bottom-line" id="' + tempArr[0] +'">' + formatBytes(tempArr[1]) + '</td></tr>';
                                    	if(tempArr[2]==0){
                                    		var bnInfo = '<tr><td align="center" class="bottom-line">-</td></tr>';
                                    	}
                                    	else{
                                    		var bnInfo = '<tr><td align="center" class="bottom-line">' + tempArr[2] + '</td></tr>';
                                    	}
                                    	if(tempArr[3]==0){
                                    		var bcInfo = '<tr><td align="center" class="bottom-line">-</td></tr>';
                                    	}
                                    	else{
                                    		var bcInfo = '<tr><td align="center" class="bottom-line">' + tempArr[3] + '</td></tr>';
                                    	}
                                    	if(tempArr[4]==0){
                                    		var boInfo = '<tr><td align="center" class="bottom-line">-</td></tr>';
                                    	}
                                    	else{
                                    		var boInfo = '<tr><td align="center" class="bottom-line">' + tempArr[4] + '</td></tr>';
                                    	}
                                    	$(typesInfo).appendTo("#type-" + poolName);
                                    	$(limitInfo).appendTo("#limit-" + poolName);
                                    	$(bnInfo).appendTo("#bn-pool-table-" + poolName);
                                    	$(bcInfo).appendTo("#bc-pool-table-" + poolName);
                                    	$(boInfo).appendTo("#bo-pool-table-" + poolName);
                                }
							}							
  						});  						
  					}
  				}  				
  			});
  			$.each(directives_data, function( key, val ) {
  				if(key=="CacheDirectives"){
  					var i;
  					var size = ObjectLength(val);
  					directive_ID = size;
  					var id; var pool; var replVector; var expiry; var path; var stats; var new_stats;
  					var bytesNeeded; var bytesCached; var filesNeeded; var filesCached; var type;
  					for(i=0;i<size;i++){
  						// for each cache directive
  						var array_types = [];
  						$.each( val[i], function( key2, value2 ) {  							
  							if(key2=="id"){
  								var obj_pool;
  								for(obj_pool in val[i]){
  									if(obj_pool.includes("type")){
  										var temp = obj_pool;
										var position;
                                    	var matches = temp.match(/\[(.*?)\]/);
                                    	if (matches) {
                                    		position = matches[1];
                                    	}
                                    	var typeDir = val[i]['type[' + position + ']'];
                                    	var bytesNeededDir = val[i]['bytesNeeded[' + position + ']'];
                                    	var bytesCachedDir = val[i]['bytesCached[' + position + ']'];
                                    	var filesNeeded = val[i]['filesNeeded[' + position + ']'];
                                    	var filesCached = val[i]['filesCached[' + position + ']'];
                                    	id = val[i]['id'];
                                    	pool = val[i]['pool'];
                                    	replVector = val[i]['replicationVector'];
                                    	expiry = val[i]['expiry'];
                                    	path = val[i]['path'];
                                    	
                                    	if(replVector.includes("MEM")){
  						    				replVector = replVector.replace("MEMORY", "M");
  										}
  										if(replVector.includes("DISK")){
  						    				replVector = replVector.replace("DISK", "D");
  										}
  										if(replVector.includes("SSD")){
  						    				replVector = replVector.replace("SSD", "S");
  										}
                                    	
                                    	array_types.push(typeDir + " " + bytesNeededDir + " " + bytesCachedDir + " " + filesNeeded + " " + filesCached);
  									}
  								}
                                  
                                // format expiry if not == never
                                if(!(expiry == 'never')){
                                    var expiry_table = expiry.split('T');                                
                                    var exp_temp_time = expiry_table[1].split('+');
                                    var exp_date = expiry_table[0];
                                    var exp_time = exp_temp_time[0];
                                    // set new format
                                    expiry = exp_date + ' ' + exp_time;
                                }                                

  								var dirInfo ='<tr class="clear-directive"><td>' + id + '</td><td>' + path + '</td><td>' + pool + '</td><td>' + expiry + '</td><td>' + replVector + '</td><td><table class="types-pool-table" id="type-' + id + '"></table></td><td align="center"><table id="bn-dir-table-' + id + '"></table></td><td align="center"><table id="bc-dir-table-' + id + '"></table></td><td align="center"><table id="fn-dir-table-' + id + '"></table></td><td align="center"><table id="fc-dir-table-' + id + '"></table></td><td><div class="tools-cont"><img data-toggle="tooltip" title="Edit Directive" alt="Edit Directive" class="tools" id="edit-directive-img" src="images/pencil.png"/><img data-toggle="tooltip" title="Delete Directive" alt="Delete Directive" class="tools" id="delete-directive-img" src="images/cancel-button.png" /></div></td></tr>';
  								$(dirInfo).appendTo("#cache-directives-table");
  								for(var x=0;x<array_types.length;x++){
                                    var tempArr = array_types[x].split(/ /);                                    
                                    var typesInfo = '<tr><td class="bottom-line" id="dir-type-'+ tempArr[0] +'">' + tempArr[0] + '</td></tr>';
                                    if(tempArr[1] == 0){
                                    		var bnInfo = '<tr><td align="center" class="bottom-line">-</td></tr>';
                                    	}
                                    	else{
                                    		var bnInfo = '<tr><td align="center" class="bottom-line">' + tempArr[1] + '</td></tr>';
                                    	}
                                    	if(tempArr[2] == 0){
                                    		var bcInfo = '<tr><td align="center" class="bottom-line">-</td></tr>';
                                    	}
                                    	else{
                                    		var bcInfo = '<tr><td align="center" class="bottom-line">' + tempArr[2] + '</td></tr>';
                                    	}
                                    	if(tempArr[3] == 0){
                                    		var fnInfo = '<tr><td align="center" class="bottom-line">-</td></tr>';
                                    	}
                                    	else{
                                    		var fnInfo = '<tr><td align="center" class="bottom-line">' + tempArr[3] + '</td></tr>';
                                    	}
                                    	if(tempArr[4] == 0){
                                    		var fcInfo = '<tr><td align="center" class="bottom-line">-</td></tr>';
                                    	}
                                    	else{
                                    		var fcInfo = '<tr><td align="center" class="bottom-line">' + tempArr[4] + '</td></tr>';
                                    	}                                                                    
                                    $(typesInfo).appendTo("#type-" + id);                                    
                                    $(bnInfo).appendTo("#bn-dir-table-" + id);
                                    $(bcInfo).appendTo("#bc-dir-table-" + id);
                                    $(fnInfo).appendTo("#fn-dir-table-" + id);
                                    $(fcInfo).appendTo("#fc-dir-table-" + id);
                                }
  							}
  						});
  					}
  				}
  			});
  			});
		};
		function populateSpecific(selected_pool){
			// function to populate tables for cache pools and cache directives for specific pool that is selected
		    $.getJSON( "/jmx?qry=Hadoop:service=NameNode,name=NameNodeInfo", function( data ) {
		        var new_data=workaroundCachePools(data.beans[0]);
                var directives_data=workaroundCacheDirectives(data.beans[0]);
                $.each(new_data, function( key, val ) {
                    if (key=="CachePools"){
                        var i;
                        var size = ObjectLength(val);                   
                        for(i=0;i<size;i++){
                            var array_types = [];
                            // for each cache pool
                            $.each( val[i], function( key2, value2 ) {
                                // at any given key, we can use it first and then iterate through the general object of cache pool
                                // we choose name because it is what seperates the pools                    
                                if((key2 == "name") && (value2==selected_pool)){
                                    var obj_pool;                                       
                                    // iterate through cache pool   
                                    for(obj_pool in val[i]){
                                        // we need something in order to stop the loop -> by checking if obj_pool includes type guarantees us that
                                        // only if type is found, the next commands will be executed
                                        if(obj_pool.includes("type")){
                                            // get the index of type which has schema of: type[..]
                                            var temp = obj_pool;
                                            var position;
                                            var matches = temp.match(/\[(.*?)\]/);
                                            if (matches) {
                                                position = matches[1];
                                            }
                                            // after type's index is found, get the other details with the same index so
                                            // we guarantee that they are for the same type
                                            var typePool = val[i]['type[' + position + ']'];
                                            var limit = val[i]['limit[' + position + ']'];
                                            var bytesNeededPool = val[i]['bytesNeeded[' + position + ']'];
                                            var bytesCachedPool = val[i]['bytesCached[' + position + ']'];
                                            var bytesOverLimit = val[i]['bytesOverlimit[' + position + ']'];
                                            var poolName = val[i]['name'];
                                            var poolOwner = val[i]['owner'];
                                            var group = val[i]['group'];
                                            var mode = val[i]['mode'];
                                            var maxTtl = val[i]['maxTTL']; 
                                            maxTtl = formatTtl(maxTtl);
                                            //push to table
                                            array_types.push(typePool + " " + limit + " " + bytesNeededPool + " " + bytesCachedPool + " " + bytesOverLimit);
                                            
                                        }
                                    }   
                                    // create the schema and append it to table                         
                                    var poolInfo ='<tr class="clear-pool"><td>' + poolName + '</td><td>' + poolOwner + '</td><td>' + group + '</td><td>' + mode + '</td><td>' + maxTtl + '</td><td><table class="types-pool-table" id="type-' + poolName + '"></table></td><td><table class="limits-pool-table" id="limit-' + poolName + '"></table></td><td align="center"><table id="bn-pool-table-' + poolName + '"></table></td><td align="center"><table id="bc-pool-table-' + poolName + '"></table></td><td align="center"><table id="bo-pool-table-' + poolName + '"></table></td><td><div class="tools-cont"><img data-toggle="tooltip" title="Edit Pool" alt="Edit Pool" class="tools" id="edit-pool-img" src="images/pencil.png"/><img data-toggle="tooltip" title="Delete Pool" alt="Delete Pool" class="tools" id="delete-pool-img" src="images/cancel-button.png" /></div</td></tr>';
                                    $(poolInfo).appendTo("#cache-pools-table");
                                    for(var x=0;x<array_types.length;x++){
                                        var tempArr = array_types[x].split(/ /);                                    
                                        var typesInfo = '<tr><td class="bottom-line" id="'+ tempArr[0] +'">' + tempArr[0] + '</td></tr>';
                                        var limitInfo = '<tr><td class="bottom-line" id="' + tempArr[0] +'">' + formatBytes(tempArr[1]) + '</td></tr>';
                                        if(tempArr[2]==0){
                                            var bnInfo = '<tr><td align="center" class="bottom-line">-</td></tr>';
                                        }
                                        else{
                                            var bnInfo = '<tr><td align="center" class="bottom-line">' + tempArr[2] + '</td></tr>';
                                        }
                                        if(tempArr[3]==0){
                                            var bcInfo = '<tr><td align="center" class="bottom-line">-</td></tr>';
                                        }
                                        else{
                                            var bcInfo = '<tr><td align="center" class="bottom-line">' + tempArr[3] + '</td></tr>';
                                        }
                                        if(tempArr[4]==0){
                                            var boInfo = '<tr><td align="center" class="bottom-line">-</td></tr>';
                                        }
                                        else{
                                            var boInfo = '<tr><td align="center" class="bottom-line">' + tempArr[4] + '</td></tr>';
                                        }
                                        $(typesInfo).appendTo("#type-" + poolName);
                                        $(limitInfo).appendTo("#limit-" + poolName);
                                        $(bnInfo).appendTo("#bn-pool-table-" + poolName);
                                        $(bcInfo).appendTo("#bc-pool-table-" + poolName);
                                        $(boInfo).appendTo("#bo-pool-table-" + poolName);
                                    }
                                }                           
                            });                         
                        }
                        if(size==0){
                            // append line to table that takes colspan all and center text
                            var message = '<tr class="clear-pool"><td colspan="11">There are no pools to show</td></tr>';
                            $(message).appendTo("#cache-pools-table");
                        }
                    }               
                });
                $.each(directives_data, function( key, val ) {
                    if(key=="CacheDirectives"){
                        var i;
                        var size = ObjectLength(val);
                        var id; var pool; var replVector; var expiry; var path; var stats; var new_stats;
                        var bytesNeeded; var bytesCached; var filesNeeded; var filesCached; var type;
                        for(i=0;i<size;i++){
                            var array_types = [];
                            // for each cache directive
                            $.each( val[i], function( key2, value2 ) {
                                // simple ifs to get values from keys
                                if(key2=="pool"){
                                    pool = value2;
                                    if(pool == selected_pool){
                                        var obj_pool;
                                        for(obj_pool in val[i]){
                                            if(obj_pool.includes("type")){
                                                var temp = obj_pool;
                                                var position;
                                                var matches = temp.match(/\[(.*?)\]/);
                                                if (matches) {
                                                    position = matches[1];
                                                }
                                                var typeDir = val[i]['type[' + position + ']'];
                                                var bytesNeededDir = val[i]['bytesNeeded[' + position + ']'];
                                                var bytesCachedDir = val[i]['bytesCached[' + position + ']'];
                                                var filesNeeded = val[i]['filesNeeded[' + position + ']'];
                                                var filesCached = val[i]['filesCached[' + position + ']'];
                                                
                                                id = val[i]['id'];
                                        
                                                replVector = val[i]['replicationVector'];
                                                expiry = val[i]['expiry'];
                                                path = val[i]['path'];
                                        
                                                if(replVector.includes("MEM")){
                                                    replVector = replVector.replace("MEMORY", "M");
                                                }
                                                if(replVector.includes("DISK")){
                                                    replVector = replVector.replace("DISK", "D");
                                                }
                                                if(replVector.includes("SSD")){
                                                    replVector = replVector.replace("SSD", "S");
                                                }
                                        
                                                array_types.push(typeDir + " " + bytesNeededDir + " " + bytesCachedDir + " " + filesNeeded + " " + filesCached);
                                            
                                        }
                                    }

                                    // format expiry if not == never
                                    if(!(expiry == 'never')){
                                        var expiry_table = expiry.split('T');                                
                                        var exp_temp_time = expiry_table[1].split('+');
                                        var exp_date = expiry_table[0];
                                        var exp_time = exp_temp_time[0];
                                        // set new format
                                        expiry = exp_date + ' ' + exp_time;
                                    }   

                                    var dirInfo ='<tr class="clear-directive"><td>' + id + '</td><td>' + path + '</td><td>' + pool + '</td><td>' + expiry + '</td><td>' + replVector + '</td><td><table class="types-pool-table" id="type-' + id + '"></table></td><td align="center"><table id="bn-dir-table-' + id + '"></table></td><td align="center"><table id="bc-dir-table-' + id + '"></table></td><td align="center"><table id="fn-dir-table-' + id + '"></table></td><td align="center"><table id="fc-dir-table-' + id + '"></table></td><td><div class="tools-cont"><img data-toggle="tooltip" title="Edit Directive" alt="Edit Directive" class="tools" id="edit-directive-img" src="images/pencil.png"/><img data-toggle="tooltip" title="Delete Directive" alt="Delete Directive" class="tools" id="delete-directive-img" src="images/cancel-button.png" /></div></td></tr>';
                                    $(dirInfo).appendTo("#cache-directives-table");
                                    for(var x=0;x<array_types.length;x++){
                                        var tempArr = array_types[x].split(/ /);                                    
                                        var typesInfo = '<tr><td class="bottom-line" id="dir-type-'+ tempArr[0] +'">' + tempArr[0] + '</td></tr>';
                                        if(tempArr[1] == 0){
                                            var bnInfo = '<tr><td align="center" class="bottom-line">-</td></tr>';
                                        }
                                        else{
                                            var bnInfo = '<tr><td align="center" class="bottom-line">' + tempArr[1] + '</td></tr>';
                                        }
                                        if(tempArr[2] == 0){
                                            var bcInfo = '<tr><td align="center" class="bottom-line">-</td></tr>';
                                        }
                                        else{
                                            var bcInfo = '<tr><td align="center" class="bottom-line">' + tempArr[2] + '</td></tr>';
                                        }
                                        if(tempArr[3] == 0){
                                            var fnInfo = '<tr><td align="center" class="bottom-line">-</td></tr>';
                                        }
                                        else{
                                            var fnInfo = '<tr><td align="center" class="bottom-line">' + tempArr[3] + '</td></tr>';
                                        }
                                        if(tempArr[4] == 0){
                                            var fcInfo = '<tr><td align="center" class="bottom-line">-</td></tr>';
                                        }
                                        else{
                                            var fcInfo = '<tr><td align="center" class="bottom-line">' + tempArr[4] + '</td></tr>';
                                        }
                                                                                                                
                                        $(typesInfo).appendTo("#type-" + id);                                    
                                        $(bnInfo).appendTo("#bn-dir-table-" + id);
                                        $(bcInfo).appendTo("#bc-dir-table-" + id);
                                        $(fnInfo).appendTo("#fn-dir-table-" + id);
                                        $(fcInfo).appendTo("#fc-dir-table-" + id);
                                    }
                                    
                                }
                             }
                                    
                            });                             
                        }
                        // if no cache directives are found show message
                        if(size=0){
                            var msg = '<tr class="clear-directive"><td colspan="11">There are no cache directives to show</td></tr>';
                            $(msg).appendTo("#cache-directives-table");
                        }
                    }
                });
		    });
		};
        break;
      default:
        window.location.hash = "tab-overview";
        break;
    }
  }
  
  load_page();

  $(window).bind('hashchange', function () {
    load_page();
  });
  
  
})();
