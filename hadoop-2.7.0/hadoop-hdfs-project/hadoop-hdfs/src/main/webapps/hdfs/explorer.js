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
 * Cyprus University of Technology, 2017
 */
(function() {
  "use strict";

  // The chunk size of tailing the files, i.e., how many bytes will be shown
  // in the preview.
  var TAIL_CHUNK_SIZE = 32768;

  var current_directory = "";
  var selected_path = "";

  function show_err_msg(msg) {
    $('#alert-panel-body').html(msg);
    $('#alert-panel').show();
  }

  $(window).bind('hashchange', function () {
    $('#alert-panel').hide();

    var dir = window.location.hash.slice(1);
    if(dir == "") {
      dir = "/";
    }
    var links = document.getElementsByClassName("explorer-browse-links");
    for(var i = 0; i < links.length; i++){
        var path = $(links[i]).attr('inode-path');
        var type = $(links[i]).attr('inode-type');
        if (type != 'DIRECTORY') {
            var img = '<img data-toggle="tooltip" title="Edit Replication" alt="Edit Replication" class="tools" id="edit-replication" src="images/pencil.png"/>';
            var id = $(links[i]).attr('id');
            id = id + "-rep";
            document.getElementById(id).innerHTML = img;
        }
    } 
    if(current_directory != dir) {
      browse_directory(dir);
    }
  }); 
	$(document).on("click","#edit-replication", function(){
		var $row = $(this).closest("tr");
		var pathSuffix = $(this).closest('tr').children('td:last').text();
		selected_path = pathSuffix;
		var replication = $row.find("td:nth-child(6)").text();
		var diskRep, memRep, ssdRep, archiveRep, rdRep,dhRep, shRep, remoteRep, unspecRep;
		var mem = false, ram_disk = false, ssd_h = false, ssd = false, disk_h = false, disk = false, archive = false, remote = false; 
		replication = replication.split(/[[\],]/);
		for(var i=1;i<(replication.length-1);i++){
		    if(replication[i].includes("D")){		    	
		        var tmp = replication[i].split(/[=]/);
		        diskRep = tmp[1];
		    }
		    else if(replication[i].includes("M")){
                var tmp = replication[i].split(/[=]/);
                memRep = tmp[1];
            }
            else if(replication[i].includes("S")){
                var tmp = replication[i].split(/[=]/);
                ssdRep = tmp[1];
            }
            else if(replication[i].includes("A")){
                var tmp = replication[i].split(/[=]/);
                archiveRep = tmp[1];
            }
            else if(replication[i].includes("RD")){
                var tmp = replication[i].split(/[=]/);
                rdRep = tmp[1];
            }
            else if(replication[i].includes("DH")){
                var tmp = replication[i].split(/[=]/);
                dhRep = tmp[1];
            }
            else if(replication[i].includes("SH")){
                var tmp = replication[i].split(/[=]/);
                shRep = tmp[1];
            }
            else if(replication[i].includes("R")){
                var tmp = replication[i].split(/[=]/);                
                remoteRep = tmp[1];
            }
            else if(replication[i].includes("U")){
                var tmp = replication[i].split(/[=]/);                
                unspecRep = tmp[1];
            }
		    
		}
		// get storage types that are available and set their replication value if exist
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
                                        if(memRep != undefined){
                                            $("#MEMORY-replication").val('' + memRep);
                                        }
                                    }
                                    else if(type_stor == "RAM_DISK"){
                                        ram_disk = true;
                                        if(rdRep != undefined){
                                            $("#RAM_DISK-replication").val('' + rdRep);
                                        }
                                    }
                                    else if(type_stor =="SSD_H"){
                                        ssd_h = true;
                                        if(shRep != undefined){
                                            $("#SSD_H-replication").val('' + shRep);
                                        }
                                    }
                                    else if(type_stor == "SSD"){
                                        ssd = true;
                                        if(ssdRep != undefined){
                                           $("#SSD-replication").val('' + ssdRep);
                                        }
                                    }
                                    else if(type_stor == "DISK_H"){
                                        disk_h = true;
                                        if(dhRep != undefined){
                                            $("#DISK_H-replication").val('' + dhRep);
                                        }
                                    }
                                    else if(type_stor == "DISK"){
                                        disk = true;
                                        if(diskRep != undefined){
                                            $("#DISK-replication").val('' + diskRep);
                                        }
                                    }
                                    else if(type_stor == "ARCHIVE"){
                                        archive = true;
                                        if(archiveRep != undefined){
                                            $("#ARCHIVE-replication").val('' + archiveRep);
                                        }
                                    }
                                    else if(type_stor == "REMOTE"){
                                        remote = true;
                                        if(remoteRep != undefined){
                                            $("#REMOTE-replication").val('' + remoteRep);
                                        }
                                    }                                                                                                    
                                }
                            });
                        }
                        if(mem == false){
                            $("#memory-line").remove();                            
                        }
                        if(ram_disk == false){
                            $("#ram-disk-line").remove();
                        }
                        if(ssd_h == false){
                            $("#ssd-h-line").remove();
                        }
                        if(ssd == false){
                            $("#ssd-line").remove();
                        }
                        if(disk_h == false){
                            $("#disk-h-line").remove();
                        }
                        if(disk == false){
                            $("#disk-line").remove();
                        }
                        if(archive == false){
                            $("#archive-line").remove();
                        }
                        if(remote == false){
                            $("#remote-line").remove();
                        }
                    }           
                });
            });            
		$("#replication-wrapper").show();
		
	});
	$(document).ready(function(){
    	$('[data-toggle="tooltip"]').tooltip();
        var replElements = document.getElementsByClassName("repl");       
        $("#sourceFile").val(''); 
        var destination = $("#directory").val();
        $("#destinationPath").val(destination);        
    });        

	$(document).on('click', '#ok-button', function(){
		var replArr="";
        var diskRepl,memRepl,ssdRepl, archiveRepl, ram_diskRepl,disk_hRepl,ssd_hRepl,remoteRepl,unspecRepl;
		var chars = false;
		
		if(document.getElementById('replication-D')!=null){  
                diskRepl = $("#DISK-replication").val();
                if(!((diskRepl == ""))){
                    // not empty
                    if(diskRepl.match(/^\d+$/)){
                        if(diskRepl >= 0){
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
                        if(memRepl >= 0){
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
                	if(ssdRepl.match(/^\d+$/)){
                		if(ssdRepl >= 0){
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
                    	if(archiveRepl >= 0){
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
                    	if(ram_diskRepl >= 0){
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
                    	if(disk_hRepl>=0){
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
                    	if(ssd_hRepl>=0){
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
                    	if(remoteRepl>=0){
                        	if(replArr==""){
                            	replArr = replArr + 'R-' + remoteRepl;
                        	}
                        	else{
                            	replArr = replArr + '_R-' + remoteRepl;
                        	}
                    	}
                    	else if(remoteRepl<0){
                        	negative=true;
                    	}
                    }
                    else{
                    	chars = true;
                    }                    
                }
            }
            if(document.getElementById('replication-U')!=null){
                unspecRepl = $("#UNSPECIFIED-replication").val();
                if(!(unspecRepl == "")){
                    // not empty
                    if(unspecRepl.match(/^\d+$/)){
                    	if(unspecRepl>=0){
                        	if(replArr==""){
                            	replArr = replArr + 'U-' + unspecRepl;
                        	}
                        	else{
                            	replArr = replArr + '_U-' + unspecRepl;
                        	}
                    	}
                    	else if(unspecRepl<0){
                        	negative=true;
                    	}
                    }
                    else{
                    	chars = true;
                    }                    
                }
            }
         
        var url = "";  
		if(current_directory == "/"){
            url = '/webhdfs/v1/' + encode_path(selected_path) + '?op=SETNEWREPLICATION&vector=' + replArr;
        }
        else{
            var tmp = current_directory + '/' + selected_path;
            url = '/webhdfs/v1' + encode_path(tmp) + '?op=SETNEWREPLICATION&vector=' + replArr;
        }
		if(replArr==""){
		    var errorMsg ='<p>Replication must not be empty or contain alphabetical characters</p>';
            $("#error-field").empty().html(errorMsg);
		}		
		else if(chars==true){
		    var errorMsg ='<p>Value must not contain alphabetical characters</p>';
            $("#error-field").empty().html(errorMsg);
		}
		else{
		    if(confirm("Are you sure you want to save changes for " + selected_path + "?")){
                $.ajax({
                url: url,
                type: 'PUT',
                success: function(result, status, jqXHR) {
                    // get message returned - status may be OK but json response may say false
                    var successful = 'false';
                    $.each(jqXHR, function(key, val) {
                        if(key == 'responseText'){
                            var a = JSON.parse(val);
                            successful = a.boolean;
                        }                        
                    });
                    if(successful == true){
                        alert("Replication is set successfully!");
                        $("#MEMORY-replication").val('');
                        $("#RAM_DISK-replication").val('');
                        $("#SSD_H-replication").val('');
                        $("#SSD-replication").val('');
                        $("#DISK_H-replication").val('');
                        $("#DISK-replication").val('');
                        $("#ARCHIVE-replication").val('');
                        $("#REMOTE-replication").val('');
                        $("#error-field").empty();
                        $("#replication-wrapper").hide();
                        location.reload();
                    }
                    else{
                        alert("Error! Could not set new replication.");
                    }
                    
                }
                }).error(network_error_handler(url));        
            }
		}
	});
	$(document).on('click','#close-replication', function(){
	    $("#MEMORY-replication").val('');
        $("#RAM_DISK-replication").val('');
        $("#SSD_H-replication").val('');
        $("#SSD-replication").val('');
        $("#DISK_H-replication").val('');
        $("#DISK-replication").val('');
        $("#ARCHIVE-replication").val('');
        $("#REMOTE-replication").val('');
        $("#error-field").empty();
		$("#replication-wrapper").hide();
    });

    // upload file to FS
    $(document).on('submit', "#form", function(event){
        event.preventDefault();
        // get selected file
        var fileInput = document.getElementById("sourceFile");        
        var file = fileInput.files[0];
        // check if file is null/undefined
        if((file == null) || (file == undefined)){
            var err = '<p>Please select file to upload</p>';
            $("#errorPanel").html(err);            
        }
        else{
            $("#errorPanel").html('');
            var dest = $("#directory").val();        
            var xhr = new XMLHttpRequest();
            // Create a new FormData object
            var formData = new FormData();   
            formData.append('file', file);
            formData.append('destination', dest);

            var fileName = file.name;
            // check if file already exists in system            
            var fileWithoutSpc = "";
            if(fileName.includes(" ")){
                fileWithoutSpc = fileName.replace(/ /g, '_');
            }
            else{
                fileWithoutSpc = fileName;
            }
            // issue request for status of file
            var status_url = "";
		    if(current_directory == "/"){
                status_url = '/webhdfs/v1/' + encode_path(fileWithoutSpc) + '?op=GETFILESTATUS';
            }
            else{
                var tmp = dest + "/" + fileWithoutSpc;
                status_url = '/webhdfs/v1' + encode_path(tmp) + '?op=GETFILESTATUS';
            }

            var upload_path = '/webhdfs/v1/upload/' + encode_path(dest) + '?op=UPLOAD';
            $.ajax({
                url: status_url,
                type: 'GET',                
                success: function(result){
                    if(confirm("File " + fileName + " already exists. Are you sure you want to replace it?")){
                        doUpload(xhr, upload_path, fileName, formData);
                    }
                },
                error: function(result){
                    doUpload(xhr, upload_path, fileName, formData);
                }                    
            });
        }                                    
    });

    // actual file upload request
    function doUpload(xhr, upload_path, fileName, formData){
        xhr.open("POST", upload_path, true);
        xhr.send(formData);
        xhr.onload = function(e) {
            if (this.status == 201) {
                alert("Successfully uploaded file " + fileName + "!");
                $("#errorPanel").html('');
                $("#sourceFile").val(''); 
                location.reload(); 
            }
            else{
                alert('Error occured while uploading file.');
                switch(this.status){
                    case 401:
                        var msg = '<p>Authentication failed when trying to open ' + upload_path + ': Unauthorized.</p>';
                        break;
                    case 403:
                        if(this.responseText !== undefined) {
                            var msg;  
                            var a = JSON.parse(this.responseText);
                            $.each(a, function( key, val ) {
                                if(key == 'RemoteException'){
                                    $.each(val, function(key1, val1){
                                        if(key1 == 'message'){
                                            if(val1.includes("seconds")){
                                                var index = val1.indexOf("seconds.");
                                                var errorMsg = val1.slice(0, index + 8);
                                                msg = '<p>' + errorMsg + "</p>";
                                            }
                                            else if(val1.includes("minutes")){
                                                var index = val1.indexOf("minutes.");
                                                var errorMsg = val1.slice(0, index + 8);
                                                msg = '<p>' + errorMsg + "</p>";
                                            }                                                                                   
                                            else{
                                                msg = '<p>' + val1 + "</p>";
                                            }                                            
                                        }
                                    });
                                }
                            });
                            break;
                        }
                        var msg = '<p>Permission denied when trying to open ' + upload_path + '</p>';
                        break;
                    case 404:
                        var msg = '<p>Path does not exist on HDFS or WebHDFS is disabled.  Please check your path or enable WebHDFS</p>';
                        break;
                    default:
                        var msg = '<p>Failed to retrieve data from ' + upload_path + ': ' + this.responseText + '</p>';
                }
                show_err_msg(msg);
            }
        }; 
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
   	function ObjectLength( object ) {
    	var length = 0;
        for( var key in object ) {
        	if( object.hasOwnProperty(key) ) {
            	++length;
			}
		}
       	return length;
	};
    
  function network_error_handler(url) {
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
        default:
          var msg = '<p>Failed to retrieve data from ' + url + ': ' + err + '</p>';
        }
      show_err_msg(msg);
    };
  }  

  function append_path(prefix, s) {
    var l = prefix.length;
    var p = l > 0 && prefix[l - 1] == '/' ? prefix.substring(0, l - 1) : prefix;
    return p + '/' + s;
  }

  function get_response(data, type) {
    return data[type] !== undefined ? data[type] : null;
  }

  function get_response_err_msg(data) {
    return data.RemoteException !== undefined ? data.RemoteException.message : "";
  }

  function encode_path(abs_path) {
    abs_path = encodeURIComponent(abs_path);
    var re = /%2F/g;
    return abs_path.replace(re, '/');
  }

  function view_file_details(path, abs_path) {
    function show_block_info(blocks) {
      var menus = $('#file-info-blockinfo-list');
      menus.empty();

      menus.data("blocks", blocks);
      menus.change(function() {
        var d = $(this).data('blocks')[$(this).val()];
        if (d === undefined) {
          return;
        }

        dust.render('block-info', d, function(err, out) {
          $('#file-info-blockinfo-body').html(out);
        });

      });
      for (var i = 0; i < blocks.length; ++i) {
        var item = $('<option value="' + i + '">Block ' + i + '</option>');
        menus.append(item);
      }
      menus.change();
    }

    abs_path = encode_path(abs_path);
    var url = '/webhdfs/v1' + abs_path + '?op=GET_BLOCK_LOCATIONS';
    $.get(url).done(function(data) {
      var d = get_response(data, "LocatedBlocks");
      if (d === null) {
        show_err_msg(get_response_err_msg(data));
        return;
      }

      $('#file-info-tail').hide();
      $('#file-info-title').text("File information - " + path);

      var download_url = '/webhdfs/v1' + abs_path + '?op=OPEN';

      $('#file-info-download').attr('href', download_url);
      $('#file-info-preview').click(function() {
        var offset = d.fileLength - TAIL_CHUNK_SIZE;
        var url = offset > 0 ? download_url + '&offset=' + offset : download_url;
        $.get(url, function(t) {
          $('#file-info-preview-body').val(t);
          $('#file-info-tail').show();
        }, "text").error(network_error_handler(url));
      });

      // file delete - Working
      $("#file-delete").click(function(){
        var filePath = abs_path;
        // check if file starts with '/' - if true remove it
        if(filePath.startsWith("/")){
            filePath = filePath.slice(1);
        }
        var delete_url = '/webhdfs/v1/' + encode_path(filePath) + '?op=DELETE';
        if (confirm("Are you sure you want to delete file " + filePath + " ?")){
            $.ajax({
                url: delete_url,
                type: 'DELETE',
                crossDomain: true,
                headers: {
                    "Access-Control-Allow-Origin": "*",
                    "Access-Control-Allow-Methods": "PUT"
                },
                success: function(result, status, jqXHR){
                    alert("Successfully deleted file!");   
                    $("#errorPanel").html('');
                    $("#destPath").val('');  
                    $("#filePath").val(''); 
                    //refresh page     
                    location.reload();                
                }                    
            }).error(network_error_handler(delete_url));
        }        
      });

      if (d.fileLength > 0) {
        show_block_info(d.locatedBlocks);
        $('#file-info-blockinfo-panel').show();
      } else {
        $('#file-info-blockinfo-panel').hide();
      }
      $('#file-info').modal();      
    }).error(network_error_handler(url));
  }

  function browse_directory(dir) {
    var HELPERS = {
      'helper_date_tostring' : function (chunk, ctx, bodies, params) {
        var value = dust.helpers.tap(params.value, chunk, ctx);
        return chunk.write('' + new Date(Number(value)).toLocaleString());
      }
    };
    var url = '/webhdfs/v1' + encode_path(dir) + '?op=LISTSTATUS';
    $.get(url, function(data) {
      var d = get_response(data, "FileStatuses");
      if (d === null) {
        show_err_msg(get_response_err_msg(data));
        return;
      }

      current_directory = dir;
      $('#directory').val(dir);
      window.location.hash = dir;
      var base = dust.makeBase(HELPERS);
      dust.render('explorer', base.push(d), function(err, out) {
        $('#panel').html(out);
        // add edit image
        var links = document.getElementsByClassName("explorer-browse-links");
        for(var i = 0; i < links.length; i++){
            var path = $(links[i]).attr('inode-path');
            var type = $(links[i]).attr('inode-type');
            if (type != 'DIRECTORY') {
                var img = '<img data-toggle="tooltip" title="Edit Replication" alt="Edit Replication" class="tools" id="edit-replication" src="images/pencil.png"/>';
                var id = $(links[i]).attr('id');
                id = id + "-rep";
                document.getElementById(id).innerHTML = img;
            }
        }		
        $('.explorer-browse-links').click(function() {
          var type = $(this).attr('inode-type');
          var path = $(this).attr('inode-path');
          var abs_path = append_path(current_directory, path);
          if (type == 'DIRECTORY') {
            browse_directory(abs_path);
          } else {
            view_file_details(path, abs_path);
          }
        });
      });
    }).error(network_error_handler(url));
  }


  function init() {
    dust.loadSource(dust.compile($('#tmpl-explorer').html(), 'explorer'));
    dust.loadSource(dust.compile($('#tmpl-block-info').html(), 'block-info'));

    var b = function() { browse_directory($('#directory').val()); };
    $('#btn-nav-directory').click(b);
    var dir = window.location.hash.slice(1);
    if(dir == "") {
      window.location.hash = "/";
    } else {
      browse_directory(dir);
    }
  }

  init();
})();
