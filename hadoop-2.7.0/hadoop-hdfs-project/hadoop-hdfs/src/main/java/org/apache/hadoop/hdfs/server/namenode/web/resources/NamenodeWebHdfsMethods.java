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
 */
package org.apache.hadoop.hdfs.server.namenode.web.resources;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.security.PrivilegedExceptionAction;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.ArrayList;

import javax.servlet.ServletContext;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import javax.ws.rs.core.StreamingOutput;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CacheFlag;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Options;
import org.apache.hadoop.fs.ReplicationVector;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.fs.XAttr;
import org.apache.hadoop.fs.permission.AclStatus;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.XAttrHelper;
import org.apache.hadoop.hdfs.protocol.CacheDirectiveInfo;
import org.apache.hadoop.hdfs.protocol.CachePoolInfo;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.DirectoryListing;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.hdfs.protocol.CacheDirectiveInfo.Expiration;
import org.apache.hadoop.hdfs.security.token.delegation.DelegationTokenIdentifier;
import org.apache.hadoop.hdfs.security.token.delegation.DelegationTokenSecretManager;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockManager;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeDescriptor;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeStorageInfo;
import org.apache.hadoop.hdfs.server.common.JspHelper;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.hdfs.server.protocol.NamenodeProtocols;
import org.apache.hadoop.hdfs.web.JsonUtil;
import org.apache.hadoop.hdfs.web.ParamFilter;
import org.apache.hadoop.hdfs.web.SWebHdfsFileSystem;
import org.apache.hadoop.hdfs.web.WebHdfsFileSystem;
import org.apache.hadoop.hdfs.web.resources.*;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.ipc.RetriableException;
import org.apache.hadoop.ipc.Server;
import org.apache.hadoop.net.NetworkTopology.InvalidTopologyException;
import org.apache.hadoop.net.Node;
import org.apache.hadoop.net.NodeBase;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.hadoop.util.StringUtils;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Charsets;
import com.google.common.collect.Lists;
import com.sun.jersey.spi.container.ResourceFilters;

import com.sun.jersey.core.header.FormDataContentDisposition;
import com.sun.jersey.multipart.*;

import java.io.InputStream;

import org.apache.hadoop.fs.FileSystem;


/** Web-hdfs NameNode implementation. */
@Path("")
@ResourceFilters(ParamFilter.class)
public class NamenodeWebHdfsMethods {
  public static final Log LOG = LogFactory.getLog(NamenodeWebHdfsMethods.class);

  private static final UriFsPathParam ROOT = new UriFsPathParam("");
  
  private static final ThreadLocal<String> REMOTE_ADDRESS = new ThreadLocal<String>(); 

  /** @return the remote client address. */
  public static String getRemoteAddress() {
    return REMOTE_ADDRESS.get();
  }

  public static InetAddress getRemoteIp() {
    try {
      return InetAddress.getByName(getRemoteAddress());
    } catch (Exception e) {
      return null;
    }
  }

  /**
   * Returns true if a WebHdfs request is in progress.  Akin to
   * {@link Server#isRpcInvocation()}.
   */
  public static boolean isWebHdfsInvocation() {
    return getRemoteAddress() != null;
  }

  private @Context ServletContext context;
  private @Context HttpServletRequest request;
  private @Context HttpServletResponse response;

  private void init(final UserGroupInformation ugi,
      final DelegationParam delegation,
      final UserParam username, final DoAsParam doAsUser,
      final UriFsPathParam path, final HttpOpParam<?> op,
      final Param<?, ?>... parameters) {
    if (LOG.isTraceEnabled()) {
      LOG.trace("HTTP " + op.getValue().getType() + ": " + op + ", " + path
          + ", ugi=" + ugi + ", " + username + ", " + doAsUser
          + Param.toSortedString(", ", parameters));
    }

    //clear content type
    response.setContentType(null);
       
    // set the remote address, if coming in via a trust proxy server then
    // the address with be that of the proxied client
    REMOTE_ADDRESS.set(JspHelper.getRemoteAddr(request));
  }

  private void reset() {
    REMOTE_ADDRESS.set(null);
  }
  
  private static NamenodeProtocols getRPCServer(NameNode namenode)
      throws IOException {
     final NamenodeProtocols np = namenode.getRpcServer();
     if (np == null) {
       throw new RetriableException("Namenode is in startup mode");
     }
     return np;
  }
  
  @VisibleForTesting
  static DatanodeInfo chooseDatanode(final NameNode namenode,
      final String path, final HttpOpParam.Op op, final long openOffset,
      final long blocksize, final String excludeDatanodes) throws IOException {
    final BlockManager bm = namenode.getNamesystem().getBlockManager();
    
    HashSet<Node> excludes = new HashSet<Node>();
    if (excludeDatanodes != null) {
      for (String host : StringUtils
          .getTrimmedStringCollection(excludeDatanodes)) {
        int idx = host.indexOf(":");
        if (idx != -1) {          
          excludes.add(bm.getDatanodeManager().getDatanodeByXferAddr(
              host.substring(0, idx), Integer.parseInt(host.substring(idx + 1))));
        } else {
          excludes.add(bm.getDatanodeManager().getDatanodeByHost(host));
        }
      }
    }

    if (op == PutOpParam.Op.CREATE) {
      //choose a datanode near to client 
      final DatanodeDescriptor clientNode = bm.getDatanodeManager(
          ).getDatanodeByHost(getRemoteAddress());
      if (clientNode != null) {
        final DatanodeStorageInfo[] storages = bm.chooseTarget4WebHDFS(
            path, clientNode, excludes, blocksize);
        if (storages.length > 0) {
          return storages[0].getDatanodeDescriptor();
        }
      }
    } else if (op == GetOpParam.Op.OPEN
        || op == GetOpParam.Op.GETFILECHECKSUM
        || op == PostOpParam.Op.APPEND) {
      //choose a datanode containing a replica 
      final NamenodeProtocols np = getRPCServer(namenode);
      final HdfsFileStatus status = np.getFileInfo(path);
      if (status == null) {
        throw new FileNotFoundException("File " + path + " not found.");
      }
      final long len = status.getLen();
      if (op == GetOpParam.Op.OPEN) {
        if (openOffset < 0L || (openOffset >= len && len > 0)) {
          throw new IOException("Offset=" + openOffset
              + " out of the range [0, " + len + "); " + op + ", path=" + path);
        }
      }

      if (len > 0) {
        final long offset = op == GetOpParam.Op.OPEN? openOffset: len - 1;
        final LocatedBlocks locations = np.getBlockLocations(path, offset, 1);
        final int count = locations.locatedBlockCount();
        if (count > 0) {
          return bestNode(locations.get(0).getLocations(), excludes);
        }
      }
    } 

    return (DatanodeDescriptor)bm.getDatanodeManager().getNetworkTopology(
        ).chooseRandom(NodeBase.ROOT);
  }

  /**
   * Choose the datanode to redirect the request. Note that the nodes have been
   * sorted based on availability and network distances, thus it is sufficient
   * to return the first element of the node here.
   */
  private static DatanodeInfo bestNode(DatanodeInfo[] nodes,
      HashSet<Node> excludes) throws IOException {
    for (DatanodeInfo dn: nodes) {
      if (false == dn.isDecommissioned() && false == excludes.contains(dn)) {
        return dn;
      }
    }
    throw new IOException("No active nodes contain this block");
  }

  private Token<? extends TokenIdentifier> generateDelegationToken(
      final NameNode namenode, final UserGroupInformation ugi,
      final String renewer) throws IOException {
    final Credentials c = DelegationTokenSecretManager.createCredentials(
        namenode, ugi, renewer != null? renewer: ugi.getShortUserName());
    if (c == null) {
      return null;
    }
    final Token<? extends TokenIdentifier> t = c.getAllTokens().iterator().next();
    Text kind = request.getScheme().equals("http") ? WebHdfsFileSystem.TOKEN_KIND
        : SWebHdfsFileSystem.TOKEN_KIND;
    t.setKind(kind);
    return t;
  }

  private URI redirectURI(final NameNode namenode,
      final UserGroupInformation ugi, final DelegationParam delegation,
      final UserParam username, final DoAsParam doAsUser,
      final String path, final HttpOpParam.Op op, final long openOffset,
      final long blocksize, final String excludeDatanodes,
      final Param<?, ?>... parameters) throws URISyntaxException, IOException {
    final DatanodeInfo dn;
    try {
      dn = chooseDatanode(namenode, path, op, openOffset, blocksize,
          excludeDatanodes);
    } catch (InvalidTopologyException ite) {
      throw new IOException("Failed to find datanode, suggest to check cluster health.", ite);
    }

    final String delegationQuery;
    if (!UserGroupInformation.isSecurityEnabled()) {
      //security disabled
      delegationQuery = Param.toSortedString("&", doAsUser, username);
    } else if (delegation.getValue() != null) {
      //client has provided a token
      delegationQuery = "&" + delegation;
    } else {
      //generate a token
      final Token<? extends TokenIdentifier> t = generateDelegationToken(
          namenode, ugi, request.getUserPrincipal().getName());
      delegationQuery = "&" + new DelegationParam(t.encodeToUrlString());
    }
    final String query = op.toQueryString() + delegationQuery
        + "&" + new NamenodeAddressParam(namenode)
        + Param.toSortedString("&", parameters);    
    final String uripath = WebHdfsFileSystem.PATH_PREFIX + path;
    

    final String scheme = request.getScheme();
    int port = "http".equals(scheme) ? dn.getInfoPort() : dn
        .getInfoSecurePort();
    final URI uri = new URI(scheme, null, dn.getHostName(), port, uripath,
        query, null);

    if (LOG.isTraceEnabled()) {
      LOG.trace("redirectURI=" + uri);
    }
    return uri;
  }

  // make a redirect link for uploading files from local system
  // similar to redirectURI(), but with LocalFileParam which is used for the name of the file to be uploaded
  private URI redirectURIforLocal(final NameNode namenode,
      final UserGroupInformation ugi, final DelegationParam delegation,
      final UserParam username, final DoAsParam doAsUser,
      final String path, final HttpOpParam.Op op, final long openOffset,
      final long blocksize, final String excludeDatanodes, final LocalFileParam localFile,
      final Param<?, ?>... parameters) throws URISyntaxException, IOException {
    final DatanodeInfo dn;
    try {
      dn = chooseDatanode(namenode, path, op, openOffset, blocksize,
          excludeDatanodes);
    } catch (InvalidTopologyException ite) {
      throw new IOException("Failed to find datanode, suggest to check cluster health.", ite);
    }

    final String delegationQuery;
    if (!UserGroupInformation.isSecurityEnabled()) {
      //security disabled
      delegationQuery = Param.toSortedString("&", doAsUser, username);
    } else if (delegation.getValue() != null) {
      //client has provided a token
      delegationQuery = "&" + delegation;
    } else {
      //generate a token
      final Token<? extends TokenIdentifier> t = generateDelegationToken(
          namenode, ugi, request.getUserPrincipal().getName());
      delegationQuery = "&" + new DelegationParam(t.encodeToUrlString());
    }
    final String query = op.toQueryString() + delegationQuery
        + "&" + new NamenodeAddressParam(namenode) + "&" + localFile.getName() + "=" + localFile.getValue() + Param.toSortedString("&", parameters);    
    final String uripath = WebHdfsFileSystem.PATH_PREFIX + path;
    

    final String scheme = request.getScheme();
    int port = "http".equals(scheme) ? dn.getInfoPort() : dn
        .getInfoSecurePort();
    final URI uri = new URI(scheme, null, dn.getHostName(), port, uripath,
        query, null);

    if (LOG.isTraceEnabled()) {
      LOG.trace("redirectURI=" + uri);
    }
    return uri;
  }

  /** Handle HTTP PUT request for the root. */
  @PUT
  @Path("/")
  @Consumes({"*/*"})
  @Produces({MediaType.APPLICATION_OCTET_STREAM, MediaType.APPLICATION_JSON})
  public Response putRoot(
      @Context final UserGroupInformation ugi,
      @QueryParam(DelegationParam.NAME) @DefaultValue(DelegationParam.DEFAULT)
          final DelegationParam delegation,      
      @QueryParam(UserParam.NAME) @DefaultValue(UserParam.DEFAULT)
          final UserParam username,
      @QueryParam(DoAsParam.NAME) @DefaultValue(DoAsParam.DEFAULT)
          final DoAsParam doAsUser,
      @QueryParam(PutOpParam.NAME) @DefaultValue(PutOpParam.DEFAULT)
          final PutOpParam op,
      @QueryParam(DestinationParam.NAME) @DefaultValue(DestinationParam.DEFAULT)
          final DestinationParam destination,
      @QueryParam(OwnerParam.NAME) @DefaultValue(OwnerParam.DEFAULT)
          final OwnerParam owner,
      @QueryParam(GroupParam.NAME) @DefaultValue(GroupParam.DEFAULT)
          final GroupParam group,
      @QueryParam(PermissionParam.NAME) @DefaultValue(PermissionParam.DEFAULT)
          final PermissionParam permission,
      @QueryParam(OverwriteParam.NAME) @DefaultValue(OverwriteParam.DEFAULT)
          final OverwriteParam overwrite,
      @QueryParam(BufferSizeParam.NAME) @DefaultValue(BufferSizeParam.DEFAULT)
          final BufferSizeParam bufferSize,
      @QueryParam(ReplicationParam.NAME) @DefaultValue(ReplicationParam.DEFAULT)
          final ReplicationParam replication,
      @QueryParam(BlockSizeParam.NAME) @DefaultValue(BlockSizeParam.DEFAULT)
          final BlockSizeParam blockSize,
      @QueryParam(ModificationTimeParam.NAME) @DefaultValue(ModificationTimeParam.DEFAULT)
          final ModificationTimeParam modificationTime,
      @QueryParam(AccessTimeParam.NAME) @DefaultValue(AccessTimeParam.DEFAULT)
          final AccessTimeParam accessTime,
      @QueryParam(RenameOptionSetParam.NAME) @DefaultValue(RenameOptionSetParam.DEFAULT)
          final RenameOptionSetParam renameOptions,
      @QueryParam(CreateParentParam.NAME) @DefaultValue(CreateParentParam.DEFAULT)
          final CreateParentParam createParent,
      @QueryParam(TokenArgumentParam.NAME) @DefaultValue(TokenArgumentParam.DEFAULT)
          final TokenArgumentParam delegationTokenArgument,
      @QueryParam(AclPermissionParam.NAME) @DefaultValue(AclPermissionParam.DEFAULT) 
          final AclPermissionParam aclPermission,
      @QueryParam(XAttrNameParam.NAME) @DefaultValue(XAttrNameParam.DEFAULT) 
          final XAttrNameParam xattrName,
      @QueryParam(XAttrValueParam.NAME) @DefaultValue(XAttrValueParam.DEFAULT) 
          final XAttrValueParam xattrValue,
      @QueryParam(XAttrSetFlagParam.NAME) @DefaultValue(XAttrSetFlagParam.DEFAULT) 
          final XAttrSetFlagParam xattrSetFlag,
      @QueryParam(SnapshotNameParam.NAME) @DefaultValue(SnapshotNameParam.DEFAULT)
          final SnapshotNameParam snapshotName,
      @QueryParam(OldSnapshotNameParam.NAME) @DefaultValue(OldSnapshotNameParam.DEFAULT)
          final OldSnapshotNameParam oldSnapshotName,
      @QueryParam(ExcludeDatanodesParam.NAME) @DefaultValue(ExcludeDatanodesParam.DEFAULT)
          final ExcludeDatanodesParam excludeDatanodes,
      @QueryParam(StorTypeParam.NAME) @DefaultValue(StorTypeParam.DEFAULT)
      	  final StorTypeParam storageType,
      @QueryParam(StorLimitParam.NAME) @DefaultValue(StorLimitParam.DEFAULT)
          final StorLimitParam storageLimit,
      @QueryParam(PoolParam.NAME) @DefaultValue(PoolParam.DEFAULT)
      	  final PoolParam selectedPool,
      @QueryParam(StringReplicationParam.NAME) @DefaultValue(StringReplicationParam.DEFAULT)
      	  final StringReplicationParam stringReplication,
      @QueryParam(DirectiveIdParam.NAME) @DefaultValue(DirectiveIdParam.DEFAULT)
          final DirectiveIdParam directiveId,
      @QueryParam(TtlParam.NAME) @DefaultValue(TtlParam.DEFAULT)
            final TtlParam maxttlpool
      ) throws IOException, InterruptedException { // edited by nicolas
    return put(ugi, delegation, username, doAsUser, ROOT, op, destination,
        owner, group, permission, overwrite, bufferSize, replication,
        blockSize, modificationTime, accessTime, renameOptions, createParent,
        delegationTokenArgument, aclPermission, xattrName, xattrValue,
        xattrSetFlag, snapshotName, oldSnapshotName, excludeDatanodes, storageType, storageLimit, selectedPool, stringReplication, 
        directiveId,maxttlpool);
  }

  /** Handle HTTP PUT request. */
  @PUT
  @Path("{" + UriFsPathParam.NAME + ":.*}")
  @Consumes({"*/*"})
  @Produces({MediaType.APPLICATION_OCTET_STREAM, MediaType.APPLICATION_JSON})
  public Response put(
      @Context final UserGroupInformation ugi,
      @QueryParam(DelegationParam.NAME) @DefaultValue(DelegationParam.DEFAULT)
          final DelegationParam delegation,      
      @QueryParam(UserParam.NAME) @DefaultValue(UserParam.DEFAULT)
          final UserParam username,
      @QueryParam(DoAsParam.NAME) @DefaultValue(DoAsParam.DEFAULT)
          final DoAsParam doAsUser,
      @PathParam(UriFsPathParam.NAME) final UriFsPathParam path,
      @QueryParam(PutOpParam.NAME) @DefaultValue(PutOpParam.DEFAULT)
          final PutOpParam op,
      @QueryParam(DestinationParam.NAME) @DefaultValue(DestinationParam.DEFAULT)
          final DestinationParam destination,
      @QueryParam(OwnerParam.NAME) @DefaultValue(OwnerParam.DEFAULT)
          final OwnerParam owner,
      @QueryParam(GroupParam.NAME) @DefaultValue(GroupParam.DEFAULT)
          final GroupParam group,
      @QueryParam(PermissionParam.NAME) @DefaultValue(PermissionParam.DEFAULT)
          final PermissionParam permission,
      @QueryParam(OverwriteParam.NAME) @DefaultValue(OverwriteParam.DEFAULT)
          final OverwriteParam overwrite,
      @QueryParam(BufferSizeParam.NAME) @DefaultValue(BufferSizeParam.DEFAULT)
          final BufferSizeParam bufferSize,
      @QueryParam(ReplicationParam.NAME) @DefaultValue(ReplicationParam.DEFAULT)
          final ReplicationParam replication,
      @QueryParam(BlockSizeParam.NAME) @DefaultValue(BlockSizeParam.DEFAULT)
          final BlockSizeParam blockSize,
      @QueryParam(ModificationTimeParam.NAME) @DefaultValue(ModificationTimeParam.DEFAULT)
          final ModificationTimeParam modificationTime,
      @QueryParam(AccessTimeParam.NAME) @DefaultValue(AccessTimeParam.DEFAULT)
          final AccessTimeParam accessTime,
      @QueryParam(RenameOptionSetParam.NAME) @DefaultValue(RenameOptionSetParam.DEFAULT)
          final RenameOptionSetParam renameOptions,
      @QueryParam(CreateParentParam.NAME) @DefaultValue(CreateParentParam.DEFAULT)
          final CreateParentParam createParent,
      @QueryParam(TokenArgumentParam.NAME) @DefaultValue(TokenArgumentParam.DEFAULT)
          final TokenArgumentParam delegationTokenArgument,
      @QueryParam(AclPermissionParam.NAME) @DefaultValue(AclPermissionParam.DEFAULT) 
          final AclPermissionParam aclPermission,
      @QueryParam(XAttrNameParam.NAME) @DefaultValue(XAttrNameParam.DEFAULT) 
          final XAttrNameParam xattrName,
      @QueryParam(XAttrValueParam.NAME) @DefaultValue(XAttrValueParam.DEFAULT) 
          final XAttrValueParam xattrValue,
      @QueryParam(XAttrSetFlagParam.NAME) @DefaultValue(XAttrSetFlagParam.DEFAULT) 
          final XAttrSetFlagParam xattrSetFlag,
      @QueryParam(SnapshotNameParam.NAME) @DefaultValue(SnapshotNameParam.DEFAULT)
          final SnapshotNameParam snapshotName,
      @QueryParam(OldSnapshotNameParam.NAME) @DefaultValue(OldSnapshotNameParam.DEFAULT)
          final OldSnapshotNameParam oldSnapshotName,
      @QueryParam(ExcludeDatanodesParam.NAME) @DefaultValue(ExcludeDatanodesParam.DEFAULT)
          final ExcludeDatanodesParam excludeDatanodes,
      @QueryParam(StorTypeParam.NAME) @DefaultValue(StorTypeParam.DEFAULT)
  	      final StorTypeParam storageType,
      @QueryParam(StorLimitParam.NAME) @DefaultValue(StorLimitParam.DEFAULT)
          final StorLimitParam storageLimit,
      @QueryParam(PoolParam.NAME) @DefaultValue(PoolParam.DEFAULT)
  	  	  final PoolParam selectedPool,
  	  @QueryParam(StringReplicationParam.NAME) @DefaultValue(StringReplicationParam.DEFAULT)
  	      final StringReplicationParam stringReplication,
  	  @QueryParam(DirectiveIdParam.NAME) @DefaultValue(DirectiveIdParam.DEFAULT)
          final DirectiveIdParam directiveId,
      @QueryParam(TtlParam.NAME) @DefaultValue(TtlParam.DEFAULT)
            final TtlParam maxttlpool
      ) throws IOException, InterruptedException {

    init(ugi, delegation, username, doAsUser, path, op, destination, owner,
        group, permission, overwrite, bufferSize, replication, blockSize,
        modificationTime, accessTime, renameOptions, delegationTokenArgument,
        aclPermission, xattrName, xattrValue, xattrSetFlag, snapshotName,
        oldSnapshotName, excludeDatanodes, storageType, storageLimit, selectedPool, stringReplication, directiveId,maxttlpool); // edited by nicolas

    return ugi.doAs(new PrivilegedExceptionAction<Response>() {
      @Override
      public Response run() throws IOException, URISyntaxException {
        try {	// edited by nicolas
          return put(ugi, delegation, username, doAsUser,
              path.getAbsolutePath(), op, destination, owner, group,
              permission, overwrite, bufferSize, replication, blockSize,
              modificationTime, accessTime, renameOptions, createParent,
              delegationTokenArgument, aclPermission, xattrName, xattrValue,
              xattrSetFlag, snapshotName, oldSnapshotName, excludeDatanodes, storageType, storageLimit, selectedPool, stringReplication, directiveId,
              maxttlpool);
        } finally {
          reset();
        }
      }
    });
  }

  private Response put(
      final UserGroupInformation ugi,
      final DelegationParam delegation,      
      final UserParam username,
      final DoAsParam doAsUser,
      final String fullpath,
      final PutOpParam op,
      final DestinationParam destination,
      final OwnerParam owner,
      final GroupParam group,
      final PermissionParam permission,
      final OverwriteParam overwrite,
      final BufferSizeParam bufferSize,
      final ReplicationParam replication,
      final BlockSizeParam blockSize,
      final ModificationTimeParam modificationTime,
      final AccessTimeParam accessTime,
      final RenameOptionSetParam renameOptions,
      final CreateParentParam createParent,
      final TokenArgumentParam delegationTokenArgument,
      final AclPermissionParam aclPermission,
      final XAttrNameParam xattrName,
      final XAttrValueParam xattrValue, 
      final XAttrSetFlagParam xattrSetFlag,
      final SnapshotNameParam snapshotName,
      final OldSnapshotNameParam oldSnapshotName,      
      final ExcludeDatanodesParam exclDatanodes,
      final StorTypeParam storageType,
      final StorLimitParam storageLimit,
      final PoolParam selectedPool,
      final StringReplicationParam stringReplication,
      final DirectiveIdParam directiveId,
      final TtlParam maxttlpool
      //final LocalFileParam localFile
      ) throws IOException, URISyntaxException {

    final Configuration conf = (Configuration)context.getAttribute(JspHelper.CURRENT_CONF);
    final NameNode namenode = (NameNode)context.getAttribute("name.node");
    final NamenodeProtocols np = getRPCServer(namenode);

    switch(op.getValue()) {
    case CREATE:
    {
        final URI uri = redirectURI(namenode, ugi, delegation, username,
          doAsUser, fullpath, op.getValue(), -1L, blockSize.getValue(conf),
          exclDatanodes.getValue(), permission, overwrite, bufferSize,
          replication, blockSize);
        /* 
            for future reference: If calls are made by AJAX, access control headers are needed by this,
            and the correspodning methods in WebHdfsHandler.java
            Also, for catching exceptions, the same headers must be provided in ExceptionHandler.java
            eg: return Response.temporaryRedirect(uri).header("Access-Control-Allow-Origin", "*").header("Access-Control-Allow-Methods", "PUT").type(MediaType.APPLICATION_OCTET_STREAM).build();
        */
        return Response.temporaryRedirect(uri).type(MediaType.APPLICATION_OCTET_STREAM).build();
      
    }     
    case MKDIRS:
    {
      final boolean b = np.mkdirs(fullpath, permission.getFsPermission(), true);
      final String js = JsonUtil.toJsonString("boolean", b);
      return Response.ok(js).type(MediaType.APPLICATION_JSON).build();
    }

    /*added by nicolas
     * create cache pool
     * */
    case MAKEPOOL:
    {
    	if(fullpath == null){
    		// error response
    		return Response.serverError().status(Status.BAD_REQUEST).build();
    	}
    	String[] poolName = fullpath.split("/");
    	CachePoolInfo info = new CachePoolInfo(poolName[1]);       	    	
    	String typeString = storageType.getValue();
    	if(typeString == null){
    		// error response
    		return Response.serverError().status(Status.BAD_REQUEST).build();
    	}
    	String[] typeArray = typeString.split("-");
    	String limitPerTypeString = storageLimit.getValue();
    	if(limitPerTypeString == null){
    		// error response
    		return Response.serverError().status(Status.BAD_REQUEST).build();
    	}
    	String[] limitArray = limitPerTypeString.split("-");
    	for(int i=0; i <typeArray.length; i++){
    		StorageType type = StorageType.parseStorageType(typeArray[i]);
            Long limitPerType = StringUtils.TraditionalBinaryPrefix.string2long(limitArray[i]);
            info.setLimit(limitPerType, type);
    	}
    	String maxTtlString = maxttlpool.getValue();
    	Long maxTtl = null;
    	if (maxTtlString != null) {
    		if (maxTtlString.equalsIgnoreCase("never")) {
    			maxTtl = CachePoolInfo.RELATIVE_EXPIRY_NEVER;
            } 
            else if(maxTtlString.equals("0")){
                maxTtl = CachePoolInfo.RELATIVE_EXPIRY_NEVER;
            }
            else {
    	    	maxTtl = DFSUtil.parseRelativeTime(maxTtlString);
    	    }
        }
        else{
            // in case maxTtlString is empty then it has to be never - by default
            maxTtl = CachePoolInfo.RELATIVE_EXPIRY_NEVER;
        }

    	if (maxTtl != null) {
    		info.setMaxRelativeExpiryMs(maxTtl);
        }

    	np.addCachePool(info);
    	final String js = JsonUtil.toJsonString("boolean", true);
    	return Response.ok(js).type(MediaType.APPLICATION_JSON).build();
    }
    // delete cache pool
    case DELETEPOOL:
    {
    	if(fullpath == null){
    		// error response
    		return Response.serverError().status(Status.BAD_REQUEST).build();
    	}
    	String[] poolName = fullpath.split("/");
    	np.removeCachePool(poolName[1]);
    	final String js = JsonUtil.toJsonString("boolean", true);
    	return Response.ok(js).type(MediaType.APPLICATION_JSON).build();
    }
    // modify cache pool
    case MODIFYPOOL:
    {
    	if(fullpath == null){
    		// error response
    		return Response.serverError().status(Status.BAD_REQUEST).build();
    	}
    	String[] poolName = fullpath.split("/");
    	CachePoolInfo info = new CachePoolInfo(poolName[1]);      	
    	String typeString = storageType.getValue();
    	if(typeString == null){
    		// error response
    		return Response.serverError().status(Status.BAD_REQUEST).build();
        }        
    	String[] typeArray = typeString.split("-");
    	String limitPerTypeString = storageLimit.getValue();
    	if(limitPerTypeString == null){
    		// error response
    		return Response.serverError().status(Status.BAD_REQUEST).build();
        }        
    	String[] limitArray = limitPerTypeString.split("-");
    	for(int i=0; i <typeArray.length; i++){
    		StorageType type = StorageType.parseStorageType(typeArray[i]);
            Long limitPerType = StringUtils.TraditionalBinaryPrefix.string2long(limitArray[i]);
            info.setLimit(limitPerType, type);
        }
        
    	String maxTtlString = maxttlpool.getValue();
        Long maxTtl = null;        
    	if (maxTtlString != null) {
    		if (maxTtlString.equalsIgnoreCase("never")) {
    			maxTtl = CachePoolInfo.RELATIVE_EXPIRY_NEVER;
            } 
            else if(maxTtlString.equals("0")){
                maxTtl = CachePoolInfo.RELATIVE_EXPIRY_NEVER;
            }
            else {
    	    	maxTtl = DFSUtil.parseRelativeTime(maxTtlString);
    	    }
        }
        else{
            // in case maxTtlString is empty then it has to be modified to never - by default
            maxTtl = CachePoolInfo.RELATIVE_EXPIRY_NEVER;
        }

    	if (maxTtl != null) {
            info.setMaxRelativeExpiryMs(maxTtl);        
        }

    	np.modifyCachePool(info);
    	final String js = JsonUtil.toJsonString("boolean", true);
    	return Response.ok(js).type(MediaType.APPLICATION_JSON).build();
    }       
    // create new cache directive
    case MAKECACHEDIRECTIVE:
    {
    	CacheDirectiveInfo.Builder builder = new CacheDirectiveInfo.Builder();
    	builder.setPath(new org.apache.hadoop.fs.Path(fullpath));
    	builder.setPool(selectedPool.getValue());
    	String replicationString = stringReplication.getValue();
    	if(replicationString != null){
    		replicationString = replicationString.replace("_", ",");
    		replicationString = replicationString.replace("-", "=");
    		
    		long replicationV = ReplicationVector.ParseReplicationVector(replicationString);
      	  	builder.setReplVector(replicationV);
    	}    	
    	String ttlString = maxttlpool.getValue();
        CacheDirectiveInfo.Expiration ex = null;
        if (ttlString != null) {
            if (ttlString.equalsIgnoreCase("never")) {
              ex = CacheDirectiveInfo.Expiration.NEVER;
            } 
            else if(ttlString.equals("0")){
                ex = CacheDirectiveInfo.Expiration.NEVER;
            }
            else {
              long ttl = DFSUtil.parseRelativeTime(ttlString);
              ex = CacheDirectiveInfo.Expiration.newRelative(ttl);
            }
        }  
        else{
            ex = CacheDirectiveInfo.Expiration.NEVER;
        }      
        if (ex != null) {
          builder.setExpiration(ex);
        }
        
  	  	CacheDirectiveInfo directive = builder.build();
  	  	EnumSet<CacheFlag> flags = EnumSet.noneOf(CacheFlag.class); 
  	  	long id = np.addCacheDirective(directive, flags);
  	  	
    	final String js = JsonUtil.toJsonString("boolean", true);
    	return Response.ok(js).type(MediaType.APPLICATION_JSON).build();
    }
    // modify cache directice
    case MODIFYDIRECTIVE:
    {
    	CacheDirectiveInfo.Builder builder = new CacheDirectiveInfo.Builder();
    	boolean modified = false;
    	String idString = directiveId.getValue();
    	if(idString == null){
    		// error response
    		return Response.serverError().status(Status.BAD_REQUEST).build();
    	}
    	builder.setId(Long.parseLong(idString));
    	String path = fullpath.toString();
    	if(path != null){
    		builder.setPath(new org.apache.hadoop.fs.Path(path));
            modified = true;
    	}
    	String replicationString = stringReplication.getValue();
    	if(replicationString != null){
    		replicationString = replicationString.replace("_", ",");
    		replicationString = replicationString.replace("-", "=");
    		long replicationV = ReplicationVector.ParseReplicationVector(replicationString);
      	  	builder.setReplVector(replicationV);
      	  	modified = true;
    	} 
    	String poolName = selectedPool.getValue();
    	if (poolName != null) {
    		builder.setPool(poolName);
    	    modified = true;
    	}
    	String ttlString = maxttlpool.getValue();
        CacheDirectiveInfo.Expiration ex = null;
        if (ttlString != null) {
            if (ttlString.equalsIgnoreCase("never")) {
              ex = CacheDirectiveInfo.Expiration.NEVER;
            } 
            else if(ttlString.equals("0")){
                ex = CacheDirectiveInfo.Expiration.NEVER;
            }
            else {
              long ttl = DFSUtil.parseRelativeTime(ttlString);
              ex = CacheDirectiveInfo.Expiration.newRelative(ttl);
            }
        }        
        if (ex != null) {
          builder.setExpiration(ex);
        }
    	EnumSet<CacheFlag> flags = EnumSet.noneOf(CacheFlag.class);
        np.modifyCacheDirective(builder.build(), flags);
        
        final String js = JsonUtil.toJsonString("boolean", true);
    	return Response.ok(js).type(MediaType.APPLICATION_JSON).build();
    	
    }
    case DELETEDIRECTIVE:
    {
    	String idString = directiveId.getValue();
    	if(idString == null){
    		return Response.serverError().status(Status.BAD_REQUEST).build();
    	}
    	else if(!(idString.matches("[0-9]+"))){
    		return Response.serverError().status(Status.BAD_REQUEST).build();
    	}
    	long id;    	
    	id = Long.parseLong(idString);
    	np.removeCacheDirective(id);

    	final String js = JsonUtil.toJsonString("boolean", true);
    	return Response.ok(js).type(MediaType.APPLICATION_JSON).build();
    }
    case CREATESYMLINK:
    {
      np.createSymlink(destination.getValue(), fullpath,
          PermissionParam.getDefaultFsPermission(), createParent.getValue());
      return Response.ok().type(MediaType.APPLICATION_OCTET_STREAM).build();
    }
    case RENAME:
    {
      final EnumSet<Options.Rename> s = renameOptions.getValue();
      if (s.isEmpty()) {
        final boolean b = np.rename(fullpath, destination.getValue());
        final String js = JsonUtil.toJsonString("boolean", b);
        return Response.ok(js).type(MediaType.APPLICATION_JSON).build();
      } else {
        np.rename2(fullpath, destination.getValue(),
            s.toArray(new Options.Rename[s.size()]));
        return Response.ok().type(MediaType.APPLICATION_OCTET_STREAM).build();
      }
    }
    case SETREPLICATION:
    {
      final boolean b = np.setReplication(fullpath, replication.getValue(conf));
      final String js = JsonUtil.toJsonString("boolean", b);
      return Response.ok(js).type(MediaType.APPLICATION_JSON).build();
    }
    case SETNEWREPLICATION:
    {	
    	String replicationString = stringReplication.getValue();
    	replicationString = replicationString.replace("_", ",");
		replicationString = replicationString.replace("-", "=");
		long replicationV = ReplicationVector.ParseReplicationVector(replicationString);
    	final boolean b = np.setReplication(fullpath, replicationV);
    	final String js = JsonUtil.toJsonString("boolean", b);
        return Response.ok(js).type(MediaType.APPLICATION_JSON).build();
    }
    case SETOWNER:
    {
      if (owner.getValue() == null && group.getValue() == null) {
        throw new IllegalArgumentException("Both owner and group are empty.");
      }

      np.setOwner(fullpath, owner.getValue(), group.getValue());
      return Response.ok().type(MediaType.APPLICATION_OCTET_STREAM).build();
    }
    case SETPERMISSION:
    {
      np.setPermission(fullpath, permission.getFsPermission());
      return Response.ok().type(MediaType.APPLICATION_OCTET_STREAM).build();
    }
    case SETTIMES:
    {
      np.setTimes(fullpath, modificationTime.getValue(), accessTime.getValue());
      return Response.ok().type(MediaType.APPLICATION_OCTET_STREAM).build();
    }
    case RENEWDELEGATIONTOKEN:
    {
      final Token<DelegationTokenIdentifier> token = new Token<DelegationTokenIdentifier>();
      token.decodeFromUrlString(delegationTokenArgument.getValue());
      final long expiryTime = np.renewDelegationToken(token);
      final String js = JsonUtil.toJsonString("long", expiryTime);
      return Response.ok(js).type(MediaType.APPLICATION_JSON).build();
    }
    case CANCELDELEGATIONTOKEN:
    {
      final Token<DelegationTokenIdentifier> token = new Token<DelegationTokenIdentifier>();
      token.decodeFromUrlString(delegationTokenArgument.getValue());
      np.cancelDelegationToken(token);
      return Response.ok().type(MediaType.APPLICATION_OCTET_STREAM).build();
    }
    case MODIFYACLENTRIES: {
      np.modifyAclEntries(fullpath, aclPermission.getAclPermission(true));
      return Response.ok().type(MediaType.APPLICATION_OCTET_STREAM).build();
    }
    case REMOVEACLENTRIES: {
      np.removeAclEntries(fullpath, aclPermission.getAclPermission(false));
      return Response.ok().type(MediaType.APPLICATION_OCTET_STREAM).build();
    }
    case REMOVEDEFAULTACL: {
      np.removeDefaultAcl(fullpath);
      return Response.ok().type(MediaType.APPLICATION_OCTET_STREAM).build();
    }
    case REMOVEACL: {
      np.removeAcl(fullpath);
      return Response.ok().type(MediaType.APPLICATION_OCTET_STREAM).build();
    }
    case SETACL: {
      np.setAcl(fullpath, aclPermission.getAclPermission(true));
      return Response.ok().type(MediaType.APPLICATION_OCTET_STREAM).build();
    }
    case SETXATTR: {
      np.setXAttr(
          fullpath,
          XAttrHelper.buildXAttr(xattrName.getXAttrName(),
              xattrValue.getXAttrValue()), xattrSetFlag.getFlag());
      return Response.ok().type(MediaType.APPLICATION_OCTET_STREAM).build();
    }
    case REMOVEXATTR: {
      np.removeXAttr(fullpath, XAttrHelper.buildXAttr(xattrName.getXAttrName()));
      return Response.ok().type(MediaType.APPLICATION_OCTET_STREAM).build();
    }
    case CREATESNAPSHOT: {
      String snapshotPath = np.createSnapshot(fullpath, snapshotName.getValue());
      final String js = JsonUtil.toJsonString(
          org.apache.hadoop.fs.Path.class.getSimpleName(), snapshotPath);
      return Response.ok(js).type(MediaType.APPLICATION_JSON).build();
    }
    case RENAMESNAPSHOT: {
      np.renameSnapshot(fullpath, oldSnapshotName.getValue(),
          snapshotName.getValue());
      return Response.ok().type(MediaType.APPLICATION_OCTET_STREAM).build();
    }
    default:
      throw new UnsupportedOperationException(op + " is not supported");
    }
  }

  /** Handle HTTP POST request for the root. */
  @POST
  @Path("/")
  @Consumes({"*/*", MediaType.MULTIPART_FORM_DATA})
  @Produces({MediaType.APPLICATION_OCTET_STREAM, MediaType.APPLICATION_JSON})
  public Response postRoot(
      @Context final UserGroupInformation ugi,
      @QueryParam(DelegationParam.NAME) @DefaultValue(DelegationParam.DEFAULT)
          final DelegationParam delegation,
      @QueryParam(UserParam.NAME) @DefaultValue(UserParam.DEFAULT)
          final UserParam username,
      @QueryParam(DoAsParam.NAME) @DefaultValue(DoAsParam.DEFAULT)
          final DoAsParam doAsUser,
      @QueryParam(PostOpParam.NAME) @DefaultValue(PostOpParam.DEFAULT)
          final PostOpParam op,
      @QueryParam(ConcatSourcesParam.NAME) @DefaultValue(ConcatSourcesParam.DEFAULT)
          final ConcatSourcesParam concatSrcs,
      @QueryParam(BufferSizeParam.NAME) @DefaultValue(BufferSizeParam.DEFAULT)
          final BufferSizeParam bufferSize,
      @QueryParam(ExcludeDatanodesParam.NAME) @DefaultValue(ExcludeDatanodesParam.DEFAULT)
          final ExcludeDatanodesParam excludeDatanodes,
      @QueryParam(NewLengthParam.NAME) @DefaultValue(NewLengthParam.DEFAULT)
          final NewLengthParam newLength
      ) throws IOException, InterruptedException {
            return post(ugi, delegation, username, doAsUser, ROOT, op, concatSrcs,
                bufferSize, excludeDatanodes, newLength);          
  }

  // Handle HTTP UPLOAD for ROOT
  @POST
  @Path("/upload")
  @Consumes(MediaType.MULTIPART_FORM_DATA)
  @Produces(MediaType.APPLICATION_JSON)
  public Response postUploadFileRoot(
    @Context final UserGroupInformation ugi,
    @QueryParam(DelegationParam.NAME) @DefaultValue(DelegationParam.DEFAULT)
          final DelegationParam delegation,
    @QueryParam(UserParam.NAME) @DefaultValue(UserParam.DEFAULT)
          final UserParam username,
    @QueryParam(DoAsParam.NAME) @DefaultValue(DoAsParam.DEFAULT)
          final DoAsParam doAsUser,
    @QueryParam(PostOpParam.NAME) @DefaultValue(PostOpParam.DEFAULT)
          final PostOpParam op,
    @QueryParam(BlockSizeParam.NAME) @DefaultValue(BlockSizeParam.DEFAULT)
          final BlockSizeParam blockSize,
    @QueryParam(PermissionParam.NAME) @DefaultValue(PermissionParam.DEFAULT)
          final PermissionParam permission,
    @QueryParam(OverwriteParam.NAME) @DefaultValue(OverwriteParam.DEFAULT)
          final OverwriteParam overwrite,
    @QueryParam(BufferSizeParam.NAME) @DefaultValue(BufferSizeParam.DEFAULT)
          final BufferSizeParam bufferSize,
    @QueryParam(ReplicationParam.NAME) @DefaultValue(ReplicationParam.DEFAULT)
          final ReplicationParam replication,
    @QueryParam(ExcludeDatanodesParam.NAME) @DefaultValue(ExcludeDatanodesParam.DEFAULT)
          final ExcludeDatanodesParam exclDatanodes,
    @FormDataParam("file") final InputStream uploadedInputStream,
    @FormDataParam("file") final FormDataContentDisposition fileDetail,
    @FormDataParam("destination") final String destination) throws IOException, URISyntaxException{
        
        return postUpload(ugi, delegation, username, doAsUser, 
            ROOT.getValueString(), op, blockSize, permission, overwrite, 
            bufferSize, replication, exclDatanodes, uploadedInputStream, fileDetail, destination);                   
  }  

  /** Handle HTTP POST request. */
  @POST
  @Path("{" + UriFsPathParam.NAME + ":.*}")
  @Consumes({"*/*", MediaType.MULTIPART_FORM_DATA})
  @Produces({MediaType.APPLICATION_OCTET_STREAM, MediaType.APPLICATION_JSON})
  public Response post(
      @Context final UserGroupInformation ugi,
      @QueryParam(DelegationParam.NAME) @DefaultValue(DelegationParam.DEFAULT)
          final DelegationParam delegation,
      @QueryParam(UserParam.NAME) @DefaultValue(UserParam.DEFAULT)
          final UserParam username,
      @QueryParam(DoAsParam.NAME) @DefaultValue(DoAsParam.DEFAULT)
          final DoAsParam doAsUser,
      @PathParam(UriFsPathParam.NAME) final UriFsPathParam path,
      @QueryParam(PostOpParam.NAME) @DefaultValue(PostOpParam.DEFAULT)
          final PostOpParam op,
      @QueryParam(ConcatSourcesParam.NAME) @DefaultValue(ConcatSourcesParam.DEFAULT)
          final ConcatSourcesParam concatSrcs,
      @QueryParam(BufferSizeParam.NAME) @DefaultValue(BufferSizeParam.DEFAULT)
          final BufferSizeParam bufferSize,
      @QueryParam(ExcludeDatanodesParam.NAME) @DefaultValue(ExcludeDatanodesParam.DEFAULT)
          final ExcludeDatanodesParam excludeDatanodes,
      @QueryParam(NewLengthParam.NAME) @DefaultValue(NewLengthParam.DEFAULT)
          final NewLengthParam newLength
      ) throws IOException, InterruptedException {

    init(ugi, delegation, username, doAsUser, path, op, concatSrcs, bufferSize,
        excludeDatanodes, newLength);

    return ugi.doAs(new PrivilegedExceptionAction<Response>() {
      @Override
      public Response run() throws IOException, URISyntaxException {
        try {
            return post(ugi, delegation, username, doAsUser,
              path.getAbsolutePath(), op, concatSrcs, bufferSize,
              excludeDatanodes, newLength);
        } finally {
          reset();
        }
      }
    });
  }

  // Handle HTTP UPLOAD
  @POST
  @Path("/upload/{" + UriFsPathParam.NAME + ":.*}")
  @Consumes(MediaType.MULTIPART_FORM_DATA)
  @Produces(MediaType.APPLICATION_JSON)
  public Response postUploadFile(
    @Context final UserGroupInformation ugi,
    @QueryParam(DelegationParam.NAME) @DefaultValue(DelegationParam.DEFAULT)
          final DelegationParam delegation,
    @QueryParam(UserParam.NAME) @DefaultValue(UserParam.DEFAULT)
          final UserParam username,
    @QueryParam(DoAsParam.NAME) @DefaultValue(DoAsParam.DEFAULT)
          final DoAsParam doAsUser,
    @PathParam(UriFsPathParam.NAME) final UriFsPathParam path,   
    @QueryParam(PostOpParam.NAME) @DefaultValue(PostOpParam.DEFAULT)
          final PostOpParam op,
    @QueryParam(BlockSizeParam.NAME) @DefaultValue(BlockSizeParam.DEFAULT)
          final BlockSizeParam blockSize,
    @QueryParam(PermissionParam.NAME) @DefaultValue(PermissionParam.DEFAULT)
          final PermissionParam permission,
    @QueryParam(OverwriteParam.NAME) @DefaultValue(OverwriteParam.DEFAULT)
          final OverwriteParam overwrite,
    @QueryParam(BufferSizeParam.NAME) @DefaultValue(BufferSizeParam.DEFAULT)
          final BufferSizeParam bufferSize,
    @QueryParam(ReplicationParam.NAME) @DefaultValue(ReplicationParam.DEFAULT)
          final ReplicationParam replication,
    @QueryParam(ExcludeDatanodesParam.NAME) @DefaultValue(ExcludeDatanodesParam.DEFAULT)
          final ExcludeDatanodesParam exclDatanodes,
    @FormDataParam("file") final InputStream uploadedInputStream,
    @FormDataParam("file") final FormDataContentDisposition fileDetail,
    @FormDataParam("destination") final String destination) throws IOException, InterruptedException{
        
        init(ugi, delegation, username, doAsUser, path, op, bufferSize);

        return ugi.doAs(new PrivilegedExceptionAction<Response>() {
            @Override
            public Response run() throws IOException, URISyntaxException {
              try {
                      return postUpload(ugi, delegation, username, doAsUser,
                        path.getAbsolutePath(), op, blockSize, permission, overwrite, 
                        bufferSize, replication, exclDatanodes, uploadedInputStream, fileDetail, destination);
              } finally {
                reset();
              }
            }
          });
  }

  // handle Upload of file
  private Response postUpload(
    final UserGroupInformation ugi,
    final DelegationParam delegation,
    final UserParam username,
    final DoAsParam doAsUser,
    final String fullpath,
    final PostOpParam op,
    final BlockSizeParam blockSize,
    final PermissionParam permission,
    final OverwriteParam overwrite,
    final BufferSizeParam bufferSize,
    final ReplicationParam replication,    
    final ExcludeDatanodesParam exclDatanodes,
    final InputStream uploadedInputStream,
    final FormDataContentDisposition fileDetail,
    final String destination) throws IOException, URISyntaxException {
        final Configuration conf = (Configuration)context.getAttribute(JspHelper.CURRENT_CONF);
        final NameNode namenode = (NameNode)context.getAttribute("name.node");                
        switch(op.getValue()) {
            case UPLOAD:{
                String filename = fileDetail.getFileName();
                if(filename.equals("") || (filename == null)){
                    return Response.serverError().status(Status.BAD_REQUEST).build();                    
                }
                else if((fullpath == null) || (fullpath.equals(""))){
                    return Response.serverError().status(Status.BAD_REQUEST).build();
                }
                else{
                    LocalFileParam localFile = new LocalFileParam(filename);
                    // get redirect uri for datanode in which file should be created
                    final URI uri = redirectURIforLocal(namenode, ugi, delegation, username,
                        doAsUser, fullpath, op.getValue(), -1L, blockSize.getValue(conf),
                        exclDatanodes.getValue(), localFile, permission, overwrite, bufferSize,
                        replication, blockSize);
                    
                    FileSystem fs = FileSystem.get(conf);
                    // check for whitespaces - if they exist replace with '_'
                    if(filename.contains(" ")){
                        filename = filename.replace(" ", "_");
                    }
                    else if(filename.contains("+")){
                        filename = filename.replace("+", "_");
                    }
                    else if(filename.contains("%20")){
                        filename = filename.replace("%20", "_");
                    }
                    String pathForFile = fullpath + "/" + filename;
                    final URI destUri = new URI("hdfs", new NamenodeAddressParam(namenode).getValue(), pathForFile, null, null);
                    org.apache.hadoop.fs.Path destPath = new org.apache.hadoop.fs.Path(destUri.toString());
                    OutputStream out = fs.create(destPath);
                    int read = 0;
			        byte[] bytes = new byte[1024];
                    while ((read = uploadedInputStream.read(bytes)) != -1) {
                        out.write(bytes, 0, read);
                    }                    
                    out.flush();
			        out.close();
                    
                    return Response.created(uri).type(MediaType.APPLICATION_JSON).build();
                }                
            }
            default:
                throw new UnsupportedOperationException(op + " is not supported");
    }
  }


  private Response post(
      final UserGroupInformation ugi,
      final DelegationParam delegation,
      final UserParam username,
      final DoAsParam doAsUser,
      final String fullpath,
      final PostOpParam op,
      final ConcatSourcesParam concatSrcs,
      final BufferSizeParam bufferSize,
      final ExcludeDatanodesParam excludeDatanodes,
      final NewLengthParam newLength
      ) throws IOException, URISyntaxException {
    final NameNode namenode = (NameNode)context.getAttribute("name.node");
    final NamenodeProtocols np = getRPCServer(namenode);

    switch(op.getValue()) {
    case APPEND:
    {
      final URI uri = redirectURI(namenode, ugi, delegation, username,
          doAsUser, fullpath, op.getValue(), -1L, -1L,
          excludeDatanodes.getValue(), bufferSize);
      return Response.temporaryRedirect(uri).type(MediaType.APPLICATION_OCTET_STREAM).build();
    }
    case CONCAT:
    {
      np.concat(fullpath, concatSrcs.getAbsolutePaths());
      return Response.ok().build();
    }
    case TRUNCATE:
    {
      // We treat each rest request as a separate client.
      final boolean b = np.truncate(fullpath, newLength.getValue(), 
          "DFSClient_" + DFSUtil.getSecureRandom().nextLong());
      final String js = JsonUtil.toJsonString("boolean", b);
      return Response.ok(js).type(MediaType.APPLICATION_JSON).build();
    }    
    default:
      throw new UnsupportedOperationException(op + " is not supported");
    }
  }

  /** Handle HTTP GET request for the root. */
  @GET
  @Path("/")
  @Produces({MediaType.APPLICATION_OCTET_STREAM, MediaType.APPLICATION_JSON})
  public Response getRoot(
      @Context final UserGroupInformation ugi,
      @QueryParam(DelegationParam.NAME) @DefaultValue(DelegationParam.DEFAULT)
          final DelegationParam delegation,
      @QueryParam(UserParam.NAME) @DefaultValue(UserParam.DEFAULT)
          final UserParam username,
      @QueryParam(DoAsParam.NAME) @DefaultValue(DoAsParam.DEFAULT)
          final DoAsParam doAsUser,
      @QueryParam(GetOpParam.NAME) @DefaultValue(GetOpParam.DEFAULT)
          final GetOpParam op,
      @QueryParam(OffsetParam.NAME) @DefaultValue(OffsetParam.DEFAULT)
          final OffsetParam offset,
      @QueryParam(LengthParam.NAME) @DefaultValue(LengthParam.DEFAULT)
          final LengthParam length,
      @QueryParam(RenewerParam.NAME) @DefaultValue(RenewerParam.DEFAULT)
          final RenewerParam renewer,
      @QueryParam(BufferSizeParam.NAME) @DefaultValue(BufferSizeParam.DEFAULT)
          final BufferSizeParam bufferSize,
      @QueryParam(XAttrNameParam.NAME) @DefaultValue(XAttrNameParam.DEFAULT) 
          final List<XAttrNameParam> xattrNames,
      @QueryParam(XAttrEncodingParam.NAME) @DefaultValue(XAttrEncodingParam.DEFAULT) 
          final XAttrEncodingParam xattrEncoding,
      @QueryParam(ExcludeDatanodesParam.NAME) @DefaultValue(ExcludeDatanodesParam.DEFAULT)
          final ExcludeDatanodesParam excludeDatanodes,
      @QueryParam(FsActionParam.NAME) @DefaultValue(FsActionParam.DEFAULT)
          final FsActionParam fsAction,
      @QueryParam(TokenKindParam.NAME) @DefaultValue(TokenKindParam.DEFAULT)
          final TokenKindParam tokenKind,
      @QueryParam(TokenServiceParam.NAME) @DefaultValue(TokenServiceParam.DEFAULT)
          final TokenServiceParam tokenService
      ) throws IOException, InterruptedException {
    return get(ugi, delegation, username, doAsUser, ROOT, op, offset, length,
        renewer, bufferSize, xattrNames, xattrEncoding, excludeDatanodes, fsAction,
        tokenKind, tokenService);
  }

  /** Handle HTTP GET request. */
  @GET
  @Path("{" + UriFsPathParam.NAME + ":.*}")
  @Produces({MediaType.APPLICATION_OCTET_STREAM, MediaType.APPLICATION_JSON})
  public Response get(
      @Context final UserGroupInformation ugi,
      @QueryParam(DelegationParam.NAME) @DefaultValue(DelegationParam.DEFAULT)
          final DelegationParam delegation,
      @QueryParam(UserParam.NAME) @DefaultValue(UserParam.DEFAULT)
          final UserParam username,
      @QueryParam(DoAsParam.NAME) @DefaultValue(DoAsParam.DEFAULT)
          final DoAsParam doAsUser,
      @PathParam(UriFsPathParam.NAME) final UriFsPathParam path,
      @QueryParam(GetOpParam.NAME) @DefaultValue(GetOpParam.DEFAULT)
          final GetOpParam op,
      @QueryParam(OffsetParam.NAME) @DefaultValue(OffsetParam.DEFAULT)
          final OffsetParam offset,
      @QueryParam(LengthParam.NAME) @DefaultValue(LengthParam.DEFAULT)
          final LengthParam length,
      @QueryParam(RenewerParam.NAME) @DefaultValue(RenewerParam.DEFAULT)
          final RenewerParam renewer,
      @QueryParam(BufferSizeParam.NAME) @DefaultValue(BufferSizeParam.DEFAULT)
          final BufferSizeParam bufferSize,
      @QueryParam(XAttrNameParam.NAME) @DefaultValue(XAttrNameParam.DEFAULT) 
          final List<XAttrNameParam> xattrNames,
      @QueryParam(XAttrEncodingParam.NAME) @DefaultValue(XAttrEncodingParam.DEFAULT) 
          final XAttrEncodingParam xattrEncoding,
      @QueryParam(ExcludeDatanodesParam.NAME) @DefaultValue(ExcludeDatanodesParam.DEFAULT)
          final ExcludeDatanodesParam excludeDatanodes,
      @QueryParam(FsActionParam.NAME) @DefaultValue(FsActionParam.DEFAULT)
          final FsActionParam fsAction,
      @QueryParam(TokenKindParam.NAME) @DefaultValue(TokenKindParam.DEFAULT)
          final TokenKindParam tokenKind,
      @QueryParam(TokenServiceParam.NAME) @DefaultValue(TokenServiceParam.DEFAULT)
          final TokenServiceParam tokenService
      ) throws IOException, InterruptedException {

    init(ugi, delegation, username, doAsUser, path, op, offset, length,
        renewer, bufferSize, xattrEncoding, excludeDatanodes, fsAction,
        tokenKind, tokenService);

    return ugi.doAs(new PrivilegedExceptionAction<Response>() {
      @Override
      public Response run() throws IOException, URISyntaxException {
        try {
          return get(ugi, delegation, username, doAsUser,
              path.getAbsolutePath(), op, offset, length, renewer, bufferSize,
              xattrNames, xattrEncoding, excludeDatanodes, fsAction, tokenKind,
              tokenService);
        } finally {
          reset();
        }
      }
    });
  }

  private Response get(
      final UserGroupInformation ugi,
      final DelegationParam delegation,
      final UserParam username,
      final DoAsParam doAsUser,
      final String fullpath,
      final GetOpParam op,
      final OffsetParam offset,
      final LengthParam length,
      final RenewerParam renewer,
      final BufferSizeParam bufferSize,
      final List<XAttrNameParam> xattrNames,
      final XAttrEncodingParam xattrEncoding,
      final ExcludeDatanodesParam excludeDatanodes,
      final FsActionParam fsAction,
      final TokenKindParam tokenKind,
      final TokenServiceParam tokenService
      ) throws IOException, URISyntaxException {
    final NameNode namenode = (NameNode)context.getAttribute("name.node");
    final NamenodeProtocols np = getRPCServer(namenode);

    switch(op.getValue()) {
    case OPEN:
    {
      final URI uri = redirectURI(namenode, ugi, delegation, username,
          doAsUser, fullpath, op.getValue(), offset.getValue(), -1L,
          excludeDatanodes.getValue(), offset, length, bufferSize);
      return Response.temporaryRedirect(uri).type(MediaType.APPLICATION_OCTET_STREAM).build();
    }    
    case GET_BLOCK_LOCATIONS:
    {
      final long offsetValue = offset.getValue();
      final Long lengthValue = length.getValue();
      final LocatedBlocks locatedblocks = np.getBlockLocations(fullpath,
          offsetValue, lengthValue != null? lengthValue: Long.MAX_VALUE);
      final String js = JsonUtil.toJsonString(locatedblocks);
      return Response.ok(js).type(MediaType.APPLICATION_JSON).build();
    }
    case GETFILESTATUS:
    {
      final HdfsFileStatus status = np.getFileInfo(fullpath);
      if (status == null) {
        throw new FileNotFoundException("File does not exist: " + fullpath);
      }

      final String js = JsonUtil.toJsonString(status, true);
      return Response.ok(js).type(MediaType.APPLICATION_JSON).build();
    }
    case LISTSTATUS:
    {
      final StreamingOutput streaming = getListingStream(np, fullpath);
      return Response.ok(streaming).type(MediaType.APPLICATION_JSON).build();
    }
    case GETCONTENTSUMMARY:
    {
      final ContentSummary contentsummary = np.getContentSummary(fullpath);
      final String js = JsonUtil.toJsonString(contentsummary);
      return Response.ok(js).type(MediaType.APPLICATION_JSON).build();
    }
    case GETFILECHECKSUM:
    {
      final URI uri = redirectURI(namenode, ugi, delegation, username, doAsUser,
          fullpath, op.getValue(), -1L, -1L, null);
      return Response.temporaryRedirect(uri).type(MediaType.APPLICATION_OCTET_STREAM).build();
    }
    case GETDELEGATIONTOKEN:
    {
      if (delegation.getValue() != null) {
        throw new IllegalArgumentException(delegation.getName()
            + " parameter is not null.");
      }
      final Token<? extends TokenIdentifier> token = generateDelegationToken(
          namenode, ugi, renewer.getValue());

      final String setServiceName = tokenService.getValue();
      final String setKind = tokenKind.getValue();
      if (setServiceName != null) {
        token.setService(new Text(setServiceName));
      }
      if (setKind != null) {
        token.setKind(new Text(setKind));
      }
      final String js = JsonUtil.toJsonString(token);
      return Response.ok(js).type(MediaType.APPLICATION_JSON).build();
    }
    case GETHOMEDIRECTORY:
    {
      final String js = JsonUtil.toJsonString(
          org.apache.hadoop.fs.Path.class.getSimpleName(),
          WebHdfsFileSystem.getHomeDirectoryString(ugi));
      return Response.ok(js).type(MediaType.APPLICATION_JSON).build();
    }
    case GETACLSTATUS: {
      AclStatus status = np.getAclStatus(fullpath);
      if (status == null) {
        throw new FileNotFoundException("File does not exist: " + fullpath);
      }

      final String js = JsonUtil.toJsonString(status);
      return Response.ok(js).type(MediaType.APPLICATION_JSON).build();
    }
    case GETXATTRS: {
      List<String> names = null;
      if (xattrNames != null) {
        names = Lists.newArrayListWithCapacity(xattrNames.size());
        for (XAttrNameParam xattrName : xattrNames) {
          if (xattrName.getXAttrName() != null) {
            names.add(xattrName.getXAttrName());
          }
        }
      }
      List<XAttr> xAttrs = np.getXAttrs(fullpath, (names != null && 
          !names.isEmpty()) ? XAttrHelper.buildXAttrs(names) : null);
      final String js = JsonUtil.toJsonString(xAttrs,
          xattrEncoding.getEncoding());
      return Response.ok(js).type(MediaType.APPLICATION_JSON).build();
    }
    case LISTXATTRS: {
      final List<XAttr> xAttrs = np.listXAttrs(fullpath);
      final String js = JsonUtil.toJsonString(xAttrs);
      return Response.ok(js).type(MediaType.APPLICATION_JSON).build();
    }
    case CHECKACCESS: {
      np.checkAccess(fullpath, FsAction.getFsAction(fsAction.getValue()));
      return Response.ok().build();
    }
    default:
      throw new UnsupportedOperationException(op + " is not supported");
    }
  }

  private static DirectoryListing getDirectoryListing(final NamenodeProtocols np,
      final String p, byte[] startAfter) throws IOException {
    final DirectoryListing listing = np.getListing(p, startAfter, false);
    if (listing == null) { // the directory does not exist
      throw new FileNotFoundException("File " + p + " does not exist.");
    }
    return listing;
  }
  
  private static StreamingOutput getListingStream(final NamenodeProtocols np, 
      final String p) throws IOException {
    // allows exceptions like FNF or ACE to prevent http response of 200 for
    // a failure since we can't (currently) return error responses in the
    // middle of a streaming operation
    final DirectoryListing firstDirList = getDirectoryListing(np, p,
        HdfsFileStatus.EMPTY_NAME);

    // must save ugi because the streaming object will be executed outside
    // the remote user's ugi
    final UserGroupInformation ugi = UserGroupInformation.getCurrentUser();
    return new StreamingOutput() {
      @Override
      public void write(final OutputStream outstream) throws IOException {
        final PrintWriter out = new PrintWriter(new OutputStreamWriter(
            outstream, Charsets.UTF_8));
        out.println("{\"" + FileStatus.class.getSimpleName() + "es\":{\""
            + FileStatus.class.getSimpleName() + "\":[");

        try {
          // restore remote user's ugi
          ugi.doAs(new PrivilegedExceptionAction<Void>() {
            @Override
            public Void run() throws IOException {
              long n = 0;
              for (DirectoryListing dirList = firstDirList; ;
                   dirList = getDirectoryListing(np, p, dirList.getLastName())
              ) {
                // send each segment of the directory listing
                for (HdfsFileStatus s : dirList.getPartialListing()) {
                  if (n++ > 0) {
                    out.println(',');
                  }
                  out.print(JsonUtil.toJsonString(s, false));
                }
                // stop if last segment
                if (!dirList.hasMore()) {
                  break;
                }
              }
              return null;
            }
          });
        } catch (InterruptedException e) {
          throw new IOException(e);
        }
        
        out.println();
        out.println("]}}");
        out.flush();
      }
    };
  }

  /** Handle HTTP DELETE request for the root. */
  @DELETE
  @Path("/")
  @Produces(MediaType.APPLICATION_JSON)
  public Response deleteRoot(
      @Context final UserGroupInformation ugi,
      @QueryParam(DelegationParam.NAME) @DefaultValue(DelegationParam.DEFAULT)
          final DelegationParam delegation,
      @QueryParam(UserParam.NAME) @DefaultValue(UserParam.DEFAULT)
          final UserParam username,
      @QueryParam(DoAsParam.NAME) @DefaultValue(DoAsParam.DEFAULT)
          final DoAsParam doAsUser,
      @QueryParam(DeleteOpParam.NAME) @DefaultValue(DeleteOpParam.DEFAULT)
          final DeleteOpParam op,
      @QueryParam(RecursiveParam.NAME) @DefaultValue(RecursiveParam.DEFAULT)
          final RecursiveParam recursive,
      @QueryParam(SnapshotNameParam.NAME) @DefaultValue(SnapshotNameParam.DEFAULT)
          final SnapshotNameParam snapshotName
      ) throws IOException, InterruptedException {
    return delete(ugi, delegation, username, doAsUser, ROOT, op, recursive,
        snapshotName);
  }

  /** Handle HTTP DELETE request. */
  @DELETE
  @Path("{" + UriFsPathParam.NAME + ":.*}")
  @Produces(MediaType.APPLICATION_JSON)
  public Response delete(
      @Context final UserGroupInformation ugi,
      @QueryParam(DelegationParam.NAME) @DefaultValue(DelegationParam.DEFAULT)
          final DelegationParam delegation,
      @QueryParam(UserParam.NAME) @DefaultValue(UserParam.DEFAULT)
          final UserParam username,
      @QueryParam(DoAsParam.NAME) @DefaultValue(DoAsParam.DEFAULT)
          final DoAsParam doAsUser,
      @PathParam(UriFsPathParam.NAME) final UriFsPathParam path,
      @QueryParam(DeleteOpParam.NAME) @DefaultValue(DeleteOpParam.DEFAULT)
          final DeleteOpParam op,
      @QueryParam(RecursiveParam.NAME) @DefaultValue(RecursiveParam.DEFAULT)
          final RecursiveParam recursive,
      @QueryParam(SnapshotNameParam.NAME) @DefaultValue(SnapshotNameParam.DEFAULT)
          final SnapshotNameParam snapshotName
      ) throws IOException, InterruptedException {

    init(ugi, delegation, username, doAsUser, path, op, recursive, snapshotName);

    return ugi.doAs(new PrivilegedExceptionAction<Response>() {
      @Override
      public Response run() throws IOException {
        try {
          return delete(ugi, delegation, username, doAsUser,
              path.getAbsolutePath(), op, recursive, snapshotName);
        } finally {
          reset();
        }
      }
    });
  }

  private Response delete(
      final UserGroupInformation ugi,
      final DelegationParam delegation,
      final UserParam username,
      final DoAsParam doAsUser,
      final String fullpath,
      final DeleteOpParam op,
      final RecursiveParam recursive,
      final SnapshotNameParam snapshotName
      ) throws IOException {
    final NameNode namenode = (NameNode)context.getAttribute("name.node");
    final NamenodeProtocols np = getRPCServer(namenode);

    switch(op.getValue()) {
    case DELETE: {
      final boolean b = np.delete(fullpath, recursive.getValue());
      final String js = JsonUtil.toJsonString("boolean", b);
      return Response.ok(js).type(MediaType.APPLICATION_JSON).build();
    }
    case DELETESNAPSHOT: {
      np.deleteSnapshot(fullpath, snapshotName.getValue());
      return Response.ok().type(MediaType.APPLICATION_OCTET_STREAM).build();
    }
    default:
      throw new UnsupportedOperationException(op + " is not supported");
    }
  }
}
