/* Copyright (c) 2016, 2020, Oracle and/or its affiliates. All rights reserved. */

package waggle.server.integration.docs;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import oracle.stellent.ridc.IdcClient;
import oracle.stellent.ridc.IdcClientConfig;
import oracle.stellent.ridc.IdcClientException;
import oracle.stellent.ridc.IdcClientManager;
import oracle.stellent.ridc.IdcContext;
import oracle.stellent.ridc.common.http.utils.RIDCHttpConstants.HttpLibrary;
import oracle.stellent.ridc.model.DataBinder;
import oracle.stellent.ridc.model.DataObject;
import oracle.stellent.ridc.model.DataResultSet;
import oracle.stellent.ridc.model.TransferFile;
import oracle.stellent.ridc.protocol.Protocol;
import oracle.stellent.ridc.protocol.ServiceResponse;
import oracle.stellent.ridc.protocol.http.IdcHttpClientConfig;

import waggle.common.modules.conversation.enums.XConversationRole;
import waggle.common.modules.document.infos.XContentServerVersionInfo;
import waggle.common.modules.group.enums.XGroupOriginType;
import waggle.common.modules.group.enums.XGroupType;
import waggle.common.modules.group.infos.XGroupInfo;
import waggle.common.modules.member.infos.XMemberInfo;
import waggle.common.modules.search.infos.XContentServerSearchFilterInfo;
import waggle.common.modules.search.infos.XSearchResultInfo;
import waggle.common.modules.user.infos.XUserInfo;
import waggle.core.annotations.XDisallowInstantiation;
import waggle.core.api.XAPIInputStream;
import waggle.core.exceptions.XRuntimeException;
import waggle.core.http.XHTTPProxy;
import waggle.core.id.XObjectID;
import waggle.core.log.XLog;
import waggle.core.properties.XPropertiesManager;
import waggle.core.utils.XClass;
import waggle.core.utils.XCollections;
import waggle.core.utils.XContentServerUtil;
import waggle.core.utils.XFormat;
import waggle.core.utils.XString;
import waggle.server.accesscontrol.XAccessControlUtil;
import waggle.server.accesscontrol.infos.XResourceInfo;
import waggle.server.csf.XUsernamePasswordCredential;
import waggle.server.hybridlink.XHybridLinkPermissions;
import waggle.server.identity.utils.XIdentityUtil;
import waggle.server.integration.docs.enums.XGroupSyncAction;
import waggle.server.integration.docs.structs.XGroupSyncStruct;
import waggle.server.modules.group.database.group.XGroupObject;
import waggle.server.modules.group.database.group.XGroupObjectManager;
import waggle.server.modules.group.database.groups.XGroupsObjectManager;
import waggle.server.modules.member.database.member.XMemberObject;
import waggle.server.modules.user.database.user.XUserObject;
import waggle.server.modules.user.database.users.XUsersObjectManager;
import waggle.server.modules.user.utils.XUserUtils;
import waggle.server.servlet.session.XSessionManager;
import waggle.server.utils.XValidate;

import static waggle.common.modules.conversation.enums.XConversationRole.GROUP_MANAGER;
import static waggle.common.modules.conversation.enums.XConversationRole.GROUP_MEMBER;
import static waggle.common.modules.group.enums.XGroupType.*;

/**
 * Utility class for IDC implementation for invoking services in Content Server.
 */
@XDisallowInstantiation
public final class XIdcUtils
{
	private static final 	String SERVICE_AVATAR_UPDATE = "UPDATE_AVATAR";
	private static final 	String SERVICE_AVATAR_DELETE = "DELETE_USER_AVATAR";

	private static final 	String SERVICE_LOGO_UPDATE = "UPLOAD_TENANT_LOGO";
	private static final 	String SERVICE_LOGO_DELETE = "REMOVE_TENANT_LOGO";
	private static final 	String SERVICE_LOGO_PARAM = "logoPath";

	private static final 	String SERVICE_HYBRID_LINK = "CREATE_HYBRID_LINK";
	private static final 	String SERVICE_SITES_ACCESS = "SCS_CHECK_SITE_ACCESS_AND_ROLE";
	private static final	String CONTENT_SERVER_SEARCH_SERVICE = "GET_SEARCH_RESULTS";
	private static final 	String SERVICE_SCS_COPY_SITES = "SCS_COPY_SITES";
	private static final 	String SERVICE_SCS_GET_SITE_PART_GUID = "SCS_GET_SITE_PART_GUID";
	private static final 	String SERVICE_SCS_IMPORT_OOTB_TEMPLATES = "SCS_IMPORT_OOTB_TEMPLATES";
	private static final	String SERVICE_SCS_GET_BACKGROUND_SERVICE_JOB_STATUS = "SCS_GET_BACKGROUND_SERVICE_JOB_STATUS";
	private static final	String SERVICE_SCS_BROWSE_SITES = "SCS_BROWSE_SITES";
	private static final	String SERVICE_SCS_ACTIVATE_SITE = "SCS_ACTIVATE_SITE";
	private static final	String SERVICE_SCS_DEACTIVATE_SITE = "SCS_DEACTIVATE_SITE";
	private static final	String SERVICE_CREATE_GROUP = "CREATE_GROUP";
	private static final	String SERVICE_DELETE_GROUP = "DELETE_GROUP";
	private static final	String SERVICE_MODIFY_GROUP = "MODIFY_GROUP";
	private static final	String SERVICE_ADD_GROUP_MEMBERS = "ADD_GROUP_MEMBERS";
	private static final	String SERVICE_REMOVE_GROUP_MEMBERS = "REMOVE_GROUP_MEMBERS";
	private static final	String SERVICE_JOIN_GROUP = "JOIN_GROUP";
	private static final	String SERVICE_LEAVE_GROUP = "LEAVE_GROUP";
	private static final	String SERVICE_GRANT_GROUP_PRIVILEGE = "GRANT_GROUP_PRIVILEGE";
	private static final	String SERVICE_MODIFY_GROUP_PRIVILEGE = "MODIFY_GROUP_PRIVILEGE";
	private static final	String SERVICE_REVOKE_GROUP_PRIVILEGE = "REVOKE_GROUP_PRIVILEGE";
	// these are used for testing only
	private static final	String SERVICE_VIEW_GROUP_INFO = "VIEW_GROUP_INFO";
	private static final	String SERVICE_VIEW_GROUP_MEMBERS = "VIEW_GROUP_MEMBERS";
	private static final	String SERVICE_VIEW_GROUP_PRIVILEGES = "VIEW_GROUP_PRIVILEGES";
	private static final 	String SERVICE_FLD_INFO = "FLD_INFO";

	private static final 	String SERVICE_ASSET_INFO = "AR_ASSET_INFO";
	private static final 	String SERVICE_SHARED_REPO_USERS = "AR_GET_SHARED_REPOSITORY_USERS";
	private static final 	String SERVICE_SHARED_FOLDER_USERS = "GET_SHARED_FOLDER_USERS";

	private static final	int DEFAULT_PAGE_SIZE = 20;
	private static final	int STATUS_CODE_SUCCESS = 0;

	private static final 	String SERVICE_AVATAR_PARAM = "avatarPath";
	private static final 	String NOTIFY_OSN_PARAM = "notifyOSN";
	private static final 	String HYBRID_LINK_PARAM = "dLinkID";
	private static final 	String ASSIGNED_USERS_PREFIX = "#OSN:";
	private static final 	String ITEM_PREFIX = "fFileGUID:";
	private static final 	String FOLDER_PREFIX = "fFolderGUID:";
	private static final	String ACCESS_PERMISSION = "canAccess";
	private static final 	String DELETE_PERMISSION = "canDelete";

	private static final 	String SERVICE_TENANT_CONFIG_UPDATE = "SET_TENANT_CONFIG";
	private static final 	String TENANT_OPTION_NAME = "dTenantOptionName";
	private static final	String TENANT_OPTION_VALUE = "dTenantOptionValue";
	private static final	String FLD_INFO_FILE_VERSION_PARAM = "dLatestActiveRevisionID";
	private static final	String IDP_GROUP_TYPE = "idp";

	/**
	 * DoCS Impersonation user header.
	 */
	public static final String			IMPERSONATED_USER_HEADER = "X-IMPERSONATED-USER";

	private static final 	XLog sLogger = XLog.getLogger();

	private XIdcUtils()
	{
	}

	/**
	 * Get the IDC client object for invoking services in Content Server.
	 *
	 * @return The IdcClient object.
	 *
	 * @throws IdcClientException Exception during client creation.
	 */
	private static IdcClient getIdcClient() throws IdcClientException
	{
		String 						contentServerFullUrl = XContentServerUtil.getContentServerFullURL();

		if ( sLogger.isDebugEnabled() )
		{
			sLogger.debug( "Content Server Integration Full URL: {0}", contentServerFullUrl );
		}

		IdcClientManager 			manager = new IdcClientManager();
		IdcClient 					idcClient = manager.createClient( contentServerFullUrl );

		if ( sLogger.isDebugEnabled() )
		{
			sLogger.debug( "Created IDC client instance." );
		}

		IdcClientConfig				idcClientConfig	= idcClient.getConfig();
		idcClientConfig.setSocketTimeout( 300000 );

		boolean 					proxyIsSet = setProxyIfEnabled( idcClientConfig );

		if ( !proxyIsSet )
		{
			idcClient.getConfig().setProperty( "http.library", "apache4" );
		}

		return idcClient;
	}

	private static boolean setProxyIfEnabled( IdcClientConfig idcClientConfig )
	{
		// Assume the proxy is not set
		boolean		proxyIsSet = false;
		boolean		useProxy = XPropertiesManager.getInstance().getBoolean( "waggle.server.docsintegration.use.proxy", false );

		if ( useProxy && ( idcClientConfig instanceof IdcHttpClientConfig ) )
		{
			if ( sLogger.isDebugEnabled() )
			{
				sLogger.debug( "Checking if Oracle Documents proxy is enabled." );
			}

			try
			{
				if ( XString.isNotBlank( XHTTPProxy.getProxyHost() )
					 && ( XString.isNotBlank( String.valueOf( XHTTPProxy.getProxyPort() ) ) ) )
				{
					IdcHttpClientConfig httpClientConfig = (IdcHttpClientConfig) idcClientConfig;
					httpClientConfig.setHttpLibrary( HttpLibrary.httpurlconnection );

					if ( sLogger.isDebugEnabled() )
					{
						sLogger.debug( "Setting Proxy in IdC Client: {0}:{1}", XHTTPProxy.getProxyHost(),
									   XHTTPProxy.getProxyPort() );
					}

					httpClientConfig.setProxyHost( XHTTPProxy.getProxyHost() );
					httpClientConfig.setProxyPort( XHTTPProxy.getProxyPort() );
					httpClientConfig.setUseSystemProxy( false );
					proxyIsSet = true;
				}
			}
			catch ( XRuntimeException xre )
			{
				// Explicitly set this to false
				sLogger.warning( "Unable to parse Oracle Documents proxy settings, setting to default.", xre );
				proxyIsSet = false;
			}
		}

		return proxyIsSet;
	}

	/**
	 * Get the APP ID user name and password credentials as an IdcContext object.
	 *
	 * @return The IdcContext instance.
	 */
	private static IdcContext getIdcContext()
	{
		XUsernamePasswordCredential credentials = XIdentityUtil.getIdentityUserCredential();

		XValidate.argumentNotNull( "Credentials", credentials );

		String 						osnAppIdUser = credentials.getName();
		String 						osnAppIdPassword = credentials.getPassword().toClearTextString();

		// TODO: remove this - Devang
		//String osnAppIdUser = "swebcli.INTEGRATION_USER";
		//String osnAppIdPassword = "welcome1";

		XValidate.argumentNotEmpty( "ApplicationID", osnAppIdUser );
		XValidate.argumentNotEmpty( "Password", osnAppIdPassword );

		if ( sLogger.isDebugEnabled() )
		{
			sLogger.debug( "OSN App ID User: {0}", osnAppIdUser );
		}

		return new IdcContext( osnAppIdUser, osnAppIdPassword );
	}

	/**
	 * Get the data binder for the specified service name.
	 * This binder contains the header for carrying out impersonation for the logged in user.
	 *
	 * @param idcClient The IdcClient instance.
	 * @param serviceName The Remote Service to be invoked.
	 *
	 * @return The binder object.
	 */
	private static DataBinder getDataBinder( final IdcClient idcClient, final String serviceName )
	{
		return getDataBinder( idcClient, serviceName, XSessionManager.getUserObject() );
	}

	/**
	 * Get the data binder for the specified service name.
	 * This binder contains the header for carrying out impersonation for the logged in user.
	 *
	 * @param idcClient The IdcClient instance.
	 * @param serviceName The Remote Service to be invoked.
	 * @param userObject The User executing this operation.
	 *
	 * @return The binder object.
	 */
	private static DataBinder getDataBinder( final IdcClient idcClient, final String serviceName, final XUserObject userObject )
	{
		XUserObject 	currentUserObject = userObject;

		if ( currentUserObject == null )
		{
			currentUserObject = XSessionManager.getUserObject();
		}

		String 			currentUserLoginName = null;

		if ( currentUserObject != null )
		{
			currentUserLoginName = currentUserObject.getName();
		}

		//currentUserLoginName = "swebcli.Admin2";

		DataBinder 		binder = idcClient.createBinder();

		// Add the impersonated user login name to the request header.

		binder.putLocal( Protocol.IDC_HEADER_PREFIX + IMPERSONATED_USER_HEADER, currentUserLoginName );
		binder.putLocal( "IdcService", serviceName );

		if ( sLogger.isDebugEnabled() )
		{
			sLogger.debug( "Current Logged in user (LoginName) = Impersonated User: {0}", currentUserLoginName );
		}

		return binder;
	}

	private static DataBinder getDataBinderForIDPGroups( final IdcClient idcClient, final String serviceName )
	{
		DataBinder 		binder = idcClient.createBinder();

		// Add the impersonated user login name to the request header.

		binder.putLocal( Protocol.IDC_HEADER_PREFIX + IMPERSONATED_USER_HEADER, "docadmin" );
		binder.putLocal( "IdcService", serviceName );

		return binder;
	}

	/**
	 * Executes the IDC request, checks response to be of binder type and returns it.
	 *
	 * @param idcClient 			The IdcClient instance.
	 * @param userContext 			The IdcContext instance.
	 * @param binder				The DataBinder instance.
	 * @param exceptionResourceId 	The exception resource ID. (Useful for logging purpose).
	 * @return						The ServiceResponse object on invoking the IDC request.
	 * @throws IdcClientException
	 */
	private static ServiceResponse execute(
		final IdcClient 	idcClient,
		final IdcContext 	userContext,
		final DataBinder 	binder,
		final String 		exceptionResourceId,
		final Object...		exceptionArguments ) throws IdcClientException
	{
		ServiceResponse response = null;

		if ( ( idcClient != null ) && ( userContext != null ) && ( binder != null ) )
		{
			response = idcClient.sendRequest( userContext, binder );

			if ( ( response == null ) || ( !response.getResponseType().equals( ServiceResponse.ResponseType.BINDER ) ) )
			{
				if ( response == null )
				{
					sLogger.error( "IDC request for Service - {0} returned null ServiceResponse." );
				}
				else
				{
					sLogger.error( "IDC request for Service - {0} returned a ServiceResponse of type: {1}. Expected BINDER type.",
								   binder.getLocal( "IdcService" ), response.getResponseType().name() );
				}

				throw new XRuntimeException( exceptionResourceId, exceptionArguments );
			}

			if ( sLogger.isDebugEnabled() )
			{
				sLogger.debug( "Response from Content Server: {0}", response.getResponseAsBinder().toString() );
			}
		}
		else
		{
			if ( sLogger.isDebugEnabled() )
			{
				sLogger.debug( "Some or all arguments for XIdcUtils.execute method were null. Skipped executing IDC request." );
			}
		}

		return response;
	}

	/**
	 * Updates the Avatar of the user in Content Server. Uses the UPDATE_AVATAR service.
	 *
	 * @param contentStream The image input stream.
	 * @param fileName The file name with extension.
	 * @param contentLength Length of the file in bytes.
	 * @param contentType The mime type.
	 */
	public static void updateAvatar(
		final XAPIInputStream	contentStream,
		final String 			fileName,
		final long 				contentLength,
		final String 			contentType )
	{
		ServiceResponse 	response = null;

		try
		{
			IdcClient 		idcClient = getIdcClient();
			IdcContext 		userContext = getIdcContext();
			DataBinder 		binder = getDataBinder( idcClient, SERVICE_AVATAR_UPDATE );

			binder.addFile( SERVICE_AVATAR_PARAM, new TransferFile( contentStream, fileName, contentLength, contentType ) );

			// We need to set NOTIFY_OSN_PARAM flag to "0" (false) so that DoCS does not call OSN back for updating user avatar.

			binder.putLocal( NOTIFY_OSN_PARAM, "0" );

			if ( sLogger.isDebugEnabled() )
			{
				sLogger.debug( "Binder object in the IDC request: {0}", binder.toString() );
			}

			response = execute( idcClient, userContext, binder, "waggle.server.integration.docs.idc.CouldNotUpdateProfilePicCS" );
		}
		catch ( Throwable ex )
		{
			sLogger.error( "Exception in invoking {0} service", SERVICE_AVATAR_UPDATE, ex );

			throw new XRuntimeException( "waggle.server.integration.docs.idc.CouldNotUpdateProfilePicCS", ex );
		}
		finally
		{
			if ( response != null )
			{
				response.close();
			}
		}
	}

	/**
	 * Deletes / Removes the avatar for the logged in user. Uses the DELETE_USER_AVATAR IDC Service.
	 */
	public static void deleteAvatar()
	{
		ServiceResponse 	response = null;

		try
		{
			IdcClient 		idcClient = getIdcClient();
			IdcContext 		userContext = getIdcContext();
			DataBinder 		binder = getDataBinder( idcClient, SERVICE_AVATAR_DELETE );

			if ( sLogger.isDebugEnabled() )
			{
				sLogger.debug( "Binder object in the IDC request: {0}", binder.toString() );
			}

			response = execute( idcClient, userContext, binder, "waggle.server.integration.docs.idc.CouldNotDeleteProfilePicCS" );
		}
		catch ( Throwable ex )
		{
			sLogger.error( "Exception in invoking {0} service", SERVICE_AVATAR_DELETE, ex );

			throw new XRuntimeException( "waggle.server.integration.docs.idc.CouldNotDeleteProfilePicCS", ex );
		}
		finally
		{
			if ( response != null )
			{
				response.close();
			}
		}
	}

	/**
	 * Fetches the Hybrid / Shared Link from Content Server using CREATE_HYBRID_LINK IDC service.
	 *
	 * @param contentServerGUID 	The GUID of the document in Content Server.
	 * @param conversationID 		The ID of the conversation that contains / will contain the document.
	 * @return 						The String ID of the Hybrid Link. Clients will use this ID to create the URL.
	 */
	public static String fetchHybridLink(
		final String 	contentServerGUID,
		final String 	conversationID )
	{
		ServiceResponse 	response = null;
		String				retVal = null;

		try
		{
			IdcClient 		idcClient = getIdcClient();
			IdcContext 		userContext = getIdcContext();
			DataBinder 		binder = getDataBinder( idcClient, SERVICE_HYBRID_LINK );

			binder.putLocal( "item", ITEM_PREFIX + contentServerGUID );
			binder.putLocal( "dAssignedUsers", ASSIGNED_USERS_PREFIX + conversationID );

			if ( sLogger.isDebugEnabled() )
			{
				sLogger.debug( "Binder object in the IDC request for SERVICE_HYBRID_LINK: {0}", binder.toString() );
			}

			response = execute( idcClient, userContext, binder, "waggle.server.integration.docs.idc.CouldNotFetchHybridLink" );

			DataBinder respBinder = response.getResponseAsBinder();

			if ( respBinder.getLocalData() != null )
			{
				retVal = respBinder.getLocalData().get( HYBRID_LINK_PARAM );
			}
			else
			{
				sLogger.warning( "IDC call: {0} to Content Server returned null local data. Could not retrieve Hybrid link." );
			}
		}
		catch ( Throwable ex )
		{
			sLogger.error( "Exception in invoking {0} service", SERVICE_HYBRID_LINK, ex );

			throw new XRuntimeException( "waggle.server.integration.docs.idc.CouldNotFetchHybridLink", ex );
		}
		finally
		{
			if ( response != null )
			{
				response.close();
			}
		}

		if ( sLogger.isDebugEnabled() )
		{
			sLogger.debug( "Hybrid / Shared Link received from Content Server: {0}", retVal );
		}

		return retVal;
	}

	/**
	 * Checks the access of current user for the site by using content server's service SCS_CHECK_SITE_ACCESS_AND_ROLE.
	 *
	 * @param sitesFolderGUID, ID of the sites folder for which the access to be checked.
	 *
	 * @return Permission values for current user.
	 */
	public static XHybridLinkPermissions checkSitesAccess(
		final String 		sitesFolderGUID )
	{
		return checkSitesAccess( sitesFolderGUID, XSessionManager.getUserObject() );
	}


	/**
	 * Checks the access of specified user for the site by using content server's service SCS_CHECK_SITE_ACCESS_AND_ROLE.
	 *
	 * @param sitesFolderGUID, ID of the sites folder for which the access to be checked.
	 * @param userObject, User whose access to be checked.
	 *
	 * @return Permission values for the specified user.
	 */
	public static XHybridLinkPermissions checkSitesAccess(
		final String 		sitesFolderGUID,
		final XUserObject	userObject )
	{
		ServiceResponse 		response = null;
		XHybridLinkPermissions 	retval = new XHybridLinkPermissions();

		try
		{
			IdcClient 			idcClient = getIdcClient();
			IdcContext 			userContext = getIdcContext();
			DataBinder 			binder = getDataBinder( idcClient, SERVICE_SITES_ACCESS, userObject );

			binder.putLocal( "item",
					sitesFolderGUID.startsWith( FOLDER_PREFIX ) ? sitesFolderGUID : FOLDER_PREFIX + sitesFolderGUID );

			if ( sLogger.isDebugEnabled() )
			{
				sLogger.debug( "Binder object in the IDC request for SERVICE_SITES_ACCESS: {0}", binder.toString() );
			}

			response = execute( idcClient, userContext, binder, "waggle.server.integration.docs.idc.CouldNotCheckSitesAccess" );

			DataBinder 			responseBinder = response.getResponseAsBinder();

			if ( ( responseBinder != null ) && ( responseBinder.getLocalData() != null ) )
			{
				 DataObject localData = responseBinder.getLocalData();

				if ( localData.getInteger( ACCESS_PERMISSION ) == 1 )
				{
					retval.setAccess( true );
				}

				if ( localData.getInteger( DELETE_PERMISSION ) == 1 )
				{
					retval.setDelete( true );

					// Delete permission needs the user to have contributor role in SITES. If the user has this role, he can
					// create / copy the hybrid links also.

					retval.setCreate( true );
				}
			}
			else
			{
				sLogger.warning( "Call to the Content Server returned null local data. Could not retrieve the permission." );
			}

		}
		catch ( Throwable ex )
		{
			sLogger.error( "Exception in invoking {0} service", SERVICE_SITES_ACCESS, ex );

			throw new XRuntimeException( "waggle.server.integration.docs.idc.CouldNotCheckSitesAccess", ex );
		}
		finally
		{
			if ( response != null )
			{
				response.close();
			}
		}

		return retval;
	}

	/**
	 * Carry out document and folder search in Content Server using RIDC protocol.
	 *
	 * @param filterInfo Search criteria.
	 * @return Matching results.
	 */
	public static List<XSearchResultInfo> searchDocumentsAndFoldersInContentServer( XContentServerSearchFilterInfo filterInfo, final String sortField, final boolean isSearchShared )
	{
		List<XSearchResultInfo> searchResults = new ArrayList<>();
		String 					queryText = "<ftx>" + filterInfo.SearchString + "</ftx>";
		String					folderList = null;

		// Build a comma separated string of folderGUIDs to which the search should be limited.

		if ( XCollections.isNotEmpty( filterInfo.FolderGUIDs ) )
		{
			StringBuilder 		builder = new StringBuilder();

			for ( String folderGUID : filterInfo.FolderGUIDs )
			{
				if ( builder.length() > 0 )
				{
					builder.append( "," );
				}

				builder.append( FOLDER_PREFIX ).append( folderGUID );
			}

			folderList = builder.toString();
		}

		if ( sLogger.isDebugEnabled() )
		{
			sLogger.debug( "Search query text: {0}", queryText );
			sLogger.debug( "Sort Field: {0}", sortField );
			sLogger.debug( "Invoking {0} on Content Server to carry out Federated Search.", CONTENT_SERVER_SEARCH_SERVICE );

			if ( XString.isNotBlank( folderList ) )
			{
				sLogger.debug( "Limiting the search to folders {0}.", folderList );
			}
		}

		DataResultSet			resultSet;
		ServiceResponse 		response = null;

		try
		{
			IdcClient 			idcClient = getIdcClient();
			IdcContext 			userContext = getIdcContext();
			DataBinder 			binder = getDataBinder( idcClient, CONTENT_SERVER_SEARCH_SERVICE );

			// Required parameters for search in DoCS.

			binder.putLocal( "QueryText", queryText );

			// Other optional parameters

			if ( XString.isNotBlank( folderList ) )
			{
				binder.putLocal( "items", folderList );
			}

			if ( XString.isNotBlank( sortField ) )
			{
				binder.putLocal( "SortField", sortField );
			}

			if ( !filterInfo.SortOrderDescending )
			{
				binder.putLocal( "SortOrder", "ASC" );
			}

			if ( isSearchShared )
			{
				binder.putLocal( "IsSearchShared", Boolean.toString( isSearchShared ) );
			}

			binder.putLocal( "ResultCount", Integer.toString( filterInfo.NumResults ) );

			// The Content Server API requires the StartRow to start from 1. FirstResult field in filterInfo starts from 0.

			int 			startRow = filterInfo.FirstResult + 1;
			binder.putLocal( "StartRow", Integer.toString( startRow ) );


			response = execute( idcClient, userContext, binder, "waggle.server.integration.docs.idc.CouldNotPerformContentSearch" );

			DataBinder serverBinder = response.getResponseAsBinder();

			if ( sLogger.isDebugEnabled() )
			{
				sLogger.debug( "Response data binder: {0}", serverBinder.toString() );
			}

			resultSet = serverBinder.getResultSet( "SearchResults" );

			if ( resultSet == null )
			{
				if ( sLogger.isDebugEnabled() )
				{
					sLogger.debug( "No search hits." );
				}

				return searchResults;
			}

			if ( sLogger.isDebugEnabled() )
			{
				sLogger.debug( "Search executed on Content Server. Result Count = {0}", resultSet.getRows().size() );
			}

			for ( DataObject dataObject : resultSet.getRows() )
			{
				XContentServerVersionInfo docsVersionInfo = new XContentServerVersionInfo();

				docsVersionInfo.ItemGUID = dataObject.get( "fItemGUID" );
				docsVersionInfo.Name = dataObject.get( "fItemName" );
				docsVersionInfo.ObjectType = dataObject.get( "fItemType" );
				docsVersionInfo.ParentGUID = dataObject.get( "fParentGUID" );

				docsVersionInfo.CreatedTimestamp = dataObject.getDate( "fCreateDate" );
				docsVersionInfo.ModifiedTimestamp = dataObject.getDate( "fLastModifiedDate" );

				String 		creatorLoginName = dataObject.get( "fCreatorLoginName" );
				String 		ownerLoginName = dataObject.get( "fOwnerLoginName" );
				String 		modifierLoginName = dataObject.get( "fLastModifierLoginName" );

				docsVersionInfo.CreatedByUserID = XUserUtils.getUserIDFromName( creatorLoginName );
				docsVersionInfo.OwnedByID = XUserUtils.getUserIDFromName( ownerLoginName );
				docsVersionInfo.ModifiedByUserID = XUserUtils.getUserIDFromName( modifierLoginName );

				docsVersionInfo.CreatedByUserName = dataObject.get( "fCreatorFullName" );
				docsVersionInfo.OwnerUserName = dataObject.get( "fOwnerFullName" );
				docsVersionInfo.ModifiedByUserName = dataObject.get( "fLastModifierFullName" );

				docsVersionInfo.CreatorLoginName = dataObject.get( "fCreatorLoginName" );
				docsVersionInfo.OwnerLoginName = dataObject.get( "fOwnerLoginName" );
				docsVersionInfo.ModifierLoginName = dataObject.get( "fLastModifierLoginName" );

				docsVersionInfo.DocumentExtension = dataObject.get( "dExtension" );
				docsVersionInfo.DocumentFormatType = dataObject.get( "dDocFormatType" );
				docsVersionInfo.ContentLength = dataObject.getInteger( "dFileSize" );
				docsVersionInfo.FolderDescription = dataObject.get( "fFolderDescription" );

				docsVersionInfo.VersionNumber = dataObject.getInteger( "dRevLabel" );

				String isThumbnailPresent = dataObject.get( "dRendition1" );

				if ( ( XString.isNotBlank( isThumbnailPresent ) ) && ( "P".equals( isThumbnailPresent ) ) )
				{
					docsVersionInfo.ThumbnailPresent = true;
				}

				// Content search API returns a field called dRenditions2 which when = "D" means the document has
				// its preview stored in DoCS server. But as of now it is a bug on DoCS side as it does not
				// return the value correctly. Returning true always for RenditionsSupported as of now the bug is resolved.

				docsVersionInfo.RenditionsSupported = true;

				if ( sLogger.isDebugEnabled() )
				{
					sLogger.debug( "Search hit. Item info: " );
					sLogger.debug( "-- Name: {0}", docsVersionInfo.Name );
					sLogger.debug( "-- ObjectType: {0}", docsVersionInfo.ObjectType );
					sLogger.debug( "-- GUID: {0}", docsVersionInfo.ItemGUID );
					sLogger.debug( "-- Parent GUID: {0}", docsVersionInfo.ParentGUID );

					sLogger.debug( "-- Creator Login Name: {0}", docsVersionInfo.CreatorLoginName );
					sLogger.debug( "-- Owner Login Name: {0}", docsVersionInfo.OwnerLoginName );
					sLogger.debug( "-- Modifier Login Name: {0}", docsVersionInfo.ModifierLoginName );

					sLogger.debug( "-- Created by User ID: {0}", docsVersionInfo.CreatedByUserID );
					sLogger.debug( "-- Owned by User ID: {0}", docsVersionInfo.OwnedByID );
					sLogger.debug( "-- Modified by User ID: {0}", docsVersionInfo.ModifiedByUserID );

					sLogger.debug( "-- Created Timestamp: {0}", docsVersionInfo.CreatedTimestamp );
					sLogger.debug( "-- Modified Timestamp: {0}", docsVersionInfo.ModifiedTimestamp );

					sLogger.debug( "-- Document Extension: {0}", docsVersionInfo.DocumentExtension );
					sLogger.debug( "-- Document Format Type: {0}", docsVersionInfo.DocumentFormatType );
					sLogger.debug( "-- Content Length: {0}", docsVersionInfo.ContentLength );
					sLogger.debug( "-- Folder Description: {0}", docsVersionInfo.FolderDescription );
					sLogger.debug( "-- Version Number: {0}", docsVersionInfo.VersionNumber );
					sLogger.debug( "-- Thumbnail Present: {0}", docsVersionInfo.ThumbnailPresent );
				}

				XSearchResultInfo result = new XSearchResultInfo();
				result.ObjectInfo = docsVersionInfo;

				searchResults.add( result );
			}
		}
		catch ( Throwable ex )
		{
			sLogger.error( "Could not execute GET_SEARCH_RESULTS IDC service successfully.", ex );

			throw new XRuntimeException( "waggle.server.integration.docs.idc.CouldNotPerformContentSearch", ex );
		}
		finally
		{
			if ( response != null )
			{
				response.close();
			}
		}

		if ( sLogger.isDebugEnabled() )
		{
			sLogger.debug( "Returning {0} search hits.", searchResults.size() );
		}

		return searchResults;
	}

	/**
	 * Creates a site.
	 *
	 * @param siteName Name of site.
	 * @param description Site description.
	 * @param templateGUID GUID of template to use for site.
	 * @return Site GUID.
	 */
	public static String createSite( String siteName, String description, String templateGUID )
	{
		ServiceResponse 		response = null;
		String					retval = null;

		try
		{
			IdcClient 			idcClient = getIdcClient();
			IdcContext 			userContext = getIdcContext();
			DataBinder 			binder = getDataBinder( idcClient, SERVICE_SCS_COPY_SITES );

			binder.putLocal( "items", "fFolderGUID:" + templateGUID );
			binder.putLocal( "names", siteName );
			binder.putLocal( "descriptions", description );

			if ( sLogger.isDebugEnabled() )
			{
				sLogger.debug( "Binder object in the IDC request for SERVICE_SCS_COPY_SITES: {0}", binder.toString() );
			}

			response = execute( idcClient, userContext, binder, "waggle.server.integration.docs.idc.CouldNotCreateSite" );

			DataBinder 			responseBinder = response.getResponseAsBinder();

			if ( responseBinder != null )
			{
				DataResultSet siteInfo = responseBinder.getResultSet( "SiteInfo" );

				if ( siteInfo != null )
				{
					List<DataObject> 	dataObjects = siteInfo.getRows();

					for ( DataObject dataObject : dataObjects )
					{
						if ( dataObject.get( "fFolderName" ).equals( siteName ) )
						{
							retval = dataObject.get( "fFolderGUID" );
						}
					}
				}
				else
				{
					sLogger.warning( "Call to the Content Server returned null package info. Could not create site." );

					throw new XRuntimeException( "waggle.server.integration.docs.idc.CouldNotCreateSite" );
				}

			}
			else
			{
				sLogger.warning( "Call to the Content Server returned null local data. Could not create site." );

				throw new XRuntimeException( "waggle.server.integration.docs.idc.CouldNotCreateSite" );
			}

		}
		catch ( Throwable ex )
		{
			sLogger.error( "Exception in invoking {0} service", SERVICE_SCS_COPY_SITES, ex );

			throw new XRuntimeException( "waggle.server.integration.docs.idc.CouldNotCreateSite", ex );
		}
		finally
		{
			if ( response != null )
			{
				response.close();
			}
		}

		if ( retval == null )
		{
			sLogger.error( "No error encountered during site creation, but unable to find site information in SCS_COPY_SITES response." );

			throw new XRuntimeException( "waggle.server.integration.docs.idc.CouldNotFindSitePostCreation" );
		}

		return retval;
	}

	/**
	 * Sets a sites state to active or otherwise.
	 *
	 * @param siteGUID Site GUID.
	 * @param active True to activate. False to deactivate.
	 */
	public static void setSiteState( String siteGUID, boolean active )
	{
		ServiceResponse 		response = null;
		String					serviceName = null;

		try
		{
			IdcClient 			idcClient = getIdcClient();
			IdcContext 			userContext = getIdcContext();
			DataBinder 			binder;

			if ( active )
			{
				serviceName = SERVICE_SCS_ACTIVATE_SITE;
			}
			else
			{
				serviceName = SERVICE_SCS_DEACTIVATE_SITE;
			}

			binder = getDataBinder( idcClient, serviceName );
			binder.putLocal( "item", "fFolderGUID:" + siteGUID );

			if ( sLogger.isDebugEnabled() )
			{
				sLogger.debug( "Binder object in the IDC request for {0}: {1}", serviceName, binder.toString() );
			}

			response = execute( idcClient,
								userContext,
								binder,
								"waggle.server.integration.docs.idc.CouldNotSetSiteState",
								( active ?
								  "waggle.server.integration.docs.idc.SiteStateActive"		// I18N
										 :
								  "waggle.server.integration.docs.idc.SiteStateInactive"	// I18N
								),
								siteGUID );

			DataBinder 			responseBinder = response.getResponseAsBinder();

			if ( ( responseBinder != null ) && ( responseBinder.getLocalData() != null ) )
			{
				DataObject 	localData = responseBinder.getLocalData();

				int statusCode = localData.getInteger( "StatusCode" );

				if ( statusCode != STATUS_CODE_SUCCESS )
				{
					sLogger.warning( "Call to the Content Server returned unsuccessful status for setting site state to {0} for site with GUID {1}.",
									 ( active ? "waggle.server.integration.docs.idc.SiteStateActive" : "waggle.server.idc.SiteStateInactive" ),
									 siteGUID );

					throw new XRuntimeException( "waggle.server.integration.docs.idc.CouldNotSetSiteState",
												 ( active ?
												   "waggle.server.integration.docs.idc.SiteStateActive"			// I18N
														  :
												   "waggle.server.integration.docs.idc.SiteStateInactive"		// I18N
												 ),
												 siteGUID );
				}

			}
			else
			{
				sLogger.warning( "Call to the Content Server returned null local data. Could not set site status to {0} for site with GUID {1}." +
								 ( active ? "Active" : "Inactive" ),
								 siteGUID );

				throw new XRuntimeException( "waggle.server.integration.docs.idc.CouldNotSetSiteState",
											 ( active ? "waggle.server.integration.docs.idc.SiteStateActive" : "waggle.server.idc.SiteStateInactive" ),
											 siteGUID );
			}

		}
		catch ( Throwable ex )
		{
			sLogger.error( "Exception in invoking {0} service", serviceName, ex );

			throw new XRuntimeException( "waggle.server.integration.docs.idc.CouldNotCreateSite", ex );
		}
		finally
		{
			if ( response != null )
			{
				response.close();
			}
		}
	}

	/**
	 * Get site GUID given its name.
	 *
	 * @param siteName Site name.
	 * @param isTemplate True if site is a template.
	 * @return Template GUID.
	 */
	public static String getSiteGUID( String siteName, boolean isTemplate )
	{
		ServiceResponse 		response = null;
		String					retval = null;

		try
		{
			IdcClient 			idcClient = getIdcClient();
			IdcContext 			userContext = getIdcContext();
			DataBinder 			binder = getDataBinder( idcClient, SERVICE_SCS_BROWSE_SITES );
			int					siteStartRow = 0;

			binder.putLocal( "siteCount", Integer.toString( DEFAULT_PAGE_SIZE ) );

			if ( isTemplate )
			{
				binder.putLocal( "fApplication", "framework.site.template" );
			}
			else
			{
				binder.putLocal( "fApplication", "framework.site" );
			}

			if ( sLogger.isDebugEnabled() )
			{
				sLogger.debug( "Binder object in the IDC request for SERVICE_SCS_BROWSE_SITES: {0}", binder.toString() );
			}

			boolean				hasMoreSites;

			do
			{
				binder.putLocal( "siteStartRow", Integer.toString( siteStartRow ) );

				response = execute( idcClient, userContext, binder, "waggle.server.integration.docs.idc.ErrorBrowsingSites" );

				DataBinder 			responseBinder = response.getResponseAsBinder();

				if ( responseBinder != null )
				{
					DataObject localData = responseBinder.getLocalData();

					hasMoreSites = localData.getBoolean( "hasMoreSites", false );

					DataResultSet siteInfo = responseBinder.getResultSet( "SiteInfo" );

					if ( siteInfo != null )
					{
						List<DataObject> dataObjects = siteInfo.getRows();

						for ( DataObject dataObject : dataObjects )
						{
							if ( dataObject.get( "fFolderName" ).equals( siteName ) )
							{
								retval = dataObject.get( "fFolderGUID" );
							}
						}
					}
					else
					{
						sLogger.warning( "Call to the Content Server returned null package info. Could not browse site templates." );

						throw new XRuntimeException( "waggle.server.integration.docs.idc.ErrorBrowsingSites" );
					}
				}
				else
				{
					sLogger.warning( "Call to the Content Server returned response that resulted in a null data binder. Could not browse site templates." );

					throw new XRuntimeException( "waggle.server.integration.docs.idc.ErrorBrowsingSites" );
				}

				siteStartRow += DEFAULT_PAGE_SIZE;
			}
			while ( ( hasMoreSites ) && ( retval == null ) );
		}
		catch ( Throwable ex )
		{
			sLogger.error( "Exception in invoking {0} service", SERVICE_SCS_BROWSE_SITES, ex );

			throw new XRuntimeException( "waggle.server.integration.docs.idc.ErrorBrowsingSites", ex );
		}
		finally
		{
			if ( response != null )
			{
				response.close();
			}
		}

		return retval;
	}

	/**
	 * Gets site ID given site name.
	 *
	 * @param siteName Name of site.
	 * @return Site ID.
	 */
	public static String getSitePartGUID( String siteName )
	{
		ServiceResponse 		response = null;
		String					retval;

		try
		{
			IdcClient 			idcClient = getIdcClient();
			IdcContext 			userContext = getIdcContext();
			DataBinder 			binder = getDataBinder( idcClient, SERVICE_SCS_GET_SITE_PART_GUID );

			binder.putLocal( "siteId", siteName );

			if ( sLogger.isDebugEnabled() )
			{
				sLogger.debug( "Binder object in the IDC request for SERVICE_SCS_GET_SITE_PART_GUID: {0}", binder.toString() );
			}

			response = execute( idcClient, userContext, binder, "waggle.server.integration.docs.idc.CouldNotGetSitePartGUID" );

			DataBinder 			responseBinder = response.getResponseAsBinder();

			if ( ( responseBinder != null ) && ( responseBinder.getLocalData() != null ) )
			{
				retval = responseBinder.getLocalData().get( "partId" );
			}
			else
			{
				sLogger.warning( "Call to the Content Server returned null local data. Could not create site." );

				throw new XRuntimeException( "waggle.server.integration.docs.idc.CouldNotGetSitePartGUID" );
			}

		}
		catch ( Throwable ex )
		{
			sLogger.error( "Exception in invoking {0} service", SERVICE_SCS_GET_SITE_PART_GUID, ex );

			throw new XRuntimeException( "waggle.server.integration.docs.idc.CouldNotGetSitePartGUID", ex );
		}
		finally
		{
			if ( response != null )
			{
				response.close();
			}
		}

		return retval;
	}

	/**
	 * Imports OOTB site templates. ONLY USED FOR TESTING.
	 *
	 * Starts the import job as a background job and returns the jobID
	 * of templateName. getBackgroundJobStatus may be called with the
	 * returned jobID to check status.
	 *
	 * @param templateName Name of template from set of OOTB templates to check for.
	 * @return Job ID.
	 */
	public static String importOOTBSiteTemplates( String templateName )
	{
		ServiceResponse 		response = null;
		String					retval = null;

		try
		{
			IdcClient 			idcClient = getIdcClient();
			IdcContext 			userContext = getIdcContext();
			DataBinder 			binder = getDataBinder( idcClient, SERVICE_SCS_IMPORT_OOTB_TEMPLATES );

			binder.putLocal( "isOverwrite", "false" );
			binder.putLocal( "useBackgroundThread", "true" );

			if ( sLogger.isDebugEnabled() )
			{
				sLogger.debug( "Binder object in the IDC request for SERVICE_SCS_IMPORT_OOTB_TEMPLATES: {0}", binder.toString() );
			}

			response = execute( idcClient, userContext, binder, "waggle.server.integration.docs.idc.ErrorImportingOOTBSiteTemplates" );

			DataBinder 			responseBinder = response.getResponseAsBinder();

			if ( ( responseBinder != null ) && ( responseBinder.getResultSet( "PackageJobInfo" ) != null ) )
			{
				DataResultSet		packageJobInfo = responseBinder.getResultSet( "PackageJobInfo" );

				if ( packageJobInfo != null )
				{
					List<DataObject> 	dataObjects = packageJobInfo.getRows();

					for ( DataObject dataObject : dataObjects )
					{
						if ( ( dataObject.get( "templateName" ).equals( templateName ) ) ||
							 ( dataObject.get( "packageName" ).equals( templateName + ".zip" ) ) )
						{
							retval = dataObject.get( "jobID" );
						}
					}
				}
				else
				{
					sLogger.warning( "Call to the Content Server returned null package info. Could not import OOTB templates." );

					throw new XRuntimeException( "waggle.server.integration.docs.idc.ErrorImportingOOTBSiteTemplates" );
				}
			}
			else
			{
				sLogger.warning( "Call to the Content Server returned null local data. Could not import OOTB templates." );

				throw new XRuntimeException( "waggle.server.integration.docs.idc.ErrorImportingOOTBSiteTemplates" );
			}
		}
		catch ( Throwable ex )
		{
			sLogger.error( "Exception in invoking {0} service", SERVICE_SCS_IMPORT_OOTB_TEMPLATES, ex );

			throw new XRuntimeException( "waggle.server.integration.docs.idc.ErrorImportingOOTBSiteTemplates", ex );
		}
		finally
		{
			if ( response != null )
			{
				response.close();
			}
		}

		return retval;
	}

	/**
	 * Gets background job status.
	 *
	 * @param jobID Job ID.
	 * @return Job status.
	 */
	public static String getBackgroundJobStatus( String jobID )
	{
		ServiceResponse 		response = null;
		String					retval = null;

		try
		{
			IdcClient 			idcClient = getIdcClient();
			IdcContext 			userContext = getIdcContext();
			DataBinder 			binder = getDataBinder( idcClient, SERVICE_SCS_GET_BACKGROUND_SERVICE_JOB_STATUS );

			binder.putLocal( "JobID", jobID );

			if ( sLogger.isDebugEnabled() )
			{
				sLogger.debug( "Binder object in the IDC request for SERVICE_SCS_GET_BACKGROUND_SERVICE_JOB_STATUS: {0}", binder.toString() );
			}

			response = execute( idcClient, userContext, binder, "waggle.server.integration.docs.idc.ErrorGetBackGroundServiceJobStatus", jobID );

			DataBinder 			responseBinder = response.getResponseAsBinder();

			if ( ( responseBinder != null ) && ( responseBinder.getResultSet( "JobInfo" ) != null ) )
			{
				DataResultSet		jobInfo = responseBinder.getResultSet( "JobInfo" );

				if ( jobInfo != null )
				{
					List<DataObject> 	dataObjects = jobInfo.getRows();

					for ( DataObject dataObject : dataObjects )
					{
						if ( dataObject.get( "JobID" ).equals( jobID ) )
						{
							retval = dataObject.get( "JobStatus" );
						}
					}
				}
				else
				{
					sLogger.warning( "Call to the Content Server returned null package info. Could not get background job status." );

					throw new XRuntimeException( "waggle.server.integration.docs.idc.ErrorGetBackGroundServiceJobStatus", jobID );
				}
			}
			else
			{
				sLogger.warning( "Call to the Content Server returned null package info. Could not get background job status." );

				throw new XRuntimeException( "waggle.server.integration.docs.idc.ErrorGetBackGroundServiceJobStatus", jobID );
			}
		}
		catch ( Throwable ex )
		{
			sLogger.error( "Exception in invoking {0} service", SERVICE_SCS_GET_BACKGROUND_SERVICE_JOB_STATUS, ex );

			throw new XRuntimeException( "waggle.server.integration.docs.idc.ErrorGetBackGroundServiceJobStatus", jobID, ex );
		}
		finally
		{
			if ( response != null )
			{
				response.close();
			}
		}

		return retval;
	}


	/**
	 * Returns the URL for a Community Site.
	 *
	 * @param siteName SiteName
	 * @return The URL for a Community Site.
	 */
	public static String getCommunitySiteURL( String siteName )
	{
		return XContentServerUtil.getContentServerExternalURL() + "/" + siteName;
	}

	/**
	 * Updates the logo to Content Server. Uses the UPLOAD_TENANT_LOGO service.
	 *
	 * @param contentStream The image input stream.
	 * @param fileName The file name with extension.
	 * @param contentLength Length of the file in bytes.
	 * @param contentType The mime type.
	 */
	public static void updateHiveLogo(
		final XAPIInputStream	contentStream,
		final String 			fileName,
		final long 				contentLength,
		final String 			contentType )
	{
		ServiceResponse 	response = null;

		try
		{
			IdcClient 		idcClient = getIdcClient();
			IdcContext 		userContext = getIdcContext();
			DataBinder 		binder = getDataBinder( idcClient, SERVICE_LOGO_UPDATE );

			binder.addFile( SERVICE_LOGO_PARAM, new TransferFile( contentStream, fileName, contentLength, contentType ) );

			// We need to set NOTIFY_OSN_PARAM flag to "0" (false) so that DoCS does not call OSN back for updating logo.

			binder.putLocal( NOTIFY_OSN_PARAM, "0" );

			if ( sLogger.isDebugEnabled() )
			{
				sLogger.debug( "Binder object in the IDC request: {0}", binder.toString() );
			}

			response = execute( idcClient, userContext, binder, "waggle.server.integration.docs.idc.CouldNotUpdateHivePicCS" );
		}
		catch ( Throwable ex )
		{
			sLogger.error( "Exception in invoking {0} service", SERVICE_LOGO_UPDATE, ex );

			throw new XRuntimeException( "waggle.server.integration.docs.idc.CouldNotUpdateHivePicCS", ex );
		}
		finally
		{
			if ( response != null )
			{
				response.close();
			}
		}
	}

	/**
	 * Delete/Remove the logo. Uses the REMOVE_TENANT_LOGO IDC Service.
	 */
	public static void deleteHiveLogo()
	{
		ServiceResponse 	response = null;

		try
		{
			IdcClient 		idcClient = getIdcClient();
			IdcContext 		userContext = getIdcContext();
			DataBinder 		binder = getDataBinder( idcClient, SERVICE_LOGO_DELETE );

			if ( sLogger.isDebugEnabled() )
			{
				sLogger.debug( "Binder object in the IDC request: {0}", binder.toString() );
			}

			response = execute( idcClient, userContext, binder, "waggle.server.integration.docs.idc.CouldNotDeleteHivePicCS" );
		}
		catch ( Throwable ex )
		{
			sLogger.error( "Exception in invoking {0} service", SERVICE_LOGO_DELETE, ex );

			throw new XRuntimeException( "waggle.server.integration.docs.idc.CouldNotDeleteHivePicCS", ex );
		}
		finally
		{
			if ( response != null )
			{
				response.close();
			}
		}
	}

	/**
	 * Update the branding text on the content server.
	 *
	 * @param newText new branding text to be set on content server.
	 */
	public static void updateHiveText( String newText )
	{
		Map<String, String> configParams  = new HashMap<>();

		configParams.put( TENANT_OPTION_NAME, "BrandedText" );
		configParams.put( TENANT_OPTION_VALUE, newText );

		updateTenantConfig( configParams );
	}

	/**
	 * Updates the tenant configuration on the content server via SET_TENANT_CONFIG service.
	 *
	 * @param params Map of values to be updated on the content server.
	 */
	private static void updateTenantConfig( Map<String, String> params )
	{
		ServiceResponse 	response = null;

		try
		{
			IdcClient 		idcClient = getIdcClient();
			IdcContext 		userContext = getIdcContext();
			DataBinder 		binder = getDataBinder( idcClient, SERVICE_TENANT_CONFIG_UPDATE );

			// Add the parameters to the binder.

			for ( Map.Entry<String, String> entry : params.entrySet() )
			{
				binder.putLocal( entry.getKey(), entry.getValue() );
			}

			// We need to set NOTIFY_OSN_PARAM flag to "0" (false) so that DoCS does not call OSN back.

			binder.putLocal( NOTIFY_OSN_PARAM, "0" );

			if ( sLogger.isDebugEnabled() )
			{
				sLogger.debug( "Binder object in the IDC request: {0}", binder.toString() );
			}

			response = execute( idcClient, userContext, binder, "waggle.server.integration.docs.idc.CouldNotUpdateTenantConfig" );
		}
		catch ( Throwable ex )
		{
			sLogger.error( "Exception in invoking {0} service", SERVICE_TENANT_CONFIG_UPDATE, ex );

			throw new XRuntimeException( "waggle.server.integration.docs.idc.CouldNotUpdateTenantConfig", ex );
		}
		finally
		{
			if ( response != null )
			{
				response.close();
			}
		}
	}

	private static String getIDCGroupType( XGroupType groupType )
	{
		String	docsGroupType;

		switch ( groupType )
		{
			case PRIVATE_CLOSED :
			{
				docsGroupType = "app_private";
				break;
			}
			case PUBLIC_CLOSED :
			{
				docsGroupType = "app_public_closed";
				break;
			}
			case PUBLIC_OPEN :
			{
				docsGroupType = "app_public_open";
				break;
			}
			default :
			{
				docsGroupType = "static";
			}
		}

		return docsGroupType;
	}

	private static XGroupType getSocialGroupType( String groupType )
	{
		XGroupType	socialGroupType = null;

		switch ( groupType )
		{
			case "app_private" :
			{
				socialGroupType = PRIVATE_CLOSED;
				break;
			}
			case "app_public_closed" :
			{
				socialGroupType = PUBLIC_CLOSED;
				break;
			}
			case "app_public_open" :
			{
				socialGroupType = PUBLIC_OPEN;
				break;
			}
		}

		return socialGroupType;
	}

	private static String getIDCGroupMemberRole( XConversationRole role )
	{
		String	docsGroupMemberRole;

		switch ( role )
		{
			case GROUP_MANAGER :
			{
				docsGroupMemberRole = "manager";
				break;
			}
			case GROUP_MEMBER :
			{
				docsGroupMemberRole = "downloader";
				break;
			}
			default :
			{
				docsGroupMemberRole = "downloader";
			}
		}

		return docsGroupMemberRole;
	}

	private static XConversationRole getSocialGroupMemberRole( String role )
	{
		XConversationRole	socialGroupMemberRole;

		switch ( role )
		{
			case "manager" :
			{
				socialGroupMemberRole = GROUP_MANAGER;
				break;
			}
			case "downloader" :
			{
				socialGroupMemberRole = GROUP_MEMBER;
				break;
			}
			default :
			{
				socialGroupMemberRole = XConversationRole.NONE;
			}
		}

		return socialGroupMemberRole;
	}

	private static String getIDCGroupID( XGroupObject groupObject )
	{
		return getIDCGroupID( groupObject.getGroupID() );
	}

	private static String getIDCGroupID( String groupID )
	{
		return ( "dGroupID:" + groupID );
	}

	private static String getGroupServiceName( XGroupSyncAction action )
	{
		String retval = null;

		switch ( action )
		{
			case ADD_GROUP_MEMBERS :
			{
				retval = SERVICE_ADD_GROUP_MEMBERS;
				break;
			}
			case REMOVE_GROUP_MEMBERS :
			{
				retval = SERVICE_REMOVE_GROUP_MEMBERS;
				break;
			}
			case GRANT_GROUP_PRIVILEGE :
			{
				retval = SERVICE_GRANT_GROUP_PRIVILEGE;
				break;
			}
			case REVOKE_GROUP_PRIVILEGE :
			{
				retval = SERVICE_REVOKE_GROUP_PRIVILEGE;
				break;
			}
			case MODIFY_GROUP_PRIVILEGE :
			{
				retval = SERVICE_MODIFY_GROUP_PRIVILEGE;
				break;
			}
			case CREATE_GROUP :
			{
				retval = SERVICE_CREATE_GROUP;
				break;
			}
			case DELETE_GROUP :
			{
				retval = SERVICE_DELETE_GROUP;
				break;
			}
			case MODIFY_GROUP :
			{
				retval = SERVICE_MODIFY_GROUP;
				break;
			}
			case JOIN_GROUP :
			{
				retval = SERVICE_JOIN_GROUP;
				break;
			}
			case LEAVE_GROUP :
			{
				retval = SERVICE_LEAVE_GROUP;
				break;
			}
		}

		return retval;
	}

	/**
	 * Create a group on DoCS.
	 *
	 * @param userObject The User object.
	 * @param groupObject The Group object.
	 *
	 * @return The status struct.
	 */
	public static XGroupSyncStruct createGroup( XUserObject userObject, XGroupObject groupObject )
	{
		ServiceResponse 		response = null;
		XGroupSyncStruct		retval;

		sLogger.error( "Invoking createGroup -> Group name::{0} -> Group type::: {1}-> USer:::{2}", groupObject.getName(), groupObject.getGroupOriginType(), userObject.getName() );

		try
		{
			IdcClient 			idcClient = getIdcClient();
			IdcContext 			userContext = getIdcContext();

			DataBinder			binder = null;

			if ( groupObject.getGroupOriginType() == XGroupOriginType.IDP )
			{
				binder = getDataBinderForIDPGroups( idcClient, SERVICE_CREATE_GROUP );
				binder.putLocal( "dGroupType", IDP_GROUP_TYPE );
			}
			else
			{
				binder = getDataBinder( idcClient, SERVICE_CREATE_GROUP, userObject );
				binder.putLocal( "dGroupType", getIDCGroupType( groupObject.getGroupType() ) );
			}

			binder.putLocal( "dGroupName", groupObject.getName() );
			binder.putLocal( "dGroupOriginType", groupObject.getGroupOriginType().name().toLowerCase() );
			binder.putLocal( "dGroupID", groupObject.getGroupID() );
			binder.putLocal( "ignoreExistenceErrors", "true" );

			if ( sLogger.isDebugEnabled() )
			{
				sLogger.debug( "Binder object in the IDC request for SERVICE_CREATE_GROUP: {0}", binder.toString() );
			}

			response = execute( idcClient, userContext, binder, "waggle.server.integration.docs.idc.CouldNotCreateGroup", groupObject.getName(), groupObject.getID() );

			DataBinder 			responseBinder = response.getResponseAsBinder();

			if ( ( responseBinder != null ) && ( responseBinder.getLocalData() != null ) )
			{
				DataObject	dataObject = responseBinder.getLocalData();

				int			statusCode = dataObject.getInteger( "StatusCode" );

				sLogger.error( "Create Group status for ::{0} is {1}", groupObject.getName(), statusCode );

				retval = new XGroupSyncStruct( XGroupSyncAction.CREATE_GROUP,
											   null,
											   groupObject,
											   ( statusCode == STATUS_CODE_SUCCESS ),
											   String.valueOf( statusCode ),
											   dataObject.get( "StatusMessage" ) );
			}
			else
			{
				sLogger.warning( "Call to the Content Server returned null local data. Could not create group." );

				throw new XRuntimeException( "waggle.server.integration.docs.idc.CouldNotCreateGroup", groupObject.getName(), groupObject.getID() );
			}

		}
		catch ( Throwable ex )
		{
			sLogger.error( "Exception in invoking {0} service", SERVICE_CREATE_GROUP, ex );

			throw new XRuntimeException( "waggle.server.integration.docs.idc.CouldNotCreateGroup", groupObject.getName(), groupObject.getID(), ex );
		}
		finally
		{
			if ( response != null )
			{
				response.close();
			}
		}

		return retval;
	}

	/**
	 * View DoCS group info.
	 *
	 * @param userObject The user object.
	 * @param groupID The group ID.
	 * @return Group info.
	 */
	public static XGroupInfo viewGroupInfo( XUserObject userObject, String groupID )
	{
		ServiceResponse 		response = null;
		XGroupInfo				retval = null;

		try
		{
			IdcClient 			idcClient = getIdcClient();
			IdcContext 			userContext = getIdcContext();
			DataBinder        	binder = null;

			XGroupObject   groupObject = XGroupObjectManager.getGroupObjectByGroupID( groupID );

			if ( XGroupOriginType.IDP == groupObject.getGroupOriginType() )
			{
				binder = getDataBinderForIDPGroups( idcClient, SERVICE_VIEW_GROUP_INFO );
			}
			else
			{
				binder = getDataBinder( idcClient, SERVICE_VIEW_GROUP_INFO, userObject );
			}


			binder.putLocal( "item", "dGroupID:" + groupID );

			//if ( sLogger.isDebugEnabled() )
			{
				sLogger.error( "Binder object in the IDC request for SERVICE_VIEW_GROUP_INFO: {0}", binder.toString() );
			}

			response = execute( idcClient, userContext, binder, "waggle.server.integration.docs.idc.CouldNotViewGroupInfo", "NA", "NA", groupID );

			DataBinder 			responseBinder = response.getResponseAsBinder();

			if ( ( responseBinder != null ) && ( responseBinder.getLocalData() != null ) )
			{
				DataObject	dataObject = responseBinder.getLocalData();
				int			statusCode = dataObject.getInteger( "StatusCode" );
				String		statusMessage = dataObject.get( "StatusMessage" );

				if ( statusCode == STATUS_CODE_SUCCESS )
				{
					String		docsGroupName = dataObject.get( "dGroupName" );
					String		docsGroupID = dataObject.get( "dGroupID" );
					String 		docsGroupType = dataObject.get( "dGroupType" );
					String		owner = dataObject.get( "dOwner" );
					String		creator = dataObject.get( "dCreator" );

					//if ( sLogger.isDebugEnabled() )
					{
						sLogger.error( "VIEW_GROUP_INFO for social GroupID {0} returned GroupID {1}, GroupName {2}, GroupType {3}, Owner {4}, Creaatpr {5}.",
									   groupID,
									   docsGroupID,
									   docsGroupName,
									   docsGroupType,
									   owner,
									   creator );
					}

					retval = new XGroupInfo();

					retval.GroupID = docsGroupID;
					retval.Name = docsGroupName;
					retval.GroupType = getSocialGroupType( docsGroupType );
					retval.GroupOwnerID = XUsersObjectManager.getUserObject( owner ).getID();
					retval.CreatedByUserName = XUsersObjectManager.getUserObject( creator ).getName();
				}
				else
				{
					//if ( sLogger.isDebugEnabled() )
					{
						sLogger.error( "VIEW_GROUP_INFO returned Status code: {0} and Status message: {1}.", statusCode, statusMessage );
					}
				}
			}
			else
			{
				sLogger.warning( "Call to the Content Server returned null local data. Could not view group info." );

				throw new XRuntimeException( "waggle.server.integration.docs.idc.CouldNotViewGroupInfo", "NA", "NA", groupID );
			}
		}
		catch ( Throwable ex )
		{
			sLogger.error( "Exception in invoking {0} service for group {1}", SERVICE_VIEW_GROUP_INFO, groupID, ex );

			throw new XRuntimeException( "waggle.server.integration.docs.idc.CouldNotViewGroupInfo", "NA", "NA", groupID, ex );
		}
		finally
		{
			if ( response != null )
			{
				response.close();
			}
		}

		return retval;
	}

	/**
	 * View DoCS group info.
	 *
	 * @param userObject The user object.
	 * @param groupObject The group object.
	 * @return Group info.
	 */
	public static XGroupInfo viewGroupInfo( XUserObject userObject, XGroupObject groupObject )
	{
		ServiceResponse 		response = null;
		XGroupInfo				retval = null;

		try
		{
			IdcClient 			idcClient = getIdcClient();
			IdcContext 			userContext = getIdcContext();
			DataBinder        	binder = null;

			if ( XGroupOriginType.IDP == groupObject.getGroupOriginType() )
			{
				binder = getDataBinderForIDPGroups( idcClient, SERVICE_VIEW_GROUP_INFO );
			}
			else
			{
				binder = getDataBinder( idcClient, SERVICE_VIEW_GROUP_INFO, userObject );
			}

			binder.putLocal( "item", getIDCGroupID( groupObject ) );

			if ( sLogger.isDebugEnabled() )
			{
				sLogger.debug( "Binder object in the IDC request for SERVICE_VIEW_GROUP_INFO: {0}", binder.toString() );
			}

			response = execute( idcClient, userContext, binder, "waggle.server.integration.docs.idc.CouldNotViewGroupInfo", groupObject.getName(), groupObject.getID(), groupObject.getGroupID() );

			DataBinder 			responseBinder = response.getResponseAsBinder();

			if ( ( responseBinder != null ) && ( responseBinder.getLocalData() != null ) )
			{
				DataObject	dataObject = responseBinder.getLocalData();
				int			statusCode = dataObject.getInteger( "StatusCode" );
				String		statusMessage = dataObject.get( "StatusMessage" );

				if ( statusCode == STATUS_CODE_SUCCESS )
				{
					DataResultSet docsGroupInfo = responseBinder.getResultSet( "GroupInfo" );
					DataObject groupDataObject = docsGroupInfo.getRows().get( 0 );

					String		docsGroupName = groupDataObject.get( "dGroupName" );
					String		docsGroupID = groupDataObject.get( "dGroupID" );
					String 		docsGroupType = groupDataObject.get( "dGroupType" );

					if ( sLogger.isDebugEnabled() )
					{
						sLogger.debug( "VIEW_GROUP_INFO for social GroupID {0} returned GroupID {1}, GroupName {2}, GroupType {3}.",
									   groupObject.getGroupID(),
									   docsGroupID,
									   docsGroupName,
									   docsGroupType );
					}

					retval = groupObject.getInfo();

					retval.GroupID = docsGroupID;
					retval.Name = docsGroupName;
					retval.GroupType = getSocialGroupType( docsGroupType );
				}
				else
				{
					if ( sLogger.isDebugEnabled() )
					{
						sLogger.debug( "VIEW_GROUP_INFO returned Status code: {0} and Status message: {1}.", statusCode, statusMessage );
					}
				}
			}
			else
			{
				sLogger.warning( "Call to the Content Server returned null local data. Could not view group info." );

				throw new XRuntimeException( "waggle.server.integration.docs.idc.CouldNotViewGroupInfo", groupObject.getName(), groupObject.getID(), groupObject.getGroupID() );
			}
		}
		catch ( Throwable ex )
		{
			sLogger.error( "Exception in invoking {0} service", SERVICE_VIEW_GROUP_INFO, ex );

			throw new XRuntimeException( "waggle.server.integration.docs.idc.CouldNotViewGroupInfo", groupObject.getName(), groupObject.getID(), groupObject.getGroupID(), ex );
		}
		finally
		{
			if ( response != null )
			{
				response.close();
			}
		}

		return retval;
	}

	/**
	 * View DoCS group members.
	 *  Note that in the rare scenario where the member returned by docs is not present in social,
	 * 	returned MemberInfo would just have the minimum details as obtained from docs.
	 * 	Verify the returned info before using.
	 *
	 * @param userObject The user object.
	 * @param groupObject The group object.
	 *
	 * @return Group members.
	 */
	public static List<XMemberInfo> viewGroupMembers( XUserObject userObject, XGroupObject groupObject )
	{
		ServiceResponse 		response = null;
		List<XMemberInfo>		retval = null;

		try
		{
			IdcClient 			idcClient = getIdcClient();
			IdcContext 			userContext = getIdcContext();
			DataBinder        	binder = null;

			if ( XGroupOriginType.IDP == groupObject.getGroupOriginType() )
			{
				binder = getDataBinderForIDPGroups( idcClient, SERVICE_VIEW_GROUP_MEMBERS );
			}
			else
			{
				binder = getDataBinder( idcClient, SERVICE_VIEW_GROUP_MEMBERS, userObject );
			}
			binder.putLocal( "item", getIDCGroupID( groupObject ) );

			if ( sLogger.isDebugEnabled() )
			{
				sLogger.debug( "Binder object in the IDC request for SERVICE_VIEW_GROUP_MEMBERS: {0}", binder.toString() );
			}

			response = execute( idcClient, userContext, binder, "waggle.server.integration.docs.idc.CouldNotViewGroupMembers", groupObject.getName(), groupObject.getID(), groupObject.getGroupID() );

			DataBinder 			responseBinder = response.getResponseAsBinder();

			if ( ( responseBinder != null ) && ( responseBinder.getLocalData() != null ) )
			{
				DataObject	dataObject = responseBinder.getLocalData();
				int			statusCode = dataObject.getInteger( "StatusCode" );
				String		statusMessage = dataObject.get( "StatusMessage" );

				if ( statusCode == STATUS_CODE_SUCCESS )
				{
					DataResultSet groupMembers = responseBinder.getResultSet( "GroupMembers" );

					if ( groupMembers != null )
					{
						List<DataObject> 	dataObjects = groupMembers.getRows();
						retval = new ArrayList<>( dataObjects.size() );
						// view group privs can only be done by managers - so use group owner
						List<DoCSGroupPrivilegeStruct> privilegeStructs = viewGroupPrivileges( groupObject.getOwnerUserObject(), groupObject );

						for ( DataObject memberDataObject : dataObjects )
						{
							String memberLoginIDName = memberDataObject.get( "dMemberIDLoginName" );
							// user or group
							String memberType = memberDataObject.get( "dMemberType" );

							if ( sLogger.isDebugEnabled() )
							{
								sLogger.debug( "Group Member MemberLoginIDName {0}, Member type {1}.", memberLoginIDName, memberType );
							}

							if ( memberType.equals( "user" ) )
							{
								XUserObject memberUserObject = XUsersObjectManager.findUserObject( memberLoginIDName );
								XUserInfo	userInfo;

								if ( memberUserObject != null )
								{
									userInfo = memberUserObject.getInfo();
									XConversationRole matchingRole = null;

									for ( DoCSGroupPrivilegeStruct struct : privilegeStructs )
									{
										if ( sLogger.isDebugEnabled() )
										{
											sLogger.debug( "Group Member Privilege UserLoginIDName {0}, Role {1} MemberLoginIDName {2}.", struct.getUserIDLoginName(), struct.getMemberRole(), memberLoginIDName );
										}

										if ( struct.getUserIDLoginName().equals( memberLoginIDName ) )
										{
											matchingRole = struct.getMemberRole();
											break;
										}
									}

									// TODO SVS - docs apparently does not send downloader privileges from the VIEW_GROUP_PRIVILEGES service
									if ( matchingRole == null )
									{
										matchingRole = XConversationRole.GROUP_MEMBER;
									}

									userInfo.MemberRole = matchingRole;
									userInfo.ObjectType = XUserObject.TYPE;
								}
								else
								{
									// User is not found in social. Ideally this should not happen.
									userInfo = new XUserInfo();
									userInfo.Name = memberLoginIDName;
									userInfo.ObjectType = XUserObject.TYPE;
								}

								retval.add( userInfo );
							}
							else if ( memberType.equals( "group" ) )
							{
								String			groupID = memberDataObject.get("dMemberID");
								XGroupInfo		groupInfo;
								XGroupObject	foundGroupObject =  XGroupsObjectManager.findGroupObjectByGroupID( groupID );

								if ( foundGroupObject != null )
								{
									groupInfo = foundGroupObject.getInfo();
									// groupInfo.MemberRole = memberType; VIEW_GROUP_PRIVILEGES does not work for groups
									groupInfo.ObjectType = XGroupObject.TYPE;
								}
								else
								{
									groupInfo = new XGroupInfo();
									groupInfo.ID = XObjectID.valueOf( groupID.substring( 2 ) );
									groupInfo.ObjectType = XGroupObject.TYPE;
								}

								retval.add( groupInfo );
							}
						}
					}
					else
					{
						sLogger.warning( "Call to the Content Server returned null members list. Could not get group members." );

						throw new XRuntimeException( "waggle.server.integration.docs.idc.CouldNotViewGroupMembers", groupObject.getName(), groupObject.getID(), groupObject.getGroupID() );
					}
				}
				else
				{
					if ( sLogger.isDebugEnabled() )
					{
						sLogger.debug( "VIEW_GROUP_MEMBERS returned Status code: {0} and Status message: {1}.", statusCode, statusMessage );
					}
				}
			}
			else
			{
				sLogger.warning( "Call to the Content Server returned null local data. Could not view group members." );

				throw new XRuntimeException( "waggle.server.integration.docs.idc.CouldNotViewGroupMembers", groupObject.getName(), groupObject.getID(), groupObject.getGroupID() );
			}
		}
		catch ( Throwable ex )
		{
			sLogger.error( "Exception in invoking {0} service", SERVICE_VIEW_GROUP_MEMBERS, ex );

			throw new XRuntimeException( "waggle.server.integration.docs.idc.CouldNotViewGroupMembers", groupObject.getName(), groupObject.getID(), groupObject.getGroupID(), ex );
		}
		finally
		{
			if ( response != null )
			{
				response.close();
			}
		}

		return retval;
	}

	/**
	 * View DoCS group members.
	 *
	 * Note: This service only works for member users
	 * and not for groups.
	 *
	 * @param userObject The user object.
	 * @param groupObject The group object.
	 *
	 * @return Group members privileges.
	 */
	private static List<DoCSGroupPrivilegeStruct> viewGroupPrivileges( XUserObject userObject, XGroupObject groupObject )
	{
		ServiceResponse 				response = null;
		List<DoCSGroupPrivilegeStruct>	retval = null;

		try
		{
			IdcClient 			idcClient = getIdcClient();
			IdcContext 			userContext = getIdcContext();
			DataBinder        	binder = null;

			if ( XGroupOriginType.IDP == groupObject.getGroupOriginType() )
			{
				binder = getDataBinderForIDPGroups( idcClient, SERVICE_VIEW_GROUP_PRIVILEGES );
			}
			else
			{
				binder = getDataBinder( idcClient, SERVICE_VIEW_GROUP_PRIVILEGES, userObject );
			}

			binder.putLocal( "item", getIDCGroupID( groupObject ) );

			if ( sLogger.isDebugEnabled() )
			{
				sLogger.debug( "Binder object in the IDC request for SERVICE_VIEW_GROUP_PRIVILEGES: {0}", binder.toString() );
			}

			response = execute( idcClient, userContext, binder, "waggle.server.integration.docs.idc.CouldNotViewGroupPrivileges", groupObject.getName(), groupObject.getID(), groupObject.getGroupID() );

			DataBinder 			responseBinder = response.getResponseAsBinder();

			if ( ( responseBinder != null ) && ( responseBinder.getLocalData() != null ) )
			{
				DataObject	dataObject = responseBinder.getLocalData();
				int			statusCode = dataObject.getInteger( "StatusCode" );
				String		statusMessage = dataObject.get( "StatusMessage" );

				if ( statusCode == STATUS_CODE_SUCCESS )
				{
					DataResultSet groupAuthMembers = responseBinder.getResultSet( "GroupAuthMembers" );

					if ( groupAuthMembers != null )
					{
						List<DataObject> 	dataObjects = groupAuthMembers.getRows();
						retval = new ArrayList<DoCSGroupPrivilegeStruct>( dataObjects.size() );
						for ( DataObject memberDataObject : dataObjects )
						{
							String userIDLoginName = memberDataObject.get( "dUserIDLoginName" );
							String roleName = memberDataObject.get( "dRoleName" );

							if ( sLogger.isDebugEnabled() )
							{
								sLogger.debug( "Group Member Privilege UserLoginIDName {0}, RoleName {1}.", userIDLoginName, roleName );
							}

							retval.add( new DoCSGroupPrivilegeStruct( userIDLoginName, roleName ) );
						}
					}
					else
					{
						sLogger.warning( "Call to the Content Server returned null members privilege list. Could not get group privileges." );

						throw new XRuntimeException( "waggle.server.integration.docs.idc.CouldNotViewGroupPrivileges", groupObject.getName(), groupObject.getID(), groupObject.getGroupID() );
					}
				}
				else
				{
					if ( sLogger.isDebugEnabled() )
					{
						sLogger.debug( "VIEW_GROUP_PRIVILEGES returned Status code: {0} and Status message: {1}.", statusCode, statusMessage );
					}
				}
			}
			else
			{
				sLogger.warning( "Call to the Content Server returned null local data. Could not view group privileges." );

				throw new XRuntimeException( "waggle.server.integration.docs.idc.CouldNotViewGroupPrivileges", groupObject.getName(), groupObject.getID(), groupObject.getGroupID() );
			}
		}
		catch ( Throwable ex )
		{
			sLogger.error( "Exception in invoking {0} service", SERVICE_VIEW_GROUP_PRIVILEGES, ex );

			throw new XRuntimeException( "waggle.server.integration.docs.idc.CouldNotViewGroupPrivileges", groupObject.getName(), groupObject.getID(), groupObject.getGroupID(), ex );
		}
		finally
		{
			if ( response != null )
			{
				response.close();
			}
		}

		return retval;
	}

	private static class DoCSGroupPrivilegeStruct
	{
		private String fUserIDLoginName;
		private XConversationRole fMemberRole;

		private DoCSGroupPrivilegeStruct( String userIDLoginName, String memberRole )
		{
			fUserIDLoginName = userIDLoginName;
			fMemberRole = getSocialGroupMemberRole( memberRole );
		}

		private String getUserIDLoginName()
		{
			return fUserIDLoginName;
		}

		private XConversationRole getMemberRole()
		{
			return fMemberRole;
		}
	}

	/**
	 * Delete a Group from DoCS.
	 *
	 * @param userObject The User object.
	 * @param groupID The GroupID.
	 * @return The status struct.
	 */
	public static XGroupSyncStruct deleteGroup( XUserObject userObject, String groupID )
	{
		return deleteGroup( userObject, groupID, null );
	}

	/**
	 * Delete a Group from DoCS.
	 *
	 * @param userObject The User object.
	 * @param groupID The GroupID.
	 * @param groupOriginType  Group origin type.
	 * @return The status struct.
	 */
	public static XGroupSyncStruct deleteGroup( XUserObject userObject, String groupID, XGroupOriginType groupOriginType )
	{
		ServiceResponse 		response = null;
		XGroupSyncStruct		retval = null;

		try
		{
			IdcClient 			idcClient = getIdcClient();
			IdcContext 			userContext = getIdcContext();
			DataBinder 			binder = null;

			sLogger.warning( " Group delete:: {0} -> {1}", groupID, groupOriginType == null ? "NULL" : groupOriginType.name()  );

			if ( groupOriginType == XGroupOriginType.IDP )
			{
				binder = getDataBinderForIDPGroups( idcClient, SERVICE_DELETE_GROUP );
			}
			else
			{
				binder = getDataBinder( idcClient, SERVICE_DELETE_GROUP, userObject );
			}

			binder.putLocal( "item", getIDCGroupID( groupID ) );
			binder.putLocal( "ignoreExistenceErrors", "true" );

			if ( sLogger.isDebugEnabled() )
			{
				sLogger.debug( "Binder object in the IDC request for SERVICE_DELETE_GROUP: {0}", binder.toString() );
			}

			response = execute( idcClient, userContext, binder, "waggle.server.integration.docs.idc.CouldNotDeleteGroup", groupID );

			DataBinder responseBinder = response.getResponseAsBinder();

			if ( ( responseBinder != null ) && ( responseBinder.getLocalData() != null ) )
			{
				DataObject dataObject = responseBinder.getLocalData();
				int statusCode = dataObject.getInteger( "StatusCode" );

				retval = new XGroupSyncStruct( XGroupSyncAction.DELETE_GROUP,
											   groupID,
											   null,
											   ( statusCode == STATUS_CODE_SUCCESS ),
											   String.valueOf( statusCode ),
											   dataObject.get( "StatusMessage" ) );
			}
			else
			{
				sLogger.warning( "Call to the Content Server returned null local data. Could not delete group." );

				throw new XRuntimeException( "waggle.server.integration.docs.idc.CouldNotDeleteGroup", groupID );
			}
		}
		catch ( Throwable ex )
		{
			sLogger.error( "Exception in invoking {0} service", SERVICE_DELETE_GROUP, ex );

			throw new XRuntimeException( "waggle.server.integration.docs.idc.CouldNotDeleteGroup", groupID, ex );
		}
		finally
		{
			if ( response != null )
			{
				response.close();
			}
		}

		return retval;
	}

	/**
	 * Modify a Group on DoCS.
	 *
	 * @param userObject The User object.
	 * @param groupObject The Group object.
	 * @param newGroupName The Group's new name. Optional.
	 * @param newGroupType The Group's new type. Optional.
	 * @return Sync status struct.
	 */
	public static XGroupSyncStruct modifyGroup( XUserObject userObject, XGroupObject groupObject, String newGroupName, XGroupType newGroupType )
	{
		ServiceResponse 		response = null;
		XGroupSyncStruct		retval;

		try
		{
			IdcClient 			idcClient = getIdcClient();
			IdcContext 			userContext = getIdcContext();
			DataBinder 			binder = null;

			sLogger.warning( " Group modify:: {0} -> {1}", groupObject.getName(), groupObject.getGroupOriginType() == null ? "NULL" : groupObject.getGroupOriginType().name()  );

			if ( groupObject.getGroupOriginType() == XGroupOriginType.IDP )
			{
				binder = getDataBinderForIDPGroups( idcClient, SERVICE_MODIFY_GROUP );
			}
			else
			{
				binder = getDataBinder( idcClient, SERVICE_MODIFY_GROUP, userObject );
			}

			binder.putLocal( "item", getIDCGroupID( groupObject ) );

			if ( XString.isNotBlank( newGroupName ) )
			{
				binder.putLocal( "dGroupName", newGroupName );
			}

			if ( newGroupType != null )
			{
				binder.putLocal( "dGroupType", getIDCGroupType( newGroupType ) );
			}

			if ( sLogger.isDebugEnabled() )
			{
				sLogger.debug( "Binder object in the IDC request for SERVICE_MODIFY_GROUP: {0}", binder.toString() );
			}

			response = execute( idcClient, userContext, binder, "waggle.server.integration.docs.idc.CouldNotModifyGroup", groupObject.getName(), groupObject.getID(), groupObject.getGroupID() );

			DataBinder 			responseBinder = response.getResponseAsBinder();

			if ( ( responseBinder != null ) && ( responseBinder.getLocalData() != null ) )
			{
				DataObject	dataObject = responseBinder.getLocalData();
				int			statusCode = dataObject.getInteger( "StatusCode" );

				retval = new XGroupSyncStruct( XGroupSyncAction.MODIFY_GROUP,
											   null,
											   groupObject,
											   ( statusCode == STATUS_CODE_SUCCESS ),
											   String.valueOf( statusCode ),
											   dataObject.get( "StatusMessage" ) );
			}
			else
			{
				sLogger.warning( "Call to the Content Server returned null local data. Could not modify group." );

				throw new XRuntimeException( "waggle.server.integration.docs.idc.CouldNotModifyGroup", groupObject.getName(), groupObject.getID(), groupObject.getGroupID() );
			}
		}
		catch ( Throwable ex )
		{
			sLogger.error( "Exception in invoking {0} service", SERVICE_MODIFY_GROUP, ex );

			throw new XRuntimeException( "waggle.server.integration.docs.idc.CouldNotModifyGroup", groupObject.getName(), groupObject.getID(), groupObject.getGroupID(), ex );
		}
		finally
		{
			if ( response != null )
			{
				response.close();
			}
		}

		return retval;
	}

	/**
	 * Join a group on DoCS.
	 *
	 * @param userObject The User object.
	 * @param groupObject The Group object.
	 * @return Sync status struct.
	 */
	public static XGroupSyncStruct joinGroup( XUserObject userObject, XGroupObject groupObject )
	{
		ServiceResponse 		response = null;
		XValidate.argumentNotNull( "JoinGroup UserObject.", userObject );
		String					userName = userObject.getName();
		XGroupSyncStruct		retval;

		try
		{
			IdcClient 			idcClient = getIdcClient();
			IdcContext 			userContext = getIdcContext();
			DataBinder 			binder = getDataBinder( idcClient, SERVICE_JOIN_GROUP, userObject );

			binder.putLocal( "item", getIDCGroupID( groupObject ) );

			if ( sLogger.isDebugEnabled() )
			{
				sLogger.debug( "Binder object in the IDC request for SERVICE_JOIN_GROUP: {0}", binder.toString() );
			}

			response = execute( idcClient, userContext, binder, "waggle.server.integration.docs.idc.CouldNotJoinGroup", userName, groupObject.getName(), groupObject.getID(), groupObject.getGroupID() );

			DataBinder 			responseBinder = response.getResponseAsBinder();

			if ( ( responseBinder != null ) && ( responseBinder.getLocalData() != null ) )
			{
				DataObject	dataObject = responseBinder.getLocalData();
				int			statusCode = dataObject.getInteger( "StatusCode" );

				retval = new XGroupSyncStruct( XGroupSyncAction.JOIN_GROUP,
											   null,
											   groupObject,
											   ( statusCode == STATUS_CODE_SUCCESS ),
											   String.valueOf( statusCode ),
											   dataObject.get( "StatusMessage" ) );
			}
			else
			{
				sLogger.warning( "Call to the Content Server returned null local data. Could not join group." );

				throw new XRuntimeException( "waggle.server.integration.docs.idc.CouldNotJoinGroup", groupObject.getName(), groupObject.getID(), groupObject.getGroupID() );
			}
		}
		catch ( Throwable ex )
		{
			sLogger.error( "Exception in invoking {0} service", SERVICE_JOIN_GROUP, ex );

			throw new XRuntimeException( "waggle.server.integration.docs.idc.CouldNotJoinGroup", userName, groupObject.getName(), groupObject.getID(), groupObject.getGroupID(), ex );
		}
		finally
		{
			if ( response != null )
			{
				response.close();
			}
		}

		return retval;
	}

	/**
	 * Leave a Group on DoCS.
	 *
	 * @param userObject The User object.
	 * @param groupObject The Group object.
	 * @return Sync status struct.
	 */
	public static XGroupSyncStruct leaveGroup( XUserObject userObject, XGroupObject groupObject )
	{
		ServiceResponse 		response = null;
		XValidate.argumentNotNull( "LeaveGroup UserObject.", userObject );
		String					userName = userObject.getName();
		XGroupSyncStruct		retval;

		try
		{
			IdcClient 			idcClient = getIdcClient();
			IdcContext 			userContext = getIdcContext();
			DataBinder 			binder = getDataBinder( idcClient, SERVICE_LEAVE_GROUP, userObject );

			binder.putLocal( "item", getIDCGroupID( groupObject ) );

			if ( sLogger.isDebugEnabled() )
			{
				sLogger.debug( "Binder object in the IDC request for SERVICE_LEAVE_GROUP: {0}", binder.toString() );
			}

			response = execute( idcClient, userContext, binder, "waggle.server.integration.docs.idc.CouldNotLeaveGroup", userName, groupObject.getName(), groupObject.getID(), groupObject.getGroupID() );

			DataBinder 			responseBinder = response.getResponseAsBinder();

			if ( ( responseBinder != null ) && ( responseBinder.getLocalData() != null ) )
			{
				DataObject	dataObject = responseBinder.getLocalData();
				int			statusCode = dataObject.getInteger( "StatusCode" );

				retval = new XGroupSyncStruct( XGroupSyncAction.LEAVE_GROUP,
											   null,
											   groupObject,
											   ( statusCode == STATUS_CODE_SUCCESS ),
											   String.valueOf( statusCode ),
											   dataObject.get( "StatusMessage" ) );
			}
			else
			{
				sLogger.warning( "Call to the Content Server returned null local data. Could not leave group." );

				throw new XRuntimeException( "waggle.server.integration.docs.idc.CouldNotLeaveGroup", groupObject.getName(), groupObject.getID(), groupObject.getGroupID() );
			}
		}
		catch ( Throwable ex )
		{
			sLogger.error( "Exception in invoking {0} service", SERVICE_LEAVE_GROUP, ex );

			throw new XRuntimeException( "waggle.server.integration.docs.idc.CouldNotLeaveGroup", userName, groupObject.getName(), groupObject.getID(), groupObject.getGroupID(), ex );
		}
		finally
		{
			if ( response != null )
			{
				response.close();
			}
		}

		return retval;
	}

	private static String getMemberIDs( Set<String> memberIDs )
	{
		return XString.join( memberIDs.toArray(), "," );
	}

	private static List<XGroupSyncStruct> performGroupMembersAction( XUserObject userObject,
																	 XGroupObject groupObject,
																	 Map<String, XMemberObject> members,
																	 XConversationRole role,
																	 XGroupSyncAction action,
																	 String exceptionResourceID,
																	 Object... exceptionResourceArgs )
	{
		if ( XCollections.isMapEmpty( members ) )
		{
			return new ArrayList<XGroupSyncStruct>();
		}

		ServiceResponse 			response = null;
		List<XGroupSyncStruct>		retval = new ArrayList<XGroupSyncStruct>( members.size() );
		String						groupServiceName = getGroupServiceName( action );

		try
		{
			IdcClient 			idcClient = getIdcClient();
			IdcContext 			userContext = getIdcContext();

			DataBinder			binder = null;

			if ( groupObject.getGroupOriginType() == XGroupOriginType.IDP )
			{
				binder = getDataBinderForIDPGroups( idcClient, groupServiceName );
			}
			else
			{
				binder = getDataBinder( idcClient, groupServiceName, userObject );
			}

			binder.putLocal( "item", getIDCGroupID( groupObject ) );

			if ( ( action.equals( XGroupSyncAction.ADD_GROUP_MEMBERS ) ) ||
				 ( action.equals( XGroupSyncAction.REMOVE_GROUP_MEMBERS ) ) ||
				 ( action.equals( XGroupSyncAction.GRANT_GROUP_PRIVILEGE ) ) ||
				 ( action.equals( XGroupSyncAction.REVOKE_GROUP_PRIVILEGE ) ) ||
				 ( action.equals( XGroupSyncAction.MODIFY_GROUP_PRIVILEGE ) ) )
			{
				binder.putLocal( "dMemberID", getMemberIDs( members.keySet() ) );
			}

			if ( role != null )
			{
				binder.putLocal( "dRoleName", getIDCGroupMemberRole( role ) );
			}

			binder.putLocal( "ignoreExistenceErrors", "true" );


			if ( sLogger.isDebugEnabled() )
			{
				sLogger.debug( "Binder object in the IDC request for SERVICE_{0}: {1}", groupServiceName, binder.toString() );
			}

			response = execute( idcClient, userContext, binder, exceptionResourceID, exceptionResourceArgs );

			DataBinder 			responseBinder = response.getResponseAsBinder();

			if ( ( responseBinder != null ) && ( responseBinder.getLocalData() != null ) )
			{
				DataResultSet		actionStatus = responseBinder.getResultSet( "ActionStatus" );

				if ( actionStatus != null )
				{
					List<DataObject> 	dataObjects = actionStatus.getRows();

					if ( sLogger.isDebugEnabled() )
					{
						sLogger.debug( "Embedded resultset 'actionstatus' size for SERVICE_{0} - {1}", groupServiceName, dataObjects.size() );
					}

					for ( DataObject dataObject : dataObjects )
					{
						String		idcGUID = dataObject.get( "dTargetUser" );

						XGroupSyncStruct groupSyncStruct =
							new XGroupSyncStruct( action,
												  idcGUID,
												  members.get( idcGUID ),
												  dataObject.getBoolean( "isSuccessful", false ),
												  String.valueOf( dataObject.getInteger( "StatusCode" ) ),
												  dataObject.get( "StatusMessage" ) );

						retval.add( groupSyncStruct );

						if ( sLogger.isDebugEnabled() )
						{
							sLogger.debug( "XGroupSyncStruct derived from 'actionstatus' dataobject {0}.", groupSyncStruct.toString() );
						}

					}
				}
				else
				{
					sLogger.warning( "Call to the Content Server returned null package info. Could not perform action {0}.", action.toString() );

					throw new XRuntimeException( exceptionResourceID, exceptionResourceArgs );
				}
			}
			else
			{
				sLogger.warning( "Call to the Content Server returned null local data. Could not perform action {0}.", action.toString() );

				throw new XRuntimeException( exceptionResourceID, exceptionResourceArgs );
			}

		}
		catch ( Throwable ex )
		{
			sLogger.error( XFormat.formatForThreadNoEx( exceptionResourceID, exceptionResourceArgs ) );
			sLogger.error( "Exception invoking service {0}.", groupServiceName, ex );

			int	size = exceptionResourceArgs.length + 1;
			Object[] args = new Object[ size ];

			for ( int i = 0; i < ( size - 1 ); i++ )
			{
				args[i] = exceptionResourceArgs[i];
			}

			args[size - 1] = ex;

			throw new XRuntimeException( exceptionResourceID, args );
		}
		finally
		{
			if ( response != null )
			{
				response.close();
			}
		}

		return retval;
	}

	public static List<XGroupSyncStruct> addGroupMembers( XUserObject userObject, XGroupObject groupObject, Map<XMemberObject, XConversationRole> memberObjects )
	{
		List<XGroupSyncStruct>			retval = null;
		Map<String, XMemberObject>		managers = new HashMap<String, XMemberObject>();
		Map<String, XMemberObject>		members = new HashMap<String, XMemberObject>();
		XUserObject						joinUser = null;

		for ( Map.Entry<XMemberObject, XConversationRole> entry : memberObjects.entrySet() )
		{
			XMemberObject		memberObject = entry.getKey();
			XConversationRole	role = entry.getValue();
			String				memberID;

			if ( memberObject instanceof XGroupObject )
			{
				XGroupObject groupMemberObject =  (XGroupObject) memberObject;

				memberID = groupMemberObject.getGroupID();
			}
			else
			{
				memberID = memberObject.getName();
			}

			if ( memberID != null )
			{
				if ( memberObject.equals( userObject ) &&
					 ( !memberObject.equals( groupObject.getOwnerUserObject() ) ) && // self join case except owner
					 ( groupObject.getGroupType().equals( PUBLIC_OPEN ) ) )
				{
					joinUser = XClass.uncheckedCast( (XUserObject) null, memberObject );
				}
				else if ( role.isGroupManager() )
				{
					managers.put( memberID, memberObject );
				}
				else
				{
					members.put( memberID, memberObject );
				}
			}
		}

		if ( joinUser != null )
		{
			XGroupSyncStruct syncStruct = joinGroup( joinUser, groupObject );

			if ( retval == null )
			{
				retval = new ArrayList<XGroupSyncStruct>();
			}

			retval.add( syncStruct );
		}

		if ( XCollections.isMapNotEmpty( managers ) )
		{
			List<XGroupSyncStruct> syncStructs = performGroupMembersAction( userObject,
																			groupObject,
																			managers,
																			GROUP_MANAGER,
																			XGroupSyncAction.ADD_GROUP_MEMBERS,
																			"waggle.server.integration.docs.idc.CouldNotAddMembersToGroup",   // I18N
																			groupObject.getName(),
																			groupObject.getID(),
																			groupObject.getGroupID() );

			if ( retval != null )
			{
				retval.addAll( syncStructs );
			}
			else
			{
				retval = syncStructs;
			}
		}

		if ( XCollections.isMapNotEmpty( members ) )
		{
			List<XGroupSyncStruct> syncStructs = performGroupMembersAction( userObject,
																			groupObject,
																			members,
																			GROUP_MEMBER,
																			XGroupSyncAction.ADD_GROUP_MEMBERS,
																			"waggle.server.integration.docs.idc.CouldNotAddMembersToGroup",    // I18N
																			groupObject.getName(),
																			groupObject.getID(),
																			groupObject.getGroupID() );

			if ( retval != null )
			{
				retval.addAll( syncStructs );
			}
			else
			{
				retval = syncStructs;
			}
		}

		return retval;
	}

	public static List<XGroupSyncStruct> removeGroupMembers( XUserObject userObject, XGroupObject groupObject, Set<XMemberObject> memberObjects )
	{
		List<XGroupSyncStruct>		retval = null;
		Map<String, XMemberObject>	members = new HashMap<String, XMemberObject>( memberObjects.size() );
		XUserObject					leaveUser = null;

		for ( XMemberObject memberObject : memberObjects )
		{
			String				memberID;

			if ( memberObject instanceof XGroupObject )
			{
				memberID = ( (XGroupObject) memberObject ).getGroupID();
			}
			else
			{
				memberID = memberObject.getName();
			}

			if ( memberObject.equals( userObject ) &&
				 !memberObject.equals( groupObject.getOwnerUserObject() ) &&
				 ( groupObject.getGroupType().equals( PUBLIC_OPEN ) ) )
			{
				leaveUser = XClass.uncheckedCast( (XUserObject) null, memberObject );
			}
			else
			{
				members.put( memberID, memberObject );
			}
		}

		if ( leaveUser != null )
		{
			XGroupSyncStruct syncStruct = leaveGroup( leaveUser, groupObject );
			retval = new ArrayList<XGroupSyncStruct>();
			retval.add( syncStruct );
		}

		if ( retval == null )
		{
			retval = new ArrayList<XGroupSyncStruct>();
		}

		retval.addAll( performGroupMembersAction( userObject,
												  groupObject,
												  members,
												  null,
												  XGroupSyncAction.REMOVE_GROUP_MEMBERS,
												  "waggle.server.integration.docs.idc.CouldNotRemoveMembersFromGroup", 	// I18N
												  groupObject.getName(),
												  groupObject.getID(),
												  groupObject.getGroupID() ) );

		return retval;
	}

	/**
	 * The API removes the users from Documents group.
	 *
	 * @param userObject User object.
	 * @param groupObject Group object.
	 * @param membersToRemoveFromDocs Set of group members - can be user or group.
	 * @return List of group sync struct
	 */
	public static List<XGroupSyncStruct> removeGroupMembersWithName( XUserObject userObject, XGroupObject groupObject, Set<String>	membersToRemoveFromDocs )
	{
		List<XGroupSyncStruct>		retval = null;
		Map<String, XMemberObject>	members = new HashMap<String, XMemberObject>();
		XUserObject					leaveUser = null;

		for ( String memberDetail : membersToRemoveFromDocs )
		{
			String				memberID = memberDetail;

			if( memberDetail.startsWith( "GS" ) )
			{
				XMemberObject		memberObject = XGroupObjectManager.findGroupObjectByGroupID( memberID );

				members.put( memberID, memberObject );
			}
			else
			{
				XUserObject			memberObject = XUsersObjectManager.findUserObject( memberDetail );

				if ( ( memberObject != null ) &&
					 memberObject.equals( userObject ) &&
					 !memberObject.equals( groupObject.getOwnerUserObject() ) &&
					 ( groupObject.getGroupType().equals( PUBLIC_OPEN ) ) )
				{
					leaveUser = XClass.uncheckedCast( (XUserObject) null, memberObject );
				}
				else
				{
					members.put( memberID, memberObject );
				}

			}
		}

		if ( leaveUser != null )
		{
			XGroupSyncStruct syncStruct = leaveGroup( leaveUser, groupObject );
			retval = new ArrayList<XGroupSyncStruct>();
			retval.add( syncStruct );
		}

		if ( retval == null )
		{
			retval = new ArrayList<XGroupSyncStruct>();
		}

		retval.addAll( performGroupMembersAction( userObject,
												  groupObject,
												  members,
												  null,
												  XGroupSyncAction.REMOVE_GROUP_MEMBERS,
												  "waggle.server.integration.docs.idc.CouldNotRemoveMembersFromGroup", 	// I18N
												  groupObject.getName(),
												  groupObject.getID(),
												  groupObject.getGroupID() ) );

		return retval;
	}

	public static List<XGroupSyncStruct> grantGroupPrivilege( XUserObject userObject, XGroupObject groupObject, Map<XMemberObject, XConversationRole> memberObjects )
	{
		List<XGroupSyncStruct>		retval = null;
		Map<String, XMemberObject>		managers = new HashMap<String, XMemberObject>();
		Map<String, XMemberObject>		members = new HashMap<String, XMemberObject>();

		for ( Map.Entry<XMemberObject, XConversationRole> entry : memberObjects.entrySet() )
		{
			XMemberObject		memberObject = entry.getKey();
			XConversationRole	role = entry.getValue();
			String				memberID;

			if ( memberObject instanceof XGroupObject )
			{
				memberID = ( (XGroupObject) memberObject ).getGroupID();
			}
			else
			{
				memberID = memberObject.getName();
			}

			if ( role.isGroupManager() )
			{
				managers.put( memberID, memberObject );
			}
			else
			{
				members.put( memberID, memberObject );
			}
		}

		if ( XCollections.isMapNotEmpty( managers ) )
		{
			retval = performGroupMembersAction( userObject,
												groupObject,
												managers,
												GROUP_MANAGER,
												XGroupSyncAction.GRANT_GROUP_PRIVILEGE,
												"waggle.server.integration.docs.idc.CouldNotGrantGroupPrivilege",    // I18N
												groupObject.getName(),
												groupObject.getID(),
												groupObject.getGroupID() );
		}

		if ( XCollections.isMapNotEmpty( members ) )
		{
			List<XGroupSyncStruct> syncStructs = performGroupMembersAction( userObject,
																			groupObject,
																			members,
																			GROUP_MEMBER,
																			XGroupSyncAction.GRANT_GROUP_PRIVILEGE,
																			"waggle.server.integration.docs.idc.CouldNotGrantGroupPrivilege",    // I18N
																			groupObject.getName(),
																			groupObject.getID(),
																			groupObject.getGroupID() );
			if ( retval != null )
			{
				retval.addAll( syncStructs );
			}
			else
			{
				retval = syncStructs;
			}
		}

		return retval;
	}

	public static List<XGroupSyncStruct> modifyGroupPrivilege( XUserObject userObject, XGroupObject groupObject, Map<XMemberObject, XConversationRole> memberObjects )
	{
		List<XGroupSyncStruct>		retval = null;
		Map<String, XMemberObject>		managers = new HashMap<String, XMemberObject>();
		Map<String, XMemberObject>		members = new HashMap<String, XMemberObject>();

		for ( Map.Entry<XMemberObject, XConversationRole> entry : memberObjects.entrySet() )
		{
			XMemberObject		memberObject = entry.getKey();
			XConversationRole	role = entry.getValue();
			String				memberID;

			if ( memberObject instanceof XGroupObject )
			{
				memberID = ( (XGroupObject) memberObject ).getGroupID();
			}
			else
			{
				memberID = memberObject.getName();
			}

			if ( role.isGroupManager() )
			{
				managers.put( memberID, memberObject );
			}
			else
			{
				members.put( memberID, memberObject );
			}
		}

		if ( XCollections.isMapNotEmpty( managers ) )
		{
			retval = performGroupMembersAction( userObject,
												groupObject,
												managers,
												GROUP_MANAGER,
												XGroupSyncAction.MODIFY_GROUP_PRIVILEGE,
												"waggle.server.integration.docs.idc.CouldNotModifyGroupPrivilege",    // I18N
												groupObject.getName(),
												groupObject.getID(),
												groupObject.getGroupID() );
		}

		if ( XCollections.isMapNotEmpty( members ) )
		{
//			List<XGroupSyncStruct> syncStructs = performGroupMembersAction( userObject,
//																			groupObject,
//																			members,
//																			GROUP_MEMBER,
//																			XGroupSyncAction.MODIFY_GROUP_PRIVILEGE,
//																			"waggle.server.integration.docs.idc.CouldNotModifyGroupPrivilege",    // I18N
//																			groupObject.getName(),
//																			groupObject.getID(),
//																			groupObject.getGroupID() );
			// MODIFY_GROUP_PRIVILEGE is not available for GROUP_MEMBER - so
			// just revoke
			List<XGroupSyncStruct> syncStructs =  performGroupMembersAction( userObject,
																			 groupObject,
																			 members,
																			 GROUP_MANAGER,
																			 XGroupSyncAction.REVOKE_GROUP_PRIVILEGE,
																			 "waggle.server.integration.docs.idc.CouldNotRevokeGroupPrivilege",	// I18N
																			 groupObject.getName(),
																			 groupObject.getID(),
																			 groupObject.getGroupID() );

			if ( retval != null )
			{
				retval.addAll( syncStructs );
			}
			else
			{
				retval = syncStructs;
			}
		}

		return retval;
	}

	public static List<XGroupSyncStruct> revokeGroupPrivilege( XUserObject userObject, XGroupObject groupObject, Set<XMemberObject> memberObjects )
	{
		List<XGroupSyncStruct>		retval;
		Map<String, XMemberObject>		members = new HashMap<String, XMemberObject>( memberObjects.size() );

		for ( XMemberObject memberObject : memberObjects )
		{
			String				memberID;

			if ( memberObject instanceof XGroupObject )
			{
				memberID = ( (XGroupObject) memberObject ).getGroupID();
			}
			else
			{
				memberID = memberObject.getName();
			}

			members.put( memberID, memberObject );
		}

		retval = performGroupMembersAction( userObject,
											groupObject,
											members,
											GROUP_MANAGER,
											XGroupSyncAction.REVOKE_GROUP_PRIVILEGE,
											"waggle.server.integration.docs.idc.CouldNotRevokeGroupPrivilege",	// I18N
											groupObject.getName(),
											groupObject.getID(),
											groupObject.getGroupID() );

		return retval;
	}

	/**
	 * Get the file version from content server using FLD_INFO IDC service.
	 *
	 * @param contentServerGUID 	The GUID of the document in Content Server.
	 * @return 						The file version.
	 */
	public static String getContentFileVersion( final String contentServerGUID )

	{
		ServiceResponse 	response = null;
		String				retVal = null;

		try
		{
			IdcClient 		idcClient = getIdcClient();
			IdcContext 		userContext = getIdcContext();
			DataBinder 		binder = getDataBinder( idcClient, SERVICE_FLD_INFO );

			binder.putLocal( "item", ITEM_PREFIX + contentServerGUID );

			if ( sLogger.isDebugEnabled() )
			{
				sLogger.debug( "Binder object in the IDC request for FLD_INFO: {0}", binder.toString() );
			}

			response = execute( idcClient, userContext, binder, "waggle.server.integration.docs.idc.CouldNotGetFileInfo" );

			DataBinder		respBinder = response.getResponseAsBinder();

			DataResultSet	resultSet = respBinder.getResultSet( "FileInfo" );

			if ( resultSet == null )
			{
				if ( sLogger.isDebugEnabled() )
				{
					sLogger.debug( "No file details found." );
				}

				return retVal;
			}
			else
			{
				if ( sLogger.isDebugEnabled() )
				{
					sLogger.debug( "Total {0} files found for GUID: {1}", resultSet.getRows().size(), contentServerGUID );
				}
				List<DataObject>		fileDataObjectList = resultSet.getRows();

				DataObject				fileDataObject = fileDataObjectList.get( 0 );

				retVal = fileDataObject.get( FLD_INFO_FILE_VERSION_PARAM );

				if ( sLogger.isDebugEnabled() )
				{
					sLogger.debug( "Current file version for GUID: {0} is {1}", contentServerGUID, retVal );
				}
			}

		}
		catch ( Throwable ex )
		{
			sLogger.error( "Exception in invoking {0} service", SERVICE_FLD_INFO, ex );

			throw new XRuntimeException( "waggle.server.integration.docs.idc.CouldNotGetFileInfo", ex );
		}
		finally
		{
			if ( response != null )
			{
				response.close();
			}
		}

		if ( sLogger.isDebugEnabled() )
		{
			sLogger.debug( "File version received from Content Server: {0}", retVal );
		}

		return retVal;
	}

	/**
	 * Get users having access to the repository.
	 *
	 * @param repositoryID Repository ID.
	 * @param userObject User object to impersonate.
	 *
	 * @return Set of user having access to the repository.
	 */
	public static Set<String> getUsersFromRepository( String repositoryID, XUserObject userObject )
	{
		ServiceResponse 	response = null;
		Set<String>			retval = new TreeSet<>();

		try
		{
			IdcClient idcClient = getIdcClient();
			IdcContext userContext = getIdcContext();
			DataBinder binder = getDataBinder( idcClient, SERVICE_SHARED_REPO_USERS, userObject );

			binder.putLocal( "repository",
					repositoryID.startsWith( FOLDER_PREFIX ) ? repositoryID : FOLDER_PREFIX + repositoryID );

			response = execute( idcClient, userContext, binder, "waggle.server.integration.docs.idc.CouldNotGetUsersFromRepo" );

			DataBinder		respBinder = response.getResponseAsBinder();

			DataResultSet	resultSet = respBinder.getResultSet( "SharedUsers" );

			if ( resultSet != null )
			{
				List<DataObject>	objectRows = resultSet.getRows();

				for ( DataObject	object : objectRows )
				{
					retval.add( object.get( "dUserIDLoginName" ) );
				}
			}
		}
		catch ( Throwable ex )
		{
			sLogger.error( "Exception in invoking {0} service", SERVICE_SHARED_REPO_USERS, ex );

			throw new XRuntimeException( "waggle.server.integration.docs.idc.CouldNotGetUsersFromRepo", repositoryID, ex );
		}
		finally
		{
			if ( response != null )
			{
				response.close();
			}
		}

		return retval;
	}

	/**
	 * Get users having access to the folder.
	 *
	 * @param folderID Folder ID.
	 * @param userObject  User object to impersonate.
	 *
	 * @return Users having access to the folder.
	 */
	public static Set<String> getUsersFromFolder( String folderID, XUserObject userObject )
	{
		ServiceResponse 	response = null;
		Set<String>			retval = new TreeSet<>();

		try
		{
			IdcClient idcClient = getIdcClient();
			IdcContext userContext = getIdcContext();
			DataBinder binder = getDataBinder( idcClient, SERVICE_SHARED_FOLDER_USERS, userObject );

			binder.putLocal( "item", folderID.startsWith( FOLDER_PREFIX ) ? folderID : FOLDER_PREFIX + folderID );

			response = execute( idcClient, userContext, binder, "waggle.server.integration.docs.idc.CouldNotGetUsersFromFolder" );

			DataBinder		respBinder = response.getResponseAsBinder();

			DataResultSet	resultSet = respBinder.getResultSet( "SharedFolderUsers" );

			if ( resultSet != null )
			{
				List<DataObject>	objectRows = resultSet.getRows();

				for ( DataObject	object : objectRows )
				{
					retval.add( object.get( "dUserIDLoginName" ) );
				}
			}
		}
		catch ( Throwable ex )
		{
			sLogger.error( "Exception in invoking {0} service", SERVICE_SHARED_FOLDER_USERS, ex );

			throw new XRuntimeException( "waggle.server.integration.docs.idc.CouldNotGetUsersFromFolder", folderID, ex );
		}
		finally
		{
			if ( response != null )
			{
				response.close();
			}
		}

		return retval;
	}

	public static XResourceInfo getFolderInfo( final String artifactID, final XUserObject userObject )

	{
		ServiceResponse 		response = null;
		XResourceInfo 			retval = null;

		try
		{
			IdcClient idcClient = getIdcClient();
			IdcContext userContext = getIdcContext();
			DataBinder binder = getDataBinder( idcClient, SERVICE_FLD_INFO, userObject );

			binder.putLocal( "item", artifactID.startsWith( FOLDER_PREFIX ) ? artifactID : FOLDER_PREFIX + artifactID );

			if ( sLogger.isDebugEnabled() )
			{
				sLogger.debug( "Binder object in the IDC request for FLD_INFO: {0}", binder.toString() );
			}

			response = execute( idcClient, userContext, binder, "waggle.server.integration.docs.idc.CouldNotGetFolderInfo" );

			DataBinder responseBinder = response.getResponseAsBinder();
			DataObject dataObject = responseBinder.getLocalData();

			String roleName = dataObject.get( "dRoleName" );

			retval = new XResourceInfo();
			retval.setRole( XAccessControlUtil.mapContentRoleToConvRole( roleName ) );

			if ( sLogger.isDebugEnabled() )
			{
				sLogger.debug( "User's role name: {0}", roleName );
			}

		}
		catch ( Throwable ex )
		{
			sLogger.error( "Exception in invoking {0} service", SERVICE_FLD_INFO, ex );

			throw new XRuntimeException( "waggle.server.integration.docs.idc.CouldNotGetFolderInfo", ex );
		}
		finally
		{
			if ( response != null )
			{
				response.close();
			}
		}

		return retval;
	}

	public static XResourceInfo getFileInfo( final String artifactID, final XUserObject userObject )

	{
		ServiceResponse 		response = null;
		XResourceInfo 			retval = null;

		try
		{
			IdcClient idcClient = getIdcClient();
			IdcContext userContext = getIdcContext();
			DataBinder binder = getDataBinder( idcClient, SERVICE_FLD_INFO, userObject );

			binder.putLocal( "item", artifactID.startsWith( ITEM_PREFIX ) ? artifactID : ITEM_PREFIX + artifactID );

			if ( sLogger.isDebugEnabled() )
			{
				sLogger.debug( "Binder object in the IDC request for FLD_INFO: {0}", binder.toString() );
			}

			response = execute( idcClient, userContext, binder, "waggle.server.integration.docs.idc.CouldNotGetFileInfo" );

			DataBinder responseBinder = response.getResponseAsBinder();
			DataObject dataObject = responseBinder.getLocalData();

			String roleName = dataObject.get( "dRoleName" );

			retval = new XResourceInfo();
			retval.setRole( XAccessControlUtil.mapContentRoleToConvRole( roleName ) );

			if ( sLogger.isDebugEnabled() )
			{
				sLogger.debug( "User's role name: {0}", roleName );
			}

		}
		catch ( Throwable ex )
		{
			sLogger.error( "Exception in invoking {0} service", SERVICE_FLD_INFO, ex );

			throw new XRuntimeException( "waggle.server.integration.docs.idc.CouldNotGetFileInfo", ex );
		}
		finally
		{
			if ( response != null )
			{
				response.close();
			}
		}

		return retval;
	}
}
