/* Copyright (c) 2010, 2020, Oracle and/or its affiliates. All rights reserved. */

package waggle.server.modules.group.utils;

import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import waggle.common.modules.conversation.enums.XConversationRole;
import waggle.common.modules.group.enums.XGroupType;
import waggle.common.modules.group.infos.XGroupMemberChangeInfo;
import waggle.common.modules.member.infos.XMemberInfo;
import waggle.core.annotations.XDisallowInstantiation;
import waggle.core.events.XEventsManager;
import waggle.core.exceptions.XExceptionUtils;
import waggle.core.exceptions.XRuntimeException;
import waggle.core.id.XObjectID;
import waggle.core.log.XLog;
import waggle.core.properties.XPersistentPropertiesManager;
import waggle.core.thread.XThreadLocalManager;
import waggle.core.utils.XCollections;
import waggle.core.utils.XString;
import waggle.server.activity.XActivityAction;
import waggle.server.activity.XActivityManager;
import waggle.server.executor.XExecutorManager;
import waggle.server.executor.XExecutorRunnable;
import waggle.server.groupsync.XGroupSyncUtils;
import waggle.server.integration.docs.XIdcUtils;
import waggle.server.modules.conversation.database.conversation.XConversationObject;
import waggle.server.modules.conversation.database.conversation.XConversationObjectManager;
import waggle.server.modules.conversation.utils.XConversationMemberUtils;
import waggle.server.modules.group.XGroupModuleServerEvents;
import waggle.server.modules.group.database.group.XGroupObject;
import waggle.server.modules.group.database.group.XGroupObjectManager;
import waggle.server.modules.group.database.groupmembers.XGroupMembersObject;
import waggle.server.modules.group.database.syncbacklog.XGroupSyncBacklogObject;
import waggle.server.modules.group.database.syncbacklog.XGroupSyncBacklogObjectManager;
import waggle.server.modules.group.database.syncbacklog.XGroupSyncOperationType;
import waggle.server.modules.group.notifications.XGroupMembershipChangedNotification;
import waggle.server.modules.hm.database.hierarchicalmembers.XHierarchicalMembersObject;
import waggle.server.modules.member.database.member.XMemberObject;
import waggle.server.modules.member.database.member.XMemberObjectManager;
import waggle.server.modules.user.database.user.XUserObject;
import waggle.server.modules.user.database.user.XUserObjectManager;
import waggle.server.modules.user.database.users.XUsersObjectManager;
import waggle.server.modules.user.exceptions.XOutsiderOperationNotAllowedException;
import waggle.server.servlet.session.XSessionManager;
import waggle.server.transaction.XTransactionRunnable;
import waggle.server.utils.XValidate;

/**
 * Group Member Utilities.
 */
@XDisallowInstantiation
public final class XGroupMemberUtils
{
	private static final XLog 	sLogger = XLog.getLogger();

	private XGroupMemberUtils()
	{
	}

	private static final class PendingGroupMembersChangedNotificationData
	{
		private XGroupObject							fGroupObject;
		private Map<XMemberObject, XConversationRole>	fAddedMemberObjects;
		private Set<XMemberObject>						fRemovedMemberObjects;

		private PendingGroupMembersChangedNotificationData( XGroupObject groupObject, Map<XMemberObject, XConversationRole> addedMemberObjects, Set<XMemberObject> removedMemberObjects )
		{
			fGroupObject = groupObject;
			fAddedMemberObjects = addedMemberObjects;
			fRemovedMemberObjects = removedMemberObjects;
		}
	}

	/**
	 * Change one Group's membership.
	 *
	 * @param groupObject The Group.
	 * @param changeInfos The changes to the Group's membership.
	 * @param syncGroupCreate Sync group create to IDC.
	 */
	public static void changeMembers( XGroupObject groupObject, List<XGroupMemberChangeInfo> changeInfos, final boolean syncGroupCreate )
	{
		changeMembers( groupObject, changeInfos, syncGroupCreate, false );
	}

	/**
	 * Change one Group's membership.
	 *
	 * @param groupObject The Group.
	 * @param changeInfos The changes to the Group's membership.
	 * @param syncGroupCreate Sync group create to IDC.
	 * @param skipOnException Skip processing current member-change if it fails and continue processing others.
	 */
	public static void changeMembers( XGroupObject groupObject, List<XGroupMemberChangeInfo> changeInfos, final boolean syncGroupCreate, boolean skipOnException )
	{
		Map<XGroupObject,List<XGroupMemberChangeInfo>> changes = new HashMap<XGroupObject,List<XGroupMemberChangeInfo>>();

		changes.put( groupObject, changeInfos );

		changeMembers( changes, false, syncGroupCreate, skipOnException );
	}

	/**
	 * Change many Group's membership.
	 *
	 * @param changes The changes to many Group's memberships.
	 * @param sync True if this is a Group sync operation to limit some of the checking.
	 * @param syncGroupCreate True if Group create should be synced as well.
	 * @param skipOnException Skip processing current member-change if it fails and continue processing others.
	 */
	public static void changeMembers( Map<XGroupObject,List<XGroupMemberChangeInfo>> changes, boolean sync, final boolean syncGroupCreate, boolean skipOnException )
	{
		changeMembers( changes, sync, syncGroupCreate, true, skipOnException );
	}

	/**
	 * Change many Group's membership.
	 *
	 * @param changes The changes to many Group's memberships.
	 * @param sync True if this is a Group sync operation to limit some of the checking.
	 * @param syncGroupCreate True if Group create should be synced as well.
	 * @param syncGroupMembers True if Group members should be synced as well.
	 * @param skipOnException Skip processing current member-change if it fails and continue processing others.
	 */
	public static void changeMembers( Map<XGroupObject,List<XGroupMemberChangeInfo>> changes, boolean sync, final boolean syncGroupCreate, boolean syncGroupMembers, boolean skipOnException )
	{
		long	startTime = System.currentTimeMillis();

		if ( ( syncGroupCreate ) && ( changes.size() != 1 ) )
		{
			throw new XRuntimeException( "waggle.server.group.sync.create.error" );
		}

		sLogger.warning( "Starting member changes for {0} group/s. Flags : sync:{1}, syncGroupCreate:{2}, syncGroupMembers:{3}, skipOnException:{4}.",
						 changes.size(), sync, syncGroupCreate, syncGroupMembers, skipOnException  );

		XUserObject					changingUserObject = XSessionManager.getUserObject();

		Map<XGroupObject, Map<XMemberObject, XConversationRole>> addedMembersToSync = new HashMap<XGroupObject, Map<XMemberObject,XConversationRole>>();
		Map<XGroupObject, Set<XMemberObject>> removedMembersToSync = new HashMap<XGroupObject, Set<XMemberObject>>();
		// map from group object to map of members
		// map of members maps member to list of 2 conversation roles - original and new
		Map<XGroupObject, Map<XMemberObject, XConversationRole>> modifiedMembersToSync = new HashMap<XGroupObject, Map<XMemberObject,XConversationRole>>();


		////////////////////////////////////////////////////////////////////////
		// collect all explicitly changed Groups and all other affected Groups
		////////////////////////////////////////////////////////////////////////

		Set<XGroupObject>			groupObjects = new HashSet<XGroupObject>( changes.keySet() );

		for ( XGroupObject groupObject : changes.keySet() )
		{
			groupObjects.addAll( groupObject.getMemberGroupsObject().getGroupObjects() );
		}

		if ( sLogger.isDebugEnabled() )
		{
			sLogger.debug( "*** 0.1 Collected all affected gorups, name=groupObjects, total time = " + ( System.currentTimeMillis() - startTime ) );
		}

		////////////////////////////////////////////////////////////////////////
		// get initial DIRECT members and EXPLODED (DIRECT/INDIRECT) user membership
		////////////////////////////////////////////////////////////////////////

		Map<XGroupObject,Set<XMemberObject>>	initialGroupMemberObjects = new HashMap<XGroupObject,Set<XMemberObject>>();
		Map<XGroupObject,Set<XMemberObject>>	initialGroupAllMemberObjects = new HashMap<XGroupObject,Set<XMemberObject>>();

		for ( XGroupObject groupObject : groupObjects )
		{
			XGroupMembersObject		membersObject = groupObject.getGroupMembersObject();

			// save the initial direct members and the exploded user membership for this Group

			initialGroupMemberObjects.put( groupObject, membersObject.getMemberObjects().keySet() );
			initialGroupAllMemberObjects.put( groupObject, membersObject.getAllMemberObjects().keySet() );
		}

		if ( sLogger.isDebugEnabled() )
		{
			sLogger.debug( "*** 0.2 Collected initial direct/inderict membership, total time = " + ( System.currentTimeMillis() - startTime ) );
		}

		////////////////////////////////////////////////////////////////////////
		// collect all Conversations in which these Groups are either DIRECT or INDIRECT members
		////////////////////////////////////////////////////////////////////////

		Set<XConversationObject>	conversationObjects = new HashSet<XConversationObject>();

		for ( XGroupObject groupObject : groupObjects )
		{
			conversationObjects.addAll( groupObject.getMemberConversationsObject().getConversationObjects() );
		}

		if ( sLogger.isDebugEnabled() )
		{
			sLogger.debug( "*** 0.3 Collected all member conversation (direct/indirect), name=conversationObjects, total time = " + ( System.currentTimeMillis() - startTime ) );
		}

		////////////////////////////////////////////////////////////////////////
		// collect initial Conversation exploded User membership
		////////////////////////////////////////////////////////////////////////

		Map<XConversationObject,Map<XMemberObject,XConversationRole>>	initialConversationRoles = new HashMap<XConversationObject,Map<XMemberObject,XConversationRole>>();

		for ( XConversationObject conversationObject : conversationObjects )
		{
			initialConversationRoles.put( conversationObject, XConversationMemberUtils.getAllMemberCalculatedRoles( conversationObject ) );
		}

		if ( sLogger.isDebugEnabled() )
		{
			sLogger.debug( "*** 0.4 Collected exploded conversation user membership, name=initialConversationRoles" );
		}

		sLogger.warning( "*** 1. Collected all needed details. Total time taken till now : " + ( System.currentTimeMillis() - startTime ) );

		////////////////////////////////////////////////////////////////////////
		// process all specified direct member changes
		////////////////////////////////////////////////////////////////////////

		Map<XGroupObject, PendingGroupMembersChangedNotificationData>		pendingGroupMembersChangedNotifications =
			new HashMap<XGroupObject, PendingGroupMembersChangedNotificationData>();

		for ( Map.Entry<XGroupObject,List<XGroupMemberChangeInfo>> entry : changes.entrySet() )
		{
			XGroupObject					groupObject = entry.getKey();
			List<XGroupMemberChangeInfo>	changeInfos = entry.getValue();

			// get Group members object

			XGroupMembersObject				membersObject = groupObject.getGroupMembersObject();

			// process Group member changes

			Set<XMemberObject>						groupRemovedMembers = new HashSet<XMemberObject>();
			Map<XMemberObject, XConversationRole>	groupAddedMembers = new HashMap<XMemberObject, XConversationRole>();
			Map<XMemberObject, XConversationRole> groupModifiedMembers = new HashMap<XMemberObject, XConversationRole>();

			for ( XGroupMemberChangeInfo changeInfo : changeInfos )
			{
				try
				{
					// process Member additions

					if ( !changeInfo.MemberDelete )
					{
						Map<XMemberObject,XConversationRole> memberObjects;

						// A) the User or Group already exists

						if ( changeInfo.MemberID != null )
						{
							XMemberObject
								memberObject =
								XMemberObjectManager.getMemberObject(
									changeInfo.MemberID );

							if ( memberObject == null )
							{
								throw new XRuntimeException( "waggle.modules.group.CouldNotFindMember" );
							}

							try
							{
								if ( memberObject instanceof XUserObject )
								{
									XValidate.checkNotOutsider( (XUserObject) memberObject );
								}
							}
							catch ( XOutsiderOperationNotAllowedException ex )
							{
								throw new XRuntimeException( "waggle.modules.group.OutsiderNotAllowedInGroups", memberObject.getName() );
							}

							memberObjects = new HashMap<XMemberObject,XConversationRole>();
							memberObjects.put( memberObject, changeInfo.MemberRole );
						}

						// A') Group already exists and is specified by string GroupID
						else if ( XString.isNotBlank( changeInfo.MemberGroupID ) )
						{
							XGroupObject memberGroupObject = XGroupObjectManager.getGroupObjectByGroupID( changeInfo.MemberGroupID );

							if ( memberGroupObject == null )
							{
								throw new XRuntimeException( "waggle.modules.group.CouldNotFindGroup", changeInfo.MemberGroupID );
							}

							memberObjects = new HashMap<XMemberObject,XConversationRole>();
							memberObjects.put( memberGroupObject, changeInfo.MemberRole );
						}

						// B) the User does not exist .. find or create the User

// Once required RealmID, but do not believe it should be required now. See #13905193 for discussion.
//					else if ( ( addInfo.MemberUserRealmID != null ) && ( addInfo.MemberUserName != null ) )
						else if ( changeInfo.MemberUserName != null )
						{
							XUserObject
								memberObject =
								XUserObjectManager.findShadowOrCreateUserObject(
									changeInfo.MemberUserRealmID,
									changeInfo.MemberUserName,
									changeInfo.MemberSendInvitation );

							if ( memberObject == null )
							{
								throw new XRuntimeException( "waggle.modules.group.CouldNotFindUser", changeInfo.MemberUserName );
							}

							try
							{
								XValidate.checkNotOutsider( (XUserObject) memberObject );
							}
							catch ( XOutsiderOperationNotAllowedException ex )
							{
								throw new XRuntimeException( "waggle.modules.group.OutsiderNotAllowedInGroups", memberObject.getName() );
							}

							memberObjects = new HashMap<XMemberObject,XConversationRole>();
							memberObjects.put( memberObject, changeInfo.MemberRole );
						}

						// C) the Group does not exist .. find or create the Group

// Once required RealmID, but do not believe it should be required now. See #13905193 for discussion.
//					else if ( ( addInfo.MemberGroupRealmID != null ) && ( addInfo.MemberGroupName != null ) )
						else if ( changeInfo.MemberGroupName != null )
						{
							XMemberObject
								memberObject =
								XGroupObjectManager.findOrShadowGroupObject(
									changeInfo.MemberGroupRealmID,
									changeInfo.MemberGroupName );

							if ( memberObject == null )
							{
								throw new XRuntimeException( "waggle.modules.group.CouldNotFindGroup", changeInfo.MemberGroupName );
							}

							memberObjects = new HashMap<XMemberObject,XConversationRole>();
							memberObjects.put( memberObject, changeInfo.MemberRole );
						}

						// D) try to find a Conversation by ID .. use the direct members

						else if ( changeInfo.MemberConversationID != null )
						{
							XConversationObject
								memberObject =
								XConversationObjectManager.getConversationObject(
									changeInfo.MemberConversationID );

							// check access

							XValidate.checkRoleDiscoverer( memberObject );

							// get members

							Set<XMemberObject> conversationMembers = memberObject.getConversationMembersObject().getMemberObjects();

							memberObjects = new HashMap<XMemberObject,XConversationRole>( conversationMembers.size() );

							for ( XMemberObject convMemberObject : conversationMembers )
							{
								memberObjects.put( convMemberObject, changeInfo.MemberRole );
							}
						}

						// no valid member locator specified

						else
						{
							throw new XRuntimeException( "waggle.modules.group.NoMemberSpecified" );
						}

						// process the members to be added

						for ( Map.Entry<XMemberObject,XConversationRole> memberEntry : memberObjects.entrySet() )
						{
							XMemberObject entryMemberObject = memberEntry.getKey();
							XConversationRole entryMemberRole = memberEntry.getValue();

							// perform some checks only if NOT a sync operation

							if ( !sync )
							{
								// make sure this direct member can be added

								if ( !entryMemberObject.isEnabledAndRealmEnabled() )
								{
									throw new XRuntimeException( "waggle.modules.group.MemberDisabled", entryMemberObject.getDisplayName() );
								}
							}

							// if a Group was specified .. make sure the Group has had an initial sync to its Realm

							if ( entryMemberObject instanceof XGroupObject )
							{
								XGroupSyncUtils.syncExternalGroup( (XGroupObject) entryMemberObject, false );
							}

							// add the Member to the Group

							XValidate.checkPrivilegeAddGroupMember( groupObject, entryMemberObject, changingUserObject, entryMemberRole );

							XConversationRole conversationRole = membersObject.addMemberObject( entryMemberObject, entryMemberRole, changingUserObject, new Date() );

							// log that a member was actually added

							if ( !conversationRole.equals( XConversationRole.NONE ) )
							{
								XActivityManager.getInstance().log(
									XActivityAction.GROUP_MEMBER_ADDED,
									groupObject,
									entryMemberObject,
									conversationRole.toString() );

								groupAddedMembers.put( entryMemberObject, entryMemberRole );
							}
						}
					}

					// process member removals

					else
					{
						XMemberObject memberObject;

						// A) the Member (User or Group) already exists

						if ( changeInfo.MemberID != null )
						{
							memberObject = XMemberObjectManager.findMemberObject( changeInfo.MemberID );

							if ( memberObject == null )
							{
								throw new XRuntimeException( "waggle.modules.group.CouldNotFindMember" );
							}
						}

						// A') the Group specified by string GroupID already exists

						else if ( XString.isNotBlank( changeInfo.MemberGroupID ) )
						{
							memberObject = XGroupObjectManager.findGroupObjectByGroupID( changeInfo.MemberGroupID );

							if ( memberObject == null )
							{
								throw new XRuntimeException( "waggle.modules.group.CouldNotFindGroup", changeInfo.MemberGroupID );
							}
						}

						// B) try to find a User by name

						else if ( changeInfo.MemberUserName != null )
						{
							memberObject = XUsersObjectManager.findUserObject( changeInfo.MemberUserName );

							if ( memberObject == null )
							{
								throw new XRuntimeException( "waggle.modules.group.CouldNotFindUser", changeInfo.MemberUserName );
							}
						}

						// no valid member locator specified

						else
						{
							throw new XRuntimeException( "waggle.modules.group.NoMemberSpecified" );
						}

						// remove the Member from the Group

						XValidate.checkPrivilegeRemoveGroupMember( groupObject, memberObject, changingUserObject );

						XConversationRole memberRemovedRole = membersObject.removeMemberObject( memberObject, changingUserObject );

						// log that a member was actually removed

						if ( !memberRemovedRole.equals( XConversationRole.NONE ) )
						{
							XActivityManager.getInstance().log(
								XActivityAction.GROUP_MEMBER_REMOVED,
								groupObject,
								memberObject );

							groupRemovedMembers.add( memberObject );
						}
					}
				}
				catch ( Throwable t )
				{
					String 	groupMemberChangeInfo = "XGroupMemberChangeInfo{" +
													  "MemberID=" + changeInfo.MemberID +
													  ", MemberUserName='" + changeInfo.MemberUserName + '\'' +
													  ", MemberUserRealmID=" + changeInfo.MemberUserRealmID +
													  ", MemberGroupName='" + changeInfo.MemberGroupName + '\'' +
													  ", MemberGroupID='" + changeInfo.MemberGroupID + '\'' +
													  ", MemberGroupRealmID=" + changeInfo.MemberGroupRealmID +
													  ", MemberConversationID=" + changeInfo.MemberConversationID +
													  ", MemberSendInvitation=" + changeInfo.MemberSendInvitation +
													  ", MemberDelete=" + changeInfo.MemberDelete +
													  ", MemberRole=" + changeInfo.MemberRole +
													  '}';

					sLogger.warning( "Failed while processing member change info for group {0}.\n Info: {1}",
									 groupObject.getID(), groupMemberChangeInfo, t );

					XActivityManager.getInstance().log(
						XActivityAction.GROUP_MEMBER_CHANGE_FAILED,
						groupObject, null, null,
						null, null, groupMemberChangeInfo, null, null,
						XExceptionUtils.getMinifiedStackTraceAsString( t ) );

					if ( !skipOnException )
					{
						throw t;
					}
				}
			}

			// check for cycles
			Set<XGroupObject> parentGroups = groupObject.getMemberGroupsObject().getGroupObjects();

			// add self to parent groups as well so we check against adding a group to itself
			parentGroups.add( groupObject );

			Set<XMemberObject> addedMembers = groupAddedMembers.keySet();
			boolean isCycle = false;
			StringBuilder cycleCandidates = new StringBuilder();

			for ( XMemberObject memberObject : addedMembers )
			{
				if ( ( memberObject instanceof XGroupObject ) && ( parentGroups.contains( memberObject ) ) )
				{
					if ( !isCycle )
					{
						isCycle = true;
						cycleCandidates.append( memberObject.getName() );
					}
					else
					{
						cycleCandidates.append( ", " ).append( memberObject.getName() );
					}
				}
			}

			if ( isCycle )
			{
				throw new XRuntimeException( "waggle.modules.group.CycleDetected", groupObject.getName(), cycleCandidates.toString() );
			}

			// use the initial member objects and added objects to figure out changed objects for sync
			if ( !syncGroupCreate )
			{
				Set<XMemberObject> initialMembers = initialGroupMemberObjects.get( groupObject );
				for ( Map.Entry<XMemberObject,XConversationRole> addedMemberEntry : groupAddedMembers.entrySet() )
				{
					XMemberObject memberKey = addedMemberEntry.getKey();
					XConversationRole roleValue = addedMemberEntry.getValue();

					if ( initialMembers.contains( memberKey ) )
					{
						groupModifiedMembers.put( memberKey, roleValue );
					}
				}

				Map<XMemberObject,XConversationRole> copyOfAddedMembers = XCollections.copyOfMap( groupAddedMembers );

				for ( XMemberObject modifiedMember : groupModifiedMembers.keySet() )
				{
					copyOfAddedMembers.remove( modifiedMember );
				}

				addedMembersToSync.put( groupObject, copyOfAddedMembers );
			}
			else
			{
				addedMembersToSync.put( groupObject, groupAddedMembers );
			}

			removedMembersToSync.put( groupObject, groupRemovedMembers );
			modifiedMembersToSync.put( groupObject, groupModifiedMembers );

			sLogger.warning( "*** 2. Processed all member changes in groups. Total time taken till now : " + ( System.currentTimeMillis() - startTime ) );

			/////////////////////////////////////////////////////////////////
			// process changes to the direct members of a Group
			/////////////////////////////////////////////////////////////////

			// get the initial set of DIRECT members

			Set<XMemberObject> 		initialMemberObjects = initialGroupMemberObjects.get( groupObject );

			// get the final set of DIRECT members

			Set<XMemberObject> 		finalMemberObjects = membersObject.getMemberObjects().keySet();

			// calculate added and removed DIRECT members

			Set<XMemberObject>		removedMemberObjects = new HashSet<XMemberObject>( initialMemberObjects );
			Set<XMemberObject>		addedMemberObjects = new HashSet<XMemberObject>( finalMemberObjects );

			removedMemberObjects.removeAll( finalMemberObjects );
			addedMemberObjects.removeAll( initialMemberObjects );

			// finally add any members who only had their roles changed
			addedMemberObjects.addAll( groupAddedMembers.keySet() );

			// was there any change to the direct membership

			if ( !addedMemberObjects.isEmpty() || !removedMemberObjects.isEmpty() )
			{
				// if we fire the event now, then the XGroupInfo.GroupNUsers value will be wrong (it will
				// have the previous value), because the value is actually updated later in this same method
				// with the line:
				//
				// 			membersObject.setNumberAccessibleUsers( numberOfAccessibleUsers );
				//
				// so for now just accumulate the events and then fire them later, after the number of
				// accessible users value has been updated.
				//
				// there must be a cleaner way to do this, but it's not obvious to me.

//				// fire event .. there were changes to the direct members of this Group
//
//				XEventsManager.fire( XGroupModuleServerEvents.class ).notifyGroupMembersChanged(
//					groupObject,
//					addedMemberObjects,
//					removedMemberObjects );

				Map<XMemberObject, XConversationRole> addedMemberObjectMap = XCollections.newHashMap( addedMemberObjects.size() );
				Map<XMemberObject, XConversationRole> currentMemberMap = membersObject.getMemberObjects();

				for ( XMemberObject memberObject : addedMemberObjects )
				{
					addedMemberObjectMap.put( memberObject, currentMemberMap.get( memberObject ) );
				}

				pendingGroupMembersChangedNotifications.put(
					groupObject,
					new PendingGroupMembersChangedNotificationData(
						groupObject,
						addedMemberObjectMap,
						removedMemberObjects ) );
			}
		}

		sLogger.warning( "*** 3. Processed all changes in direct members. Total time taken till now : " + ( System.currentTimeMillis() - startTime ) );

		/////////////////////////////////////////////////////////////////////////////////////////////////////////
		// for each Group that changed or is affected process exploded User membership changes
		/////////////////////////////////////////////////////////////////////////////////////////////////////////

		for ( XGroupObject groupObject : groupObjects )
		{
			// get members object

			XGroupMembersObject		membersObject = groupObject.getGroupMembersObject();

			// get initial exploded Users and final exploded Users

			Set<XMemberObject> 		initialMemberObjects = initialGroupAllMemberObjects.get( groupObject );
			Set<XMemberObject> 		finalMemberObjects = membersObject.getAllMemberObjects().keySet();

			// calculate added and removed exploded users

			Set<XMemberObject>		removedMemberObjects = new HashSet<XMemberObject>( initialMemberObjects );
			Set<XMemberObject>		addedMemberObjects = new HashSet<XMemberObject>( finalMemberObjects );

			removedMemberObjects.removeAll( finalMemberObjects );
			addedMemberObjects.removeAll( initialMemberObjects );

			// save the number of accessible Users

			int			numberOfAccessibleUsers = 0;

			for ( XMemberObject memberObject : finalMemberObjects )
			{
				if ( memberObject instanceof XUserObject )
				{
					numberOfAccessibleUsers++;
				}
			}

			membersObject.setNumberAccessibleUsers( numberOfAccessibleUsers );

			// collect added and removed Users

			Set<XUserObject>						removedUserObjects = new HashSet<XUserObject>();
			Map<XUserObject, XConversationRole>		addedUserObjects = XCollections.newHashMap( 1 );
			Map<XMemberObject, XConversationRole>	currentMemberMap = membersObject.getMemberObjects();

			// process and notify Users added to the Group

			for ( XMemberObject addedMemberObject : addedMemberObjects )
			{
				// add the Group to the User

				addedMemberObject.getMemberGroupsObject().addGroupObject( groupObject );

				if ( addedMemberObject instanceof XUserObject )
				{
					XUserObject	addedUserObject = (XUserObject) addedMemberObject;

					// add the User to the added Users

					addedUserObjects.put( addedUserObject, currentMemberMap.get( addedUserObject ) );

					// fire event .. a Group is now accessible

					XEventsManager.fire( XGroupModuleServerEvents.class ).notifyGroupAccessible(
						groupObject,
						addedUserObject );
				}
			}

			// process and notify Users removed from the Group

			for ( XMemberObject removedMemberObject : removedMemberObjects )
			{
				// remove the Group from the User

				removedMemberObject.getMemberGroupsObject().removeGroupObject( groupObject );

				if ( removedMemberObject instanceof XUserObject )
				{
					XUserObject	removedUserObject = (XUserObject) removedMemberObject;

					// add the User to the removed Users

					removedUserObjects.add( removedUserObject );

					// fire event .. a Group is now inaccessible

					XEventsManager.fire( XGroupModuleServerEvents.class ).notifyGroupInaccessible(
						groupObject,
						removedUserObject );
				}
			}

			// now that number-accessible-users is up to date, fire any pending GroupMembersChangedNotification

			PendingGroupMembersChangedNotificationData	notificationData = pendingGroupMembersChangedNotifications.remove( groupObject );

			if ( notificationData != null )
			{
				XEventsManager.fire( XGroupModuleServerEvents.class ).notifyGroupMembersChanged(
					notificationData.fGroupObject,
					notificationData.fAddedMemberObjects,
					notificationData.fRemovedMemberObjects );
			}

			// did we add or remove Users ..

			if ( !addedUserObjects.isEmpty() || !removedUserObjects.isEmpty() )
			{
				// send notifications

				( new XGroupMembershipChangedNotification(
					groupObject,
					addedUserObjects,
					removedUserObjects ) ).send();

				// fire event

				XEventsManager.fire( XGroupModuleServerEvents.class ).notifyGroupMembershipChanged(
					groupObject,
					addedUserObjects,
					removedUserObjects );

				// if there are no more accessible Users then log that this conversation is now orphaned

				if ( finalMemberObjects.isEmpty() )
				{
					// fire event .. a conversation is now orphaned

					XEventsManager.fire( XGroupModuleServerEvents.class ).notifyGroupOrphaned(
						groupObject );
				}
			}
		}

		sLogger.warning( "*** 4. Processed changes in exploded members. Total time taken till now : " + ( System.currentTimeMillis() - startTime ) );

		//////////////////////////////////////////////////////////////
		// process changes to all affected Conversations
		//////////////////////////////////////////////////////////////

		final Map<XConversationObject,Map<XMemberObject,XConversationRole>>	initialConversationRolesToIndexBG = new HashMap<XConversationObject,Map<XMemberObject,XConversationRole>>();
		final Map<XConversationObject,Map<XMemberObject,XConversationRole>>	initialConversationRolesToIndexInline = new HashMap<XConversationObject,Map<XMemberObject,XConversationRole>>();

		resolveConversationIndexing( initialConversationRoles, initialConversationRolesToIndexBG, initialConversationRolesToIndexInline );

		if ( sLogger.isInfoEnabled() )
		{
			sLogger.info( "*** Total conv to be indexed as part of group membership change, inline : {0}, background: {1} ",
						  initialConversationRolesToIndexInline.size(),
						  initialConversationRolesToIndexBG.size() );
		}

		// Index the priority conversation first, inline
		indexConversations( initialConversationRoles, initialConversationRolesToIndexInline );

		// Index rest of the conversations in background.

		if ( initialConversationRolesToIndexBG.size() > 0 )
		{
			indexConversationsInBackground( initialConversationRoles, initialConversationRolesToIndexBG );
		}

		sLogger.warning( "*** 5. Processed all inline conversation changes. Total time taken till now : " + ( System.currentTimeMillis() - startTime ) );

		// sync to IDC

		if ( XGroupIDCSyncUtils.isSyncEnabled() )
		{
			final Set<XGroupObject> grObjs = XCollections.copyOfSet( groupObjects );
			final XUserObject currentUser = XSessionManager.getEffectiveUserObject();
			final Map<XGroupObject,Map<XMemberObject,XConversationRole>> addedMmbrs = XCollections.copyOfMap( addedMembersToSync );
			final Map<XGroupObject,Map<XMemberObject,XConversationRole>> modedMmbrs = XCollections.copyOfMap( modifiedMembersToSync );
			final Map<XGroupObject,Set<XMemberObject>> rmvdMmbrs = XCollections.copyOfMap( removedMembersToSync );
			final XGroupSyncBacklogObject createBacklogObject;
			final Set<XGroupSyncBacklogObject> addedBacklogObjects = new HashSet<>( addedMembersToSync.size() );
			final Set<XGroupSyncBacklogObject> modifiedBacklogObjects = new HashSet<>( modifiedMembersToSync.size() );
			final Set<XGroupSyncBacklogObject> removedBacklogObjects = new HashSet<>( removedMembersToSync.size() );
			XGroupObject createdGroup = null;

			logDocsSync( addedMmbrs, modedMmbrs, removedMembersToSync );

			if ( syncGroupCreate )
			{
				createdGroup = grObjs.iterator().next();

				createBacklogObject =
					XGroupSyncBacklogObjectManager.createGroupSyncBacklogObject( currentUser,
																				 createdGroup,
																				 createdGroup.getGroupID(),
																				 XGroupSyncOperationType.CREATE,
																				 createdGroup.getGroupMembersObject().getMemberObjects(),
																				 createdGroup.getGroupType(),
																				 createdGroup.getName(),
																				 XGroupIDCSyncUtils.GroupSyncLocalStatus.INIT.getCode(),
																				 XGroupIDCSyncUtils.GroupSyncLocalStatus.INIT.getMessage(),
																				 true );

				XEventsManager.fire( XGroupModuleServerEvents.class ).notifyGroupBacklogObjectCreated( createBacklogObject );
			}
			else
			{
				createBacklogObject = null;
			}

			if ( syncGroupMembers )
			{
				for ( Map.Entry<XGroupObject,Map<XMemberObject,XConversationRole>> addedGroupMembersToSync : addedMembersToSync.entrySet() )
				{
					XGroupObject groupObject = addedGroupMembersToSync.getKey();

					if ( ( ( createdGroup == null ) || ( !groupObject.getID().equals( createdGroup.getID() ) ) ) &&
						 XCollections.isMapNotEmpty( addedGroupMembersToSync.getValue() ) )
					{

						XGroupSyncBacklogObject	backlogObject = XGroupSyncBacklogObjectManager.createGroupSyncBacklogObject( currentUser,
																																groupObject,
																																groupObject.getGroupID(),
																																XGroupSyncOperationType.MEMBER_ADD,
																																addedGroupMembersToSync.getValue(),
																																groupObject.getGroupType(),
																																groupObject.getName(),
																																XGroupIDCSyncUtils.GroupSyncLocalStatus.INIT.getCode(),
																																XGroupIDCSyncUtils.GroupSyncLocalStatus.INIT.getMessage(),
																																true );

						XEventsManager.fire( XGroupModuleServerEvents.class ).notifyGroupBacklogObjectCreated( backlogObject );

						addedBacklogObjects.add( backlogObject );
					}
				}
			}

			for ( Map.Entry<XGroupObject,Map<XMemberObject,XConversationRole>> modifiedGroupMembersToSync : modifiedMembersToSync.entrySet() )
			{
				XGroupObject groupObject = modifiedGroupMembersToSync.getKey();

				if ( XCollections.isMapNotEmpty( modifiedGroupMembersToSync.getValue() ) )
				{
					XGroupSyncBacklogObject	backlogObject = XGroupSyncBacklogObjectManager.createGroupSyncBacklogObject( currentUser,
																															groupObject,
																															groupObject.getGroupID(),
																															XGroupSyncOperationType.MEMBER_MODIFY,
																															modifiedGroupMembersToSync.getValue(),
																															groupObject.getGroupType(),
																															groupObject.getName(),
																															XGroupIDCSyncUtils.GroupSyncLocalStatus.INIT.getCode(),
																															XGroupIDCSyncUtils.GroupSyncLocalStatus.INIT.getMessage(),
																															true );

					XEventsManager.fire( XGroupModuleServerEvents.class ).notifyGroupBacklogObjectCreated( backlogObject );

					modifiedBacklogObjects.add( backlogObject );
				}
			}

			for ( Map.Entry<XGroupObject,Set<XMemberObject>> removedGroupMembersToSync : removedMembersToSync.entrySet() )
			{
				XGroupObject groupObject = removedGroupMembersToSync.getKey();

				Set<XMemberObject> removedMemberSet = removedGroupMembersToSync.getValue();

				if ( XCollections.isNotEmpty( removedMemberSet ) )
				{
					Map<XMemberObject,XConversationRole> removedMap = new HashMap<>( removedMemberSet.size() );

					for ( XMemberObject removedMember : removedMemberSet )
					{
						removedMap.put( removedMember, XConversationRole.NONE );
					}

					XGroupSyncBacklogObject	backlogObject = XGroupSyncBacklogObjectManager.createGroupSyncBacklogObject( currentUser,
																															groupObject,
																															groupObject.getGroupID(),
																															XGroupSyncOperationType.MEMBER_REMOVE,
																															removedMap,
																															groupObject.getGroupType(),
																															groupObject.getName(),
																															XGroupIDCSyncUtils.GroupSyncLocalStatus.INIT.getCode(),
																															XGroupIDCSyncUtils.GroupSyncLocalStatus.INIT.getMessage(),
																															true );

					XEventsManager.fire( XGroupModuleServerEvents.class ).notifyGroupBacklogObjectCreated( backlogObject );

					removedBacklogObjects.add( backlogObject );
				}
			}

			XExecutorManager.executeOnCommit( new XTransactionRunnable( "Group Change Members DoCS Sync" )
			{
				@Override
				public void execute()
				{
					if ( sLogger.isInfoEnabled() )
					{
						sLogger.info( "Starting main thread for groups docs sync. Following groups will be included:" );

						for ( XGroupObject grp : grObjs )
						{
							sLogger.info( grp.getID() + ", " );
						}
					}

					XGroupObject createdGroupObject = null;

					if ( syncGroupCreate )
					{
						XGroupIDCSyncUtils.groupCreated( createBacklogObject );
						createdGroupObject = createBacklogObject.getGroupObject();
					}

					for ( XGroupSyncBacklogObject backlogObject : addedBacklogObjects )
					{
						if ( ( createdGroupObject == null ) || ( !backlogObject.getGroupObject().getID().equals( createdGroupObject.getID() ) ) )
						{
							XGroupIDCSyncUtils.groupMembershipAdded( backlogObject );
						}
					}

					for ( XGroupSyncBacklogObject backlogObject : modifiedBacklogObjects )
					{
						XGroupIDCSyncUtils.groupMembershipModified( backlogObject );
					}

					for ( XGroupSyncBacklogObject backlogObject : removedBacklogObjects )
					{
						XGroupIDCSyncUtils.groupMembershipRemoved( backlogObject );
					}

					if ( sLogger.isInfoEnabled() )
					{
						sLogger.info( "Completed groups docs sync main thread." );
					}
				}
			} );

			sLogger.warning( "*** 6. Synced groupchanges to idc. Total time taken till now : " + ( System.currentTimeMillis() - startTime ) );
		}
	}

	private static void logDocsSync(
		Map<XGroupObject,Map<XMemberObject,XConversationRole>> addedMembers,
		Map<XGroupObject,Map<XMemberObject,XConversationRole>> modifiedMembers,
		Map<XGroupObject,Set<XMemberObject>> removedMembers )
	{
		String  additions = null;
		String  modifications = null;
		String  removals = null;

		StringBuilder	builder = new StringBuilder();

		if ( addedMembers != null )
		{
			for ( Map.Entry<XGroupObject,Map<XMemberObject,XConversationRole>> entry : addedMembers.entrySet() )
			{
				builder.append( "Group:" ).append( entry.getKey().getID() ).append( " Adds:" ).append( entry.getValue().size() ).append( "," );
			}

			additions = builder.toString();

			builder.setLength( 0 );
		}

		if ( modifiedMembers != null )
		{
			for ( Map.Entry<XGroupObject,Map<XMemberObject,XConversationRole>> entry : modifiedMembers.entrySet() )
			{
				builder.append( "Group:" ).append( entry.getKey().getID() ).append( " Mods:" ).append( entry.getValue().size() ).append( "," );
			}

			modifications = builder.toString();

			builder.setLength( 0 );
		}

		if ( removedMembers != null )
		{
			for ( Map.Entry<XGroupObject,Set<XMemberObject>> entry : removedMembers.entrySet() )
			{
				builder.append( "Group:" ).append( entry.getKey().getID() ).append( " Removals:" ).append( entry.getValue().size() ).append( "," );
			}

			removals = builder.toString();
		}

		sLogger.warning( "*** 5.1 Initiating docs syc. Additions: {0} Modifications: {1} Removals: {2}", additions, modifications, removals );
	}

	private static void resolveConversationIndexing(
		Map<XConversationObject,Map<XMemberObject,XConversationRole>> initialConversationRoles,
		Map<XConversationObject,Map<XMemberObject,XConversationRole>> initialConversationRolesToIndexBG,
		Map<XConversationObject,Map<XMemberObject,XConversationRole>> initialConversationRolesToIndexInline )
	{
		// 1. Segregate conversations based on hm.

		Map<XHierarchicalMembersObject,Set<XConversationObject>>	hmToConvMap = new HashMap<>();
		Set<XConversationObject>									nonHierarchicalConversations = new HashSet<>();

		for ( XConversationObject conversation : initialConversationRoles.keySet() )
		{
			XHierarchicalMembersObject hmObject = conversation.getConversationMembersObject().getHierarchicalMembersObject();

			if ( hmObject != null )
			{
				if ( !hmToConvMap.containsKey( hmObject ) )
				{
					hmToConvMap.put( hmObject, new HashSet<XConversationObject>() );
				}

				hmToConvMap.get( hmObject ).add( conversation );
			}
			else
			{
				nonHierarchicalConversations.add( conversation );
			}
		}

		// 2. Select the hm object that has any direct memberships

		Set<XHierarchicalMembersObject>		hmWithDirectMembers = new HashSet<>();

		for ( XHierarchicalMembersObject hmObject : hmToConvMap.keySet() )
		{
			if ( hmObject.hasDirectMembers() )
			{
				hmWithDirectMembers.add( hmObject );
			}
		}

		// 3. Get the conversations that need to be indexed first.

		Set<XConversationObject> conversationToBeIndexedFirst = new HashSet<>( nonHierarchicalConversations );

		for ( XHierarchicalMembersObject hmObject : hmWithDirectMembers )
		{
			// Add first conversation of each hm object.

			conversationToBeIndexedFirst.add( hmObject.getConversationMembersObjects().iterator().next().getConversationObject() );
		}

		// 4. Populate the conversations that needs to be indexed inline and in background.

		initialConversationRolesToIndexBG.putAll( initialConversationRoles );

		for ( XConversationObject conversation : conversationToBeIndexedFirst )
		{
			initialConversationRolesToIndexInline.put( conversation, initialConversationRolesToIndexBG.remove( conversation ) );
		}
	}

	private static void indexConversationsInBackground(
		final Map<XConversationObject,Map<XMemberObject,XConversationRole>> initialConversationRoles,
		final Map<XConversationObject,Map<XMemberObject,XConversationRole>> initialConversationRolesToIndexBG )
	{
		final XUserObject		changingUserObject = XSessionManager.getUserObject();

		XExecutorManager.executeOnCommit( new XExecutorRunnable( "Group conversation membership sync." )
		{
			@Override
			public void execute()
			{
				long time = System.currentTimeMillis();

				if ( sLogger.isDebugEnabled() )
				{
					sLogger.error( "*** 4.b Indexing conversation changes in a separate thread. " );
				}

				// Update conversation membership indexing thread count. Do this in separate txn so that count is visible even when the following process
				// is still in progress.

				XExecutorManager.now( new XTransactionRunnable( "Adjust conversation indexing thread count." )
				{
					@Override
					public void execute()
					{
						adjustConversationIndexThreadCount( initialConversationRolesToIndexBG.keySet(), true );
					}
				} );

				// Update conversation membership.

				XExecutorManager.now( new XTransactionRunnable( "Adjust conversations post group membership changes." )
				{
					@Override
					public void execute()
					{
						XSessionManager.setSession( changingUserObject );

						XThreadLocalManager.setIsMembershipIndexingThread( true );

						indexConversations( initialConversationRoles, initialConversationRolesToIndexBG );

						adjustConversationIndexThreadCount( initialConversationRolesToIndexBG.keySet(), false );
					}
				} );

				if ( sLogger.isDebugEnabled() )
				{
					sLogger.debug( "*** 4.b Indexed all conversations, time : " + ( System.currentTimeMillis() - time ) );
				}
			}
		} );
	}

	private static void adjustConversationIndexThreadCount( final Set<XConversationObject> conversations, final boolean increment )
	{
		for ( XConversationObject conversationObject : conversations )
		{
			if ( increment )
			{
				conversationObject.incrementIndexingThreadCount();
			}
			else
			{
				conversationObject.decrementIndexingThreadCount();
			}
		}
	}

	private static void indexConversations(
		final Map<XConversationObject, Map<XMemberObject, XConversationRole>> allConversationRoles,
		final Map<XConversationObject, Map<XMemberObject, XConversationRole>> conversationRolesToIndex )
	{
		boolean consolidateFollowupClosedEmail = XPersistentPropertiesManager.getInstance().getBoolean( "consolidate.followup.closed.email", true );

		// process final changes in scoping Conversation memberships and assignees due to Group changes

		for ( XConversationObject conversationObject : conversationRolesToIndex.keySet() )
		{
			if ( conversationObject.isScoping() )
			{
				XConversationMemberUtils.changeMembersUtil(
					conversationObject,
					null,                                        // no changes
					true,                                        // real changes
					true,
					false,                                        // not an error if removing self
					false,                                        // do not perform standard checks
					isSendMailOnGroupAddedConversations(),        // do not send accessibility mail
					consolidateFollowupClosedEmail,
					allConversationRoles );
			}
		}

		// process final changes in non-scoping Conversation memberships and assignees due to Group changes

		for ( XConversationObject conversationObject : conversationRolesToIndex.keySet() )
		{
			if ( !conversationObject.isScoping() )
			{
				XConversationMemberUtils.changeMembersUtil(
					conversationObject,
					null,                                        // no changes
					true,                                        // real changes
					true,
					false,                                        // not an error if removing self
					false,                                        // do not perform standard checks
					isSendMailOnGroupAddedConversations(),        // do not send accessibility mail
					consolidateFollowupClosedEmail,
					allConversationRoles );
			}
		}
	}

	/**
	 * Removes a deprovisioned User from Groups that they are a member of. Sets
	 * the specified alternative owner where the deprovisioned User is a group owner.
	 *
	 * @param userName Deprovisioned User name.
	 * @param alternativeOwner Alternative owner User name.
	 */
	public static void userDeprovisioned( String userName, String alternativeOwner )
	{
		XUserObject			dpUserObject = XUsersObjectManager.getUserObject( userName );

		XUserObject			altUserObject = null;

		if ( XString.isNotBlank( alternativeOwner ) )
		{
			altUserObject = XUsersObjectManager.findUserObject( alternativeOwner );

			if ( ( altUserObject != null ) )
			{
				if ( ( altUserObject.equals( dpUserObject ) ) )
				{
					throw new XRuntimeException( "waggle.server.group.deprov.alt.owner.matches.dpuser", alternativeOwner, userName );
				}

				if ( !altUserObject.isEnabled() )
				{
					throw new XRuntimeException( "waggle.server.group.deprov.alt.owner.disabled", alternativeOwner, userName );
				}
			}
		}

		Set<XGroupObject>	groupObjects = dpUserObject.getMemberGroupsObject().getGroupObjects();

		XUserObject			currentUserObject = XSessionManager.getUserObject();

		for ( XGroupObject groupObject : groupObjects )
		{
			XUserObject 	groupOwner = groupObject.getOwnerUserObject();

			if ( groupOwner.equals( dpUserObject ) )
			{
				if ( altUserObject == null )
				{
					throw new XRuntimeException( "waggle.server.group.deprov.alt.owner.notfound", alternativeOwner, userName );
				}
				groupObject.setOwnerUserObject( altUserObject );
				groupObject.getGroupMembersObject().addMemberObject( altUserObject, XConversationRole.GROUP_MANAGER, currentUserObject, new Date() );
			}

			groupObject.getGroupMembersObject().removeMemberObject( dpUserObject, currentUserObject );

			if ( altUserObject != null )
			{
				XGroupSyncBacklogObject	altBacklogObject = XGroupSyncBacklogObjectManager.createGroupSyncBacklogObject( XSessionManager.getEffectiveUserObject(),
																														   groupObject,
																														   groupObject.getGroupID(),
																														   XGroupSyncOperationType.MEMBER_ADD,
																														   Collections.singletonMap( (XMemberObject) altUserObject, XConversationRole.GROUP_MANAGER ),
																														   groupObject.getGroupType(),
																														   groupObject.getName(),
																														   XGroupIDCSyncUtils.GroupSyncLocalStatus.INIT.getCode(),
																														   XGroupIDCSyncUtils.GroupSyncLocalStatus.INIT.getMessage(),
																														   true );
				XEventsManager.fire( XGroupModuleServerEvents.class ).notifyGroupBacklogObjectCreated( altBacklogObject );

				XGroupIDCSyncUtils.groupMembershipAdded( altBacklogObject );
			}

			XGroupSyncBacklogObject	backlogObject = XGroupSyncBacklogObjectManager.createGroupSyncBacklogObject( XSessionManager.getEffectiveUserObject(),
																													groupObject,
																													groupObject.getGroupID(),
																													XGroupSyncOperationType.MEMBER_REMOVE,
																													Collections.singletonMap( (XMemberObject) dpUserObject, XConversationRole.NONE ),
																													groupObject.getGroupType(),
																													groupObject.getName(),
																													XGroupIDCSyncUtils.GroupSyncLocalStatus.INIT.getCode(),
																													XGroupIDCSyncUtils.GroupSyncLocalStatus.INIT.getMessage(),
																													true );

			XEventsManager.fire( XGroupModuleServerEvents.class ).notifyGroupBacklogObjectCreated( backlogObject );

			XGroupIDCSyncUtils.groupMembershipRemoved( backlogObject );
		}
	}

	private static boolean isSendMailOnGroupAddedConversations()
	{
		return XPersistentPropertiesManager.getInstance().getBoolean( "waggle.server.group.conversation.added.mail.for.group.adds", false );
	}

	// throws if current user cannot view group object
	public static void checkGetGroup( XGroupObject groupObject )
	{
		if ( ( groupObject.getRealmObject().isInternal() ) &&
			groupObject.getGroupType().equals( XGroupType.PRIVATE_CLOSED ) )
		{
			XValidate.checkRoleGroupMember( groupObject );
		}
	}

	// returns false if current user cannot view group object
	public static boolean isGetGroupAllowed( XGroupObject groupObject )
	{
		return ( ( groupObject.getRealmObject().isExternal() ) ||
				 ( groupObject.getGroupType().equals( XGroupType.PUBLIC_OPEN ) ) ||
				 ( groupObject.getGroupType().equals( XGroupType.PUBLIC_CLOSED ) ) ||
				 ( XValidate.isGroupRole( groupObject, XConversationRole.GROUP_MEMBER ) ) );
	}

	public static void resetConvIndexThreadCount( XObjectID groupID )
	{
		XGroupObject	groupObject = XGroupObjectManager.findGroupObject( groupID );

		if ( groupObject != null )
		{
			if ( sLogger.isDebugEnabled() )
			{
				sLogger.debug( "Resetting index thread count for conversations of group {0}.", groupID );
			}

			Set<XConversationObject> conversations = new HashSet<>( groupObject.getMemberConversationsObject().getConversationObjects() );

			for ( XGroupObject group : groupObject.getMemberGroupsObject().getGroupObjects() )
			{
				conversations.addAll( group.getMemberConversationsObject().getConversationObjects() );
			}

			StringBuilder	conversationList = new StringBuilder();

			for ( XConversationObject conversation : conversations )
			{
				conversation.resetIndexingThreadCount();

				conversationList.append( conversation.getID() ).append( " " );
			}

			if ( sLogger.isDebugEnabled() )
			{
				sLogger.debug( "Reset index thread count of below conversations.\n {0}", conversationList.toString() );
			}
		}
	}

	/**
	 * Utility method to sync missing members between Social and Docs Group member table.
	 *
	 * @param groupObject Group object
	 */
	public static void forceSyncGroupMembers ( XGroupObject groupObject )
	{
		if ( groupObject == null )
		{
			if( sLogger.isDebugEnabled() )
			{
				sLogger.debug( "Group does not exist. Hence, no sync process initiated." );
			}
			return;
		}

		// get all group members from social
		Map<XMemberObject, XConversationRole>		socialGroupMembers = groupObject.getGroupMembersObject().getMemberObjects();
		Map<Long, GroupMembersWithConvRole>			socialMemberWithRoles = new HashMap<>();
		Set<String>									membersToRemoveFromDocs = new HashSet<>();

		if ( sLogger.isDebugEnabled() )
		{
			sLogger.debug( "Total social members found for group: {0} :: {1}", groupObject.getName(), XCollections.isMapNotEmpty( socialGroupMembers ) ? socialGroupMembers.size() : 0 );
		}

		if ( XCollections.isMapNotEmpty( socialGroupMembers ) )
		{
			for( Map.Entry<XMemberObject, XConversationRole> memberEntrySet: socialGroupMembers.entrySet() )
			{
				XMemberObject					memberObject = memberEntrySet.getKey();

				GroupMembersWithConvRole		membersWithConvRole = new GroupMembersWithConvRole( memberObject, memberEntrySet.getValue() );

				socialMemberWithRoles.put( memberObject.getID().toLong(), membersWithConvRole );
			}
		}

		// get all group members from docs
		List<XMemberInfo>			docsGroupMembers = XIdcUtils.viewGroupMembers( groupObject.getOwnerUserObject(), groupObject );

		if ( sLogger.isDebugEnabled() )
		{
			sLogger.debug( "Total docs members found for group: {0} :: {1}", groupObject.getName(), XCollections.isNotEmpty( docsGroupMembers ) ? docsGroupMembers.size() : 0 );
		}

		// Add members into Docs which are available in Social but missing in Docs
		if ( XCollections.isNotEmpty( docsGroupMembers ) )
		{
			for ( XMemberInfo docsmemberInfo : docsGroupMembers )
			{
				// docsmemberInfo.ID can be null if user is available in docs but not in social. Remove such users from docs too. see bug 31378318

				if ( ( docsmemberInfo.ID != null ) && ( socialMemberWithRoles.containsKey( docsmemberInfo.ID.toLong() ) ) )
				{
					// remove member from both set as both social and docs contains member.
					// whatever left after this operation will give members to add in docs from social - socialMemberWithRoles and members to remove from docs - removeMembers
					socialMemberWithRoles.remove( docsmemberInfo.ID.toLong() );
				}
				else
				{
					if( docsmemberInfo.ObjectType == XUserObject.TYPE)
					{
						membersToRemoveFromDocs.add( docsmemberInfo.Name );
					}
					else if ( docsmemberInfo.ObjectType == XGroupObject.TYPE )
					{
						membersToRemoveFromDocs.add( "GS" + docsmemberInfo.ID.toString() );
					}
				}
			}
		}

		if( XCollections.isMapNotEmpty( socialMemberWithRoles ) )
		{
			Map<XMemberObject, XConversationRole>		addedMembersToDocs = new HashMap<>();
			for( Map.Entry<Long, GroupMembersWithConvRole> memberObjectXConversationRoleEntry : socialMemberWithRoles.entrySet() )
			{
				GroupMembersWithConvRole	groupMembersWithConvRole = memberObjectXConversationRoleEntry.getValue();

				addedMembersToDocs.put( groupMembersWithConvRole.getMemberObject(), groupMembersWithConvRole.getConversationRole() );
			}

			if( sLogger.isDebugEnabled() )
			{
				sLogger.debug( "Total members to add to Docs group: {0}", groupObject.getName(), XCollections.isMapNotEmpty( addedMembersToDocs ) ? addedMembersToDocs.size() : 0 );
			}

			if( XCollections.isMapNotEmpty( addedMembersToDocs ) )
			{
				XIdcUtils.addGroupMembers( groupObject.getOwnerUserObject(), groupObject, addedMembersToDocs );
			}
		}

		if( sLogger.isDebugEnabled() )
		{
			sLogger.debug( "Total members to remove from Docs group: {0} -- {1}", groupObject.getName(), XCollections.isNotEmpty( membersToRemoveFromDocs ) ? membersToRemoveFromDocs.size() : 0 );
		}

		if ( XCollections.isNotEmpty( membersToRemoveFromDocs ) )
		{
			XIdcUtils.removeGroupMembersWithName( groupObject.getOwnerUserObject(), groupObject, membersToRemoveFromDocs );
		}
	}

	private static class GroupMembersWithConvRole
	{
		private XMemberObject memberObject;
		private XConversationRole conversationRole;

		public GroupMembersWithConvRole(XMemberObject memberObj, XConversationRole convRole)
		{
			memberObject = memberObj;
			conversationRole = convRole;
		}

		public XMemberObject getMemberObject()
		{
			return memberObject;
		}

		public void setMemberObject( XMemberObject memberObject )
		{
			this.memberObject = memberObject;
		}

		public XConversationRole getConversationRole()
		{
			return conversationRole;
		}

		public void setConversationRole( XConversationRole conversationRole )
		{
			this.conversationRole = conversationRole;
		}
	}
}
