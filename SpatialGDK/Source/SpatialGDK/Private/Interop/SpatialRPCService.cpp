// Copyright (c) Improbable Worlds Ltd, All Rights Reserved

#include "Interop/SpatialRPCService.h"

#include "Interop/SpatialStaticComponentView.h"
#include "Schema/ClientEndpoint.h"
#include "Schema/CrossServerEndpoint.h"
#include "Schema/MulticastRPCs.h"
#include "Schema/ServerEndpoint.h"
#include "Utils/SpatialLatencyTracer.h"

DEFINE_LOG_CATEGORY(LogSpatialRPCService);

#pragma optimize ("", off)

namespace SpatialGDK
{
	SpatialRPCService::SpatialRPCService(ExtractRPCDelegate ExtractRPCCallback, const USpatialStaticComponentView* View,
		USpatialLatencyTracer* SpatialLatencyTracer)
		: ExtractRPCCallback(ExtractRPCCallback)
		, View(View)
		, SpatialLatencyTracer(SpatialLatencyTracer)
	{
		CrossServerMailbox.SetNumZeroed(RPCRingBufferUtils::GetRingBufferSize(ERPCType::CrossServerSender));
	}

	EPushRPCResult SpatialRPCService::PushRPC(Worker_EntityId EntityId, const FUnrealObjectRef& Counterpart, ERPCType Type, RPCPayload Payload, bool bCreatedEntity)
	{
		EntityRPCType EntityType = EntityRPCType(EntityId, Type);

		EPushRPCResult Result = EPushRPCResult::Success;

		if (RPCRingBufferUtils::ShouldQueueOverflowed(Type) && OverflowedRPCs.Contains(EntityType))
		{
			// Already has queued RPCs of this type, queue until those are pushed.
			AddOverflowedRPC(EntityType, MoveTemp(Payload));
			Result = EPushRPCResult::QueueOverflowed;
		}
		else
		{
			Result = PushRPCInternal(EntityId, Counterpart, Type, MoveTemp(Payload), bCreatedEntity);

			if (Result == EPushRPCResult::QueueOverflowed)
			{
				AddOverflowedRPC(EntityType, MoveTemp(Payload));
			}
		}

#if TRACE_LIB_ACTIVE
		ProcessResultToLatencyTrace(Result, Payload.Trace);
#endif

		return Result;
	}

	EPushRPCResult SpatialRPCService::PushRPCInternal(Worker_EntityId EntityId, const FUnrealObjectRef& Counterpart, ERPCType Type, RPCPayload&& Payload, bool bCreatedEntity)
	{
		const Worker_ComponentId RingBufferComponentId = RPCRingBufferUtils::GetRingBufferComponentId(Type);

		const EntityComponentId EntityComponent = { EntityId, RingBufferComponentId };
		const EntityRPCType EntityType = EntityRPCType(EntityId, Type);

		Schema_Object* EndpointObject;
		uint64 LastAckedRPCId;
		if (View->HasComponent(EntityId, RingBufferComponentId))
		{
			if (!View->HasAuthority(EntityId, RingBufferComponentId))
			{
				if (bCreatedEntity)
				{
					return EPushRPCResult::EntityBeingCreated;
				}
				return EPushRPCResult::NoRingBufferAuthority;
			}

			EndpointObject = Schema_GetComponentUpdateFields(GetOrCreateComponentUpdate(EntityComponent));

			if (Type == ERPCType::NetMulticast)
			{
				// Assume all multicast RPCs are auto-acked.
				LastAckedRPCId = LastSentRPCIds.FindRef(EntityType);
			}
			else
			{
				// Ignore these while the routing worker can be anyone.
				if (Type != ERPCType::CrossServerSender && Type != ERPCType::CrossServerReceiver)
				{
					// We shouldn't have authority over the component that has the acks.
					if (View->HasAuthority(EntityId, RPCRingBufferUtils::GetAckComponentId(Type)))
					{
						return EPushRPCResult::HasAckAuthority;
					}
					LastAckedRPCId = GetAckFromView(EntityId, Type);
				}
			}
		}
		else
		{
			if (bCreatedEntity)
			{
				return EPushRPCResult::EntityBeingCreated;
			}
			// If the entity isn't in the view, we assume this RPC was called before
			// CreateEntityRequest, so we put it into a component data object.
			EndpointObject = Schema_GetComponentDataFields(GetOrCreateComponentData(EntityComponent));

			LastAckedRPCId = 0;
		}

		uint64 NewRPCId = LastSentRPCIds.FindRef(EntityType) + 1;
		EPushRPCResult Result = EPushRPCResult::Success;

		if (Type == ERPCType::CrossServerSender)
		{
			TOptional<uint32> Slot = FindFreeSlotForCrossServerSender();
			if (Slot)
			{
				int32 SlotIdx = Slot.GetValue();

				RPCRingBufferDescriptor Descriptor = RPCRingBufferUtils::GetRingBufferDescriptor(Type);
				uint32 Field = Descriptor.GetRingBufferElementFieldId(ERPCType::CrossServerSender, SlotIdx + 1);

				Schema_Object* RPCObject = Schema_AddObject(EndpointObject, Field);
				Payload.WriteToSchemaObject(RPCObject);

				FUnrealObjectRef Target = Counterpart;
				Target.Offset = NewRPCId;
				AddObjectRefToSchema(EndpointObject, Field + 1, Target);

				Schema_ClearField(EndpointObject, Descriptor.LastSentRPCFieldId);
				Schema_AddUint64(EndpointObject, Descriptor.LastSentRPCFieldId, NewRPCId);

				CrossServerEndpointSender* Sender = View->GetComponentData<CrossServerEndpointSender>(EntityId);
				if (ensure(Sender != nullptr))
				{
					RPCRingBuffer& Buffer = Sender->ReliableRPCBuffer;
					Buffer.RingBuffer[SlotIdx].Emplace(Payload);
					Buffer.Counterpart[SlotIdx].Emplace(Target);

					SentRPCEntry& Entry = CrossServerMailbox[SlotIdx];
					Entry.RPCId = NewRPCId;
					Entry.Target = Target.Entity;
					Entry.Timestamp = FPlatformTime::Cycles64();
				}
				else
				{
					Result = EPushRPCResult::EntityBeingCreated;
				}
			}
			else
			{
				// Overflowed
				if (RPCRingBufferUtils::ShouldQueueOverflowed(Type))
				{
					Result = EPushRPCResult::QueueOverflowed;
				}
				else
				{
					Result = EPushRPCResult::DropOverflowed;
				}
			}
		}
		else
		{
			// Check capacity.
			if (LastAckedRPCId + RPCRingBufferUtils::GetRingBufferSize(Type) >= NewRPCId)
			{
				RPCRingBufferUtils::WriteRPCToSchema(EndpointObject, Type, NewRPCId, Payload);
			}
			else
			{
				// Overflowed
				if (RPCRingBufferUtils::ShouldQueueOverflowed(Type))
				{
					return EPushRPCResult::QueueOverflowed;
				}
				else
				{
					return EPushRPCResult::DropOverflowed;
				}
			}
		}

		if (Result == EPushRPCResult::Success)
		{
			LastSentRPCIds.Add(EntityType, NewRPCId);

#if TRACE_LIB_ACTIVE
			if (SpatialLatencyTracer != nullptr && Payload.Trace != InvalidTraceKey)
			{
				if (PendingTraces.Find(EntityComponent) == nullptr)
				{
					PendingTraces.Add(EntityComponent, Payload.Trace);
				}
				else
				{
					SpatialLatencyTracer->WriteAndEndTrace(Payload.Trace,
						TEXT("Multiple rpc updates in single update, ending further stack tracing"), true);
				}
			}
#endif
		}

		return Result;
	}

	void SpatialRPCService::PushOverflowedRPCs()
	{
		for (auto It = OverflowedRPCs.CreateIterator(); It; ++It)
		{
			Worker_EntityId EntityId = It.Key().EntityId;
			ERPCType Type = It.Key().Type;
			TArray<RPCPayload>& OverflowedRPCArray = It.Value();

			int NumProcessed = 0;
			bool bShouldDrop = false;
			for (RPCPayload& Payload : OverflowedRPCArray)
			{
				const EPushRPCResult Result = PushRPCInternal(EntityId, FUnrealObjectRef(), Type, MoveTemp(Payload), false);

				switch (Result)
				{
				case EPushRPCResult::Success:
					NumProcessed++;
					break;
				case EPushRPCResult::QueueOverflowed:
					UE_LOG(LogSpatialRPCService, Log,
						TEXT("SpatialRPCService::PushOverflowedRPCs: Sent some but not all overflowed RPCs. RPCs sent %d, RPCs still "
							"overflowed: %d, Entity: %lld, RPC type: %s"),
						NumProcessed, OverflowedRPCArray.Num() - NumProcessed, EntityId, *SpatialConstants::RPCTypeToString(Type));
					break;
				case EPushRPCResult::DropOverflowed:
					checkf(false, TEXT("Shouldn't be able to drop on overflow for RPC type that was previously queued."));
					break;
				case EPushRPCResult::HasAckAuthority:
					UE_LOG(LogSpatialRPCService, Warning,
						TEXT("SpatialRPCService::PushOverflowedRPCs: Gained authority over ack component for RPC type that was overflowed. "
							"Entity: %lld, RPC type: %s"),
						EntityId, *SpatialConstants::RPCTypeToString(Type));
					bShouldDrop = true;
					break;
				case EPushRPCResult::NoRingBufferAuthority:
					UE_LOG(LogSpatialRPCService, Warning,
						TEXT("SpatialRPCService::PushOverflowedRPCs: Lost authority over ring buffer component for RPC type that was "
							"overflowed. Entity: %lld, RPC type: %s"),
						EntityId, *SpatialConstants::RPCTypeToString(Type));
					bShouldDrop = true;
					break;
				default:
					checkNoEntry();
				}

#if TRACE_LIB_ACTIVE
				ProcessResultToLatencyTrace(Result, Payload.Trace);
#endif

				// This includes the valid case of RPCs still overflowing (EPushRPCResult::QueueOverflowed), as well as the error cases.
				if (Result != EPushRPCResult::Success)
				{
					break;
				}
			}

			if (NumProcessed == OverflowedRPCArray.Num() || bShouldDrop)
			{
				It.RemoveCurrent();
			}
			else
			{
				OverflowedRPCArray.RemoveAt(0, NumProcessed);
			}
		}
	}

	void SpatialRPCService::ClearOverflowedRPCs(Worker_EntityId EntityId)
	{
		for (uint8 RPCType = static_cast<uint8>(ERPCType::ClientReliable); RPCType <= static_cast<uint8>(ERPCType::NetMulticast); RPCType++)
		{
			OverflowedRPCs.Remove(EntityRPCType(EntityId, static_cast<ERPCType>(RPCType)));
		}
	}

	TArray<SpatialRPCService::UpdateToSend> SpatialRPCService::GetRPCsAndAcksToSend()
	{
		TArray<SpatialRPCService::UpdateToSend> UpdatesToSend;

		for (auto& It : PendingComponentUpdatesToSend)
		{
			SpatialRPCService::UpdateToSend& UpdateToSend = UpdatesToSend.AddZeroed_GetRef();
			UpdateToSend.EntityId = It.Key.EntityId;
			UpdateToSend.Update.component_id = It.Key.ComponentId;
			UpdateToSend.Update.schema_type = It.Value;
#if TRACE_LIB_ACTIVE
			TraceKey Trace = InvalidTraceKey;
			PendingTraces.RemoveAndCopyValue(It.Key, Trace);
			UpdateToSend.Update.Trace = Trace;
#endif
		}

		PendingComponentUpdatesToSend.Empty();

		return UpdatesToSend;
	}

	TArray<FWorkerComponentData> SpatialRPCService::GetRPCComponentsOnEntityCreation(Worker_EntityId EntityId)
	{
		static Worker_ComponentId EndpointComponentIds[] = { SpatialConstants::CLIENT_ENDPOINT_COMPONENT_ID,
															 SpatialConstants::SERVER_ENDPOINT_COMPONENT_ID,
															 SpatialConstants::MULTICAST_RPCS_COMPONENT_ID };

		TArray<FWorkerComponentData> Components;

		for (Worker_ComponentId EndpointComponentId : EndpointComponentIds)
		{
			const EntityComponentId EntityComponent = { EntityId, EndpointComponentId };

			FWorkerComponentData& Component = Components.Emplace_GetRef(FWorkerComponentData{});
			Component.component_id = EndpointComponentId;
			if (Schema_ComponentData** ComponentData = PendingRPCsOnEntityCreation.Find(EntityComponent))
			{
				// When sending initial multicast RPCs, write the number of RPCs into a separate field instead of
				// last sent RPC ID field. When the server gains authority for the first time, it will copy the
				// value over to last sent RPC ID, so the clients that checked out the entity process the initial RPCs.
				if (EndpointComponentId == SpatialConstants::MULTICAST_RPCS_COMPONENT_ID)
				{
					RPCRingBufferUtils::MoveLastSentIdToInitiallyPresentCount(Schema_GetComponentDataFields(*ComponentData),
						LastSentRPCIds[EntityRPCType(EntityId, ERPCType::NetMulticast)]);
				}

				if (EndpointComponentId == SpatialConstants::CLIENT_ENDPOINT_COMPONENT_ID)
				{
					UE_LOG(LogSpatialRPCService, Error,
						TEXT("SpatialRPCService::GetRPCComponentsOnEntityCreation: Initial RPCs present on ClientEndpoint! EntityId: %lld"),
						EntityId);
				}

				Component.schema_type = *ComponentData;
#if TRACE_LIB_ACTIVE
				TraceKey Trace = InvalidTraceKey;
				PendingTraces.RemoveAndCopyValue(EntityComponent, Trace);
				Component.Trace = Trace;
#endif
				PendingRPCsOnEntityCreation.Remove(EntityComponent);
			}
			else
			{
				Component.schema_type = Schema_CreateComponentData();
			}
		}

		return Components;
	}

	void SpatialRPCService::ExtractRPCsForEntity(Worker_EntityId EntityId, Worker_ComponentId ComponentId)
	{
		switch (ComponentId)
		{
		case SpatialConstants::CLIENT_ENDPOINT_COMPONENT_ID:
			if (View->HasAuthority(EntityId, SpatialConstants::SERVER_ENDPOINT_COMPONENT_ID))
			{
				ExtractRPCsForType(EntityId, ERPCType::ServerReliable);
				ExtractRPCsForType(EntityId, ERPCType::ServerUnreliable);
			}
			break;
		case SpatialConstants::SERVER_ENDPOINT_COMPONENT_ID:
			if (View->HasAuthority(EntityId, SpatialConstants::CLIENT_ENDPOINT_COMPONENT_ID))
			{
				ExtractRPCsForType(EntityId, ERPCType::ClientReliable);
				ExtractRPCsForType(EntityId, ERPCType::ClientUnreliable);
			}
			break;
		case SpatialConstants::MULTICAST_RPCS_COMPONENT_ID:
			ExtractRPCsForType(EntityId, ERPCType::NetMulticast);
			break;
		case SpatialConstants::CROSSSERVER_SENDER_ENDPOINT_COMPONENT_ID:
			ExtractCrossServerRPCsForType(EntityId, ERPCType::CrossServerSender);
			break;
		case SpatialConstants::CROSSSERVER_RECEIVER_ENDPOINT_COMPONENT_ID:
			ExtractRPCsForType(EntityId, ERPCType::CrossServerReceiver);
			break;
		default:
			checkNoEntry();
			break;
		}
	}

	void SpatialRPCService::OnCheckoutMulticastRPCComponentOnEntity(Worker_EntityId EntityId)
	{
		const MulticastRPCs* Component = View->GetComponentData<MulticastRPCs>(EntityId);

		if (!ensure(Component != nullptr))
		{
			UE_LOG(LogSpatialRPCService, Error,
				TEXT("Multicast RPC component for entity with ID %lld was not present at point of checking out the component."), EntityId);
			return;
		}

		// When checking out entity, ignore multicast RPCs that are already on the component.
		LastSeenMulticastRPCIds.Add(EntityId, Component->MulticastRPCBuffer.LastSentRPCId);
	}

	void SpatialRPCService::OnRemoveMulticastRPCComponentForEntity(Worker_EntityId EntityId)
	{
		LastSeenMulticastRPCIds.Remove(EntityId);
	}

	void SpatialRPCService::OnEndpointAuthorityGained(Worker_EntityId EntityId, Worker_ComponentId ComponentId)
	{
		switch (ComponentId)
		{
		case SpatialConstants::CLIENT_ENDPOINT_COMPONENT_ID:
		{
			const ClientEndpoint* Endpoint = View->GetComponentData<ClientEndpoint>(EntityId);
			LastSeenRPCIds.Add(EntityRPCType(EntityId, ERPCType::ClientReliable), Endpoint->ReliableRPCAck);
			LastSeenRPCIds.Add(EntityRPCType(EntityId, ERPCType::ClientUnreliable), Endpoint->UnreliableRPCAck);
			LastAckedRPCIds.Add(EntityRPCType(EntityId, ERPCType::ClientReliable), Endpoint->ReliableRPCAck);
			LastAckedRPCIds.Add(EntityRPCType(EntityId, ERPCType::ClientUnreliable), Endpoint->UnreliableRPCAck);
			LastSentRPCIds.Add(EntityRPCType(EntityId, ERPCType::ServerReliable), Endpoint->ReliableRPCBuffer.LastSentRPCId);
			LastSentRPCIds.Add(EntityRPCType(EntityId, ERPCType::ServerUnreliable), Endpoint->UnreliableRPCBuffer.LastSentRPCId);
			break;
		}
		case SpatialConstants::SERVER_ENDPOINT_COMPONENT_ID:
		{
			const ServerEndpoint* Endpoint = View->GetComponentData<ServerEndpoint>(EntityId);
			LastSeenRPCIds.Add(EntityRPCType(EntityId, ERPCType::ServerReliable), Endpoint->ReliableRPCAck);
			LastSeenRPCIds.Add(EntityRPCType(EntityId, ERPCType::ServerUnreliable), Endpoint->UnreliableRPCAck);
			LastAckedRPCIds.Add(EntityRPCType(EntityId, ERPCType::ServerReliable), Endpoint->ReliableRPCAck);
			LastAckedRPCIds.Add(EntityRPCType(EntityId, ERPCType::ServerUnreliable), Endpoint->UnreliableRPCAck);
			LastSentRPCIds.Add(EntityRPCType(EntityId, ERPCType::ClientReliable), Endpoint->ReliableRPCBuffer.LastSentRPCId);
			LastSentRPCIds.Add(EntityRPCType(EntityId, ERPCType::ClientUnreliable), Endpoint->UnreliableRPCBuffer.LastSentRPCId);
			break;
		}
		case SpatialConstants::MULTICAST_RPCS_COMPONENT_ID:
		{
			const MulticastRPCs* Component = View->GetComponentData<MulticastRPCs>(EntityId);

			if (Component->MulticastRPCBuffer.LastSentRPCId == 0 && Component->InitiallyPresentMulticastRPCsCount > 0)
			{
				// Update last sent ID to the number of initially present RPCs so the clients who check out this entity
				// as it's created can process the initial multicast RPCs.
				LastSentRPCIds.Add(EntityRPCType(EntityId, ERPCType::NetMulticast), Component->InitiallyPresentMulticastRPCsCount);

				RPCRingBufferDescriptor Descriptor = RPCRingBufferUtils::GetRingBufferDescriptor(ERPCType::NetMulticast);
				Schema_Object* SchemaObject =
					Schema_GetComponentUpdateFields(GetOrCreateComponentUpdate(EntityComponentId{ EntityId, ComponentId }));
				Schema_AddUint64(SchemaObject, Descriptor.LastSentRPCFieldId, Component->InitiallyPresentMulticastRPCsCount);
			}
			else
			{
				LastSentRPCIds.Add(EntityRPCType(EntityId, ERPCType::NetMulticast), Component->MulticastRPCBuffer.LastSentRPCId);
			}

			break;
		}
		case SpatialConstants::CROSSSERVER_SENDER_ENDPOINT_COMPONENT_ID:
		{

			break;
		}
		case SpatialConstants::CROSSSERVER_SENDER_ACK_ENDPOINT_COMPONENT_ID:
		{
			const CrossServerEndpointSenderACK* Endpoint = View->GetComponentData<CrossServerEndpointSenderACK>(EntityId);
			if (ensure(Endpoint != nullptr))
			{
				if (Endpoint->DottedRPCACK.Num() != 0)
				{
					this->ACKComponentsToTrack.Add(EntityId);
				}
			}
			for (const auto& Entry : CrossServerMailbox)
			{
				if (Entry.Target == EntityId)
				{
					bShouldCheckSentRPCForLocalTarget = true;
				}
			}
			break;
		}
		case SpatialConstants::CROSSSERVER_RECEIVER_ENDPOINT_COMPONENT_ID:
		{

			break;
		}
		case SpatialConstants::CROSSSERVER_RECEIVER_ACK_ENDPOINT_COMPONENT_ID:
		{

			break;
		}
		default:
			checkNoEntry();
			break;
		}
	}

	void SpatialRPCService::OnEndpointAuthorityLost(Worker_EntityId EntityId, Worker_ComponentId ComponentId)
	{
		switch (ComponentId)
		{
		case SpatialConstants::CLIENT_ENDPOINT_COMPONENT_ID:
		{
			LastSeenRPCIds.Remove(EntityRPCType(EntityId, ERPCType::ClientReliable));
			LastSeenRPCIds.Remove(EntityRPCType(EntityId, ERPCType::ClientUnreliable));
			LastAckedRPCIds.Remove(EntityRPCType(EntityId, ERPCType::ClientReliable));
			LastAckedRPCIds.Remove(EntityRPCType(EntityId, ERPCType::ClientUnreliable));
			LastSentRPCIds.Remove(EntityRPCType(EntityId, ERPCType::ServerReliable));
			LastSentRPCIds.Remove(EntityRPCType(EntityId, ERPCType::ServerUnreliable));

			ClearOverflowedRPCs(EntityId);
			break;
		}
		case SpatialConstants::SERVER_ENDPOINT_COMPONENT_ID:
		{
			LastSeenRPCIds.Remove(EntityRPCType(EntityId, ERPCType::ServerReliable));
			LastSeenRPCIds.Remove(EntityRPCType(EntityId, ERPCType::ServerUnreliable));
			LastAckedRPCIds.Remove(EntityRPCType(EntityId, ERPCType::ServerReliable));
			LastAckedRPCIds.Remove(EntityRPCType(EntityId, ERPCType::ServerUnreliable));
			LastSentRPCIds.Remove(EntityRPCType(EntityId, ERPCType::ClientReliable));
			LastSentRPCIds.Remove(EntityRPCType(EntityId, ERPCType::ClientUnreliable));
			ClearOverflowedRPCs(EntityId);
			break;
		}
		case SpatialConstants::MULTICAST_RPCS_COMPONENT_ID:
		{
			// Set last seen to last sent, so we don't process own RPCs after crossing the boundary.
			LastSeenMulticastRPCIds.Add(EntityId, LastSentRPCIds[EntityRPCType(EntityId, ERPCType::NetMulticast)]);
			LastSentRPCIds.Remove(EntityRPCType(EntityId, ERPCType::NetMulticast));
			break;
		}
		case SpatialConstants::CROSSSERVER_SENDER_ENDPOINT_COMPONENT_ID:
		{
			break;
		}
		case SpatialConstants::CROSSSERVER_SENDER_ACK_ENDPOINT_COMPONENT_ID:
		{
			ACKComponentsToTrack.Remove(EntityId);
			break;
		}
		case SpatialConstants::CROSSSERVER_RECEIVER_ENDPOINT_COMPONENT_ID:
		{

			break;
		}
		case SpatialConstants::CROSSSERVER_RECEIVER_ACK_ENDPOINT_COMPONENT_ID:
		{

			break;
		}
		default:
			checkNoEntry();
			break;
		}
	}

	void SpatialRPCService::ExtractRPCsForType(Worker_EntityId EntityId, ERPCType Type)
	{
		uint64 LastSeenRPCId;
		EntityRPCType EntityTypePair = EntityRPCType(EntityId, Type);

		if (Type == ERPCType::NetMulticast)
		{
			LastSeenRPCId = LastSeenMulticastRPCIds[EntityId];
		}
		else
		{
			LastSeenRPCId = LastSeenRPCIds[EntityTypePair];
		}

		const RPCRingBuffer& Buffer = GetBufferFromView(EntityId, Type);

		uint64 LastProcessedRPCId = LastSeenRPCId;
		if (Buffer.LastSentRPCId >= LastSeenRPCId)
		{
			uint64 FirstRPCIdToRead = LastSeenRPCId + 1;

			uint32 BufferSize = RPCRingBufferUtils::GetRingBufferSize(Type);
			if (Buffer.LastSentRPCId > LastSeenRPCId + BufferSize)
			{
				UE_LOG(LogSpatialRPCService, Warning,
					TEXT("SpatialRPCService::ExtractRPCsForType: RPCs were overwritten without being processed! Entity: %lld, RPC type: %s, "
						"last seen RPC ID: %d, last sent ID: %d, buffer size: %d"),
					EntityId, *SpatialConstants::RPCTypeToString(Type), LastSeenRPCId, Buffer.LastSentRPCId, BufferSize);
				FirstRPCIdToRead = Buffer.LastSentRPCId - BufferSize + 1;
			}

			for (uint64 RPCId = FirstRPCIdToRead; RPCId <= Buffer.LastSentRPCId; RPCId++)
			{
				const TOptional<RPCPayload>& Element = Buffer.GetRingBufferElement(RPCId);
				if (Element.IsSet())
				{
					FUnrealObjectRef Counterpart;
					if (Buffer.Type == ERPCType::CrossServerSender)
					{
						Worker_EntityId TargetId = SpatialConstants::INVALID_ENTITY_ID;
						const TOptional<FUnrealObjectRef>& Target = Buffer.GetRingBufferCounterpart(RPCId);
						if (Target.IsSet())
						{
							TargetId = Target.GetValue().Entity;
						}
						Counterpart.Entity = TargetId;
						Counterpart.Offset = RPCId;
					}
					const bool bKeepExtracting = ExtractRPCCallback.Execute(EntityId, Counterpart, Type, Element.GetValue(), &Element - Buffer.RingBuffer.GetData());
					if (!bKeepExtracting)
					{
						break;
					}
					LastProcessedRPCId = RPCId;
				}
				else
				{
					UE_LOG(LogSpatialRPCService, Warning,
						TEXT("SpatialRPCService::ExtractRPCsForType: Ring buffer element empty. Entity: %lld, RPC type: %s, empty element "
							"RPC id: %d"),
						EntityId, *SpatialConstants::RPCTypeToString(Type), RPCId);
				}
			}
		}
		else
		{
			UE_LOG(LogSpatialRPCService, Warning,
				TEXT("SpatialRPCService::ExtractRPCsForType: Last sent RPC has smaller ID than last seen RPC. Entity: %lld, RPC type: %s, "
					"last sent ID: %d, last seen ID: %d"),
				EntityId, *SpatialConstants::RPCTypeToString(Type), Buffer.LastSentRPCId, LastSeenRPCId);
		}

		if (LastProcessedRPCId > LastSeenRPCId)
		{
			if (Type == ERPCType::NetMulticast)
			{
				LastSeenMulticastRPCIds[EntityId] = LastProcessedRPCId;
			}
			else
			{
				LastSeenRPCIds[EntityTypePair] = LastProcessedRPCId;
			}
		}
	}

	void SpatialRPCService::IncrementAckedRPCID(Worker_EntityId EntityId, ERPCType Type)
	{
		if (Type == ERPCType::NetMulticast)
		{
			return;
		}

		EntityRPCType EntityTypePair = EntityRPCType(EntityId, Type);
		uint64* LastAckedRPCId = LastAckedRPCIds.Find(EntityTypePair);
		if (LastAckedRPCId == nullptr)
		{
			UE_LOG(LogSpatialRPCService, Warning,
				TEXT("SpatialRPCService::IncrementAckedRPCID: Could not find last acked RPC id. Entity: %lld, RPC type: %s"), EntityId,
				*SpatialConstants::RPCTypeToString(Type));
			return;
		}

		++(*LastAckedRPCId);

		const EntityComponentId EntityComponentPair = { EntityId, RPCRingBufferUtils::GetAckComponentId(Type) };
		Schema_Object* EndpointObject = Schema_GetComponentUpdateFields(GetOrCreateComponentUpdate(EntityComponentPair));

		RPCRingBufferUtils::WriteAckToSchema(EndpointObject, Type, *LastAckedRPCId);
	}

	void SpatialRPCService::AddOverflowedRPC(EntityRPCType EntityType, RPCPayload&& Payload)
	{
		OverflowedRPCs.FindOrAdd(EntityType).Add(MoveTemp(Payload));
	}

	uint64 SpatialRPCService::GetAckFromView(Worker_EntityId EntityId, ERPCType Type)
	{
		switch (Type)
		{
		case ERPCType::ClientReliable:
			return View->GetComponentData<ClientEndpoint>(EntityId)->ReliableRPCAck;
		case ERPCType::ClientUnreliable:
			return View->GetComponentData<ClientEndpoint>(EntityId)->UnreliableRPCAck;
		case ERPCType::ServerReliable:
			return View->GetComponentData<ServerEndpoint>(EntityId)->ReliableRPCAck;
		case ERPCType::ServerUnreliable:
			return View->GetComponentData<ServerEndpoint>(EntityId)->UnreliableRPCAck;
		case ERPCType::CrossServerSender:
			return View->GetComponentData<CrossServerEndpointSenderACK>(EntityId)->RPCAck;
		case ERPCType::CrossServerReceiver:
			return View->GetComponentData<CrossServerEndpointReceiverACK>(EntityId)->RPCAck;
		}

		checkNoEntry();
		return 0;
	}

	const RPCRingBuffer& SpatialRPCService::GetBufferFromView(Worker_EntityId EntityId, ERPCType Type)
	{
		
		// Merge nonsense -> Not in master, but I did not add it????
		//if (!LastSeenMulticastRPCIds.Contains(EntityId))
		//{
		//	UE_LOG(LogSpatialRPCService, Warning,
		//		   TEXT("Tried to extract RPCs but no entry in Last Seen Map! This can happen after server travel. Entity: %lld, type: "
		//				"Multicast"),
		//		   EntityId);
		//	return;
		//}
		//LastSeenRPCId = LastSeenMulticastRPCIds[EntityId];

		switch (Type)
		{
			// Server sends Client RPCs, so ClientReliable & ClientUnreliable buffers live on ServerEndpoint.
		case ERPCType::ClientReliable:
			return View->GetComponentData<ServerEndpoint>(EntityId)->ReliableRPCBuffer;
		case ERPCType::ClientUnreliable:
			return View->GetComponentData<ServerEndpoint>(EntityId)->UnreliableRPCBuffer;

			// Client sends Server RPCs, so ServerReliable & ServerUnreliable buffers live on ClientEndpoint.
		case ERPCType::ServerReliable:
			return View->GetComponentData<ClientEndpoint>(EntityId)->ReliableRPCBuffer;
		case ERPCType::ServerUnreliable:
			return View->GetComponentData<ClientEndpoint>(EntityId)->UnreliableRPCBuffer;

		case ERPCType::NetMulticast:
			return View->GetComponentData<MulticastRPCs>(EntityId)->MulticastRPCBuffer;
		case ERPCType::CrossServerSender:
			return View->GetComponentData<CrossServerEndpointSender>(EntityId)->ReliableRPCBuffer;
		case ERPCType::CrossServerReceiver:
			return View->GetComponentData<CrossServerEndpointReceiver>(EntityId)->ReliableRPCBuffer;
		}

		checkNoEntry();
		static const RPCRingBuffer DummyBuffer(ERPCType::Invalid);
		return DummyBuffer;
	}

	Schema_ComponentUpdate* SpatialRPCService::GetOrCreateComponentUpdate(EntityComponentId EntityComponentIdPair)
	{
<<<<<<< HEAD
		if (!LastSeenRPCIds.Contains(EntityTypePair))
		{
			UE_LOG(LogSpatialRPCService, Warning,
				   TEXT("Tried to extract RPCs but no entry in Last Seen Map! This can happen after server travel. Entity: %lld, type: %s"),
				   EntityId, *SpatialConstants::RPCTypeToString(Type));
			return;
		}
		LastSeenRPCId = LastSeenRPCIds[EntityTypePair];
=======
		Schema_ComponentUpdate** ComponentUpdatePtr = PendingComponentUpdatesToSend.Find(EntityComponentIdPair);
		if (ComponentUpdatePtr == nullptr)
		{
			ComponentUpdatePtr = &PendingComponentUpdatesToSend.Add(EntityComponentIdPair, Schema_CreateComponentUpdate());
		}
		return *ComponentUpdatePtr;
>>>>>>> Cross Server RPC Spike
	}

	Schema_ComponentData* SpatialRPCService::GetOrCreateComponentData(EntityComponentId EntityComponentIdPair)
	{
		Schema_ComponentData** ComponentDataPtr = PendingRPCsOnEntityCreation.Find(EntityComponentIdPair);
		if (ComponentDataPtr == nullptr)
		{
			ComponentDataPtr = &PendingRPCsOnEntityCreation.Add(EntityComponentIdPair, Schema_CreateComponentData());
		}
		return *ComponentDataPtr;
	}

#if TRACE_LIB_ACTIVE
	void SpatialRPCService::ProcessResultToLatencyTrace(const EPushRPCResult Result, const TraceKey Trace)
	{
		if (SpatialLatencyTracer != nullptr && Trace != InvalidTraceKey)
		{
			bool bEndTrace = false;
			FString TraceMsg;
			switch (Result)
			{
			case SpatialGDK::EPushRPCResult::Success:
				// No further action
				break;
			case SpatialGDK::EPushRPCResult::QueueOverflowed:
				TraceMsg = TEXT("Overflowed");
				break;
			case SpatialGDK::EPushRPCResult::DropOverflowed:
				TraceMsg = TEXT("OverflowedAndDropped");
				bEndTrace = true;
				break;
			case SpatialGDK::EPushRPCResult::HasAckAuthority:
				TraceMsg = TEXT("NoAckAuth");
				bEndTrace = true;
				break;
			case SpatialGDK::EPushRPCResult::NoRingBufferAuthority:
				TraceMsg = TEXT("NoRingBufferAuth");
				bEndTrace = true;
				break;
			default:
				TraceMsg = TEXT("UnrecognisedResult");
				break;
			}

			if (bEndTrace)
			{
				// This RPC has been dropped, end the trace
				SpatialLatencyTracer->WriteAndEndTrace(Trace, TraceMsg, false);
			}
			else if (!TraceMsg.IsEmpty())
			{
				// This RPC will be sent later
				SpatialLatencyTracer->WriteToLatencyTrace(Trace, TraceMsg);
			}
		}
	}
#endif // TRACE_LIB_ACTIVE


	void SpatialRPCService::ExtractCrossServerRPCsForType(Worker_EntityId SenderId, ERPCType Type)
	{
		const RPCRingBuffer& Buffer = GetBufferFromView(SenderId, Type);

		uint64 MinRPCId = 0;
		TSet<Worker_EntityId> Receivers;
		for (uint32 Slot = 0; Slot < RPCRingBufferUtils::GetRingBufferSize(Type); ++Slot)
		{
			const TOptional<RPCPayload>& Element = Buffer.RingBuffer[Slot];
			if (Element.IsSet())
			{
				Worker_EntityId TargetId = SpatialConstants::INVALID_ENTITY_ID;
				const TOptional<FUnrealObjectRef>& Target = Buffer.Counterpart[Slot];
				if (Target.IsSet())
				{
					TargetId = Target.GetValue().Entity;

					uint64 RPCId = Target.GetValue().Offset;
					if (Slot == 0)
					{
						MinRPCId = RPCId;
					}
					MinRPCId = FMath::Min(RPCId, MinRPCId);

					if (View->HasAuthority(TargetId, SpatialConstants::POSITION_COMPONENT_ID))
					{
						CrossServerEndpointSenderACK* ACKComponent = View->GetComponentData<CrossServerEndpointSenderACK>(TargetId);
						TArray<uint64>* DottedACKList = ACKComponent->DottedRPCACK.Find(SenderId);
						if (DottedACKList == nullptr || (*DottedACKList)[Slot] < RPCId)
						{
							Receivers.Add(TargetId);
							FUnrealObjectRef Counterpart(SenderId, RPCId);
							const bool bKeepExtracting = ExtractRPCCallback.Execute(TargetId, Counterpart, Type, Element.GetValue(), Slot);
							if (!bKeepExtracting)
							{
								break;
							}
						}
					}
				}
			}
		}
		if (Receivers.Num() != 0)
		{
			CleanupACKsFor(SenderId, MinRPCId, Receivers);
		}
	}

	void SpatialRPCService::WriteCrossServerACKFor(Worker_EntityId Receiver, Worker_EntityId Sender, uint64 RPCId, uint32 Slot)
	{
		CrossServerEndpointSenderACK* ACKComponent = View->GetComponentData<CrossServerEndpointSenderACK>(Receiver);
		TArray<uint64>* DottedACKList = ACKComponent->DottedRPCACK.Find(Sender);
		if (DottedACKList == nullptr)
		{
			TArray<uint64> NewACKList;
			NewACKList.SetNumZeroed(RPCRingBufferUtils::GetRingBufferSize(ERPCType::CrossServerSender));
			DottedACKList = &ACKComponent->DottedRPCACK.Add(Sender, MoveTemp(NewACKList));
		}

		(*DottedACKList)[Slot] = RPCId;
		ACKComponent->RPCAck++;

		UE_LOG(LogSpatialRPCService, Log, TEXT("DEBUG_CS %llu ACKED RPC %i from %llu with count %i"), Receiver, RPCId, Sender, ACKComponent->RPCAck);

		EntityComponentId Pair;
		Pair.EntityId = Receiver;
		Pair.ComponentId = RPCRingBufferUtils::GetAckComponentId(ERPCType::CrossServerSender);


		if (Schema_ComponentUpdate** PendingUpdate = PendingComponentUpdatesToSend.Find(Pair))
		{
			Schema_DestroyComponentUpdate(*PendingUpdate);
			PendingComponentUpdatesToSend.Remove(Pair);
		}

		Schema_ComponentUpdate* Update = GetOrCreateComponentUpdate(Pair);
		Schema_Object* UpdateObject = Schema_GetComponentUpdateFields(Update);

		ACKComponent->CreateUpdate(Update);
		ACKComponentsToTrack.Add(Receiver);

		if (View->HasAuthority(Sender, SpatialConstants::CROSSSERVER_SENDER_ENDPOINT_COMPONENT_ID))
		{
			UpdateMergedACKs(Sender, Receiver);
		}
	}

	void SpatialRPCService::UpdateMergedACKs(Worker_EntityId WorkerId, Worker_EntityId RemoteReceiver)
	{
		CrossServerEndpointSenderACK* ACKComponent = View->GetComponentData<CrossServerEndpointSenderACK>(RemoteReceiver);
		if (TArray<uint64>* DottedACKList = ACKComponent->DottedRPCACK.Find(WorkerId))
		{
			for (int32 Slot = 0; Slot < DottedACKList->Num(); ++Slot)
			{
				SentRPCEntry& Entry = CrossServerMailbox[Slot];
				Entry.MergedCrossServerACK = FMath::Max(Entry.MergedCrossServerACK, (*DottedACKList)[Slot]);
				if (Entry.MergedCrossServerACK == Entry.RPCId)
				{
					Entry.EntityRequest.Reset();
				}
			}
		}
	}

	TOptional<uint32_t> SpatialRPCService::FindFreeSlotForCrossServerSender()
	{
		uint64 LowestId;
		TOptional<uint32_t> FreeSlot;
		for (int32 Slot = 0; Slot < CrossServerMailbox.Num(); ++Slot)
		{
			SentRPCEntry& Entry = CrossServerMailbox[Slot];
			uint64 CurSlotRPCId = Entry.RPCId;
			if (CurSlotRPCId == Entry.MergedCrossServerACK)
			{
				if (!FreeSlot
					|| CurSlotRPCId < LowestId)
				{
					FreeSlot = Slot;
					LowestId = CurSlotRPCId;
				}
			}
		}

		return FreeSlot;
	}

	void SpatialRPCService::CleanupACKsFor(Worker_EntityId Sender, uint64 MinRPCId, TSet<Worker_EntityId> const& ReceiversToIgnore)
	{
		for (TSet<Worker_EntityId>::TIterator Iterator = ACKComponentsToTrack.CreateIterator(); Iterator; ++Iterator)
		{
			Worker_EntityId Receiver = *Iterator;
			if (ReceiversToIgnore.Contains(Receiver))
			{
				continue;
			}

			CrossServerEndpointSenderACK* ACKComponent = View->GetComponentData<CrossServerEndpointSenderACK>(Receiver);
			if (!ACKComponent)
			{
				continue;
			}
			if (TArray<uint64>* DottedACKList = ACKComponent->DottedRPCACK.Find(Sender))
			{
				bool bStillRelevant = false;
				for (auto RPCId : *DottedACKList)
				{
					if (RPCId >= MinRPCId)
					{
						bStillRelevant = true;
						break;
					}
				}
				if (!bStillRelevant)
				{
					ACKComponent->DottedRPCACK.Remove(Sender);
					if (ACKComponent->DottedRPCACK.Num() == 0)
					{
						Iterator.RemoveCurrent();
					}

					EntityComponentId Pair;
					Pair.EntityId = Receiver;
					Pair.ComponentId = RPCRingBufferUtils::GetAckComponentId(ERPCType::CrossServerSender);

					if (Schema_ComponentUpdate** PendingUpdate = PendingComponentUpdatesToSend.Find(Pair))
					{
						Schema_DestroyComponentUpdate(*PendingUpdate);
						PendingComponentUpdatesToSend.Remove(Pair);
					}

					Schema_ComponentUpdate* Update = GetOrCreateComponentUpdate(Pair);
					Schema_Object* UpdateObject = Schema_GetComponentUpdateFields(Update);
					ACKComponent->CreateUpdate(Update);
				}
			}
		}
	}

	void SpatialRPCService::CheckLocalTargets(Worker_EntityId LocalWorkerEntityId)
	{
		if (bShouldCheckSentRPCForLocalTarget)
		{
			bShouldCheckSentRPCForLocalTarget = false;
			ExtractCrossServerRPCsForType(LocalWorkerEntityId, ERPCType::CrossServerSender);
		}
	}
}

// 9/10 sur l'echelle du goret.
#include "Interop/Connection/SpatialOSWorkerInterface.h"

namespace SpatialGDK
{

void SpatialRPCService::HandleTimeout(SpatialOSWorkerInterface* Worker)
{
	uint64 Now = FPlatformTime::Cycles64();
	uint64 CutoffTime = Now - (0.5 / FPlatformTime::GetSecondsPerCycle64());
	// Consider timeouts.
	for (int32 Slot = 0; Slot < CrossServerMailbox.Num(); ++Slot)
	{
		SentRPCEntry& Entry = CrossServerMailbox[Slot];
		// RPC is in flight and overdue.
		if (Entry.RPCId != Entry.MergedCrossServerACK && Entry.Timestamp < CutoffTime)
		{
			if (!Entry.EntityRequest)
			{
				Worker_EntityQuery query;
				query.constraint.constraint_type = WORKER_CONSTRAINT_TYPE_ENTITY_ID;
				query.constraint.constraint.entity_id_constraint.entity_id = Entry.Target;
				// Count should be used, but will be deprecated ?? WOuld that be a reason to keep it ?
				query.result_type = WORKER_RESULT_TYPE_SNAPSHOT;
				query.snapshot_result_type_component_id_count = 1;
				Worker_ComponentId componentResults[] = { SpatialConstants::POSITION_COMPONENT_ID };
				query.snapshot_result_type_component_ids = componentResults;
				Entry.EntityRequest = Worker->SendEntityQueryRequest(&query);
			}
		}
	}
}

bool SpatialRPCService::OnEntityRequestResponse(Worker_RequestId Request, bool bEntityExists)
{
	for (int32 Slot = 0; Slot < CrossServerMailbox.Num(); ++Slot)
	{
		SentRPCEntry& Entry = CrossServerMailbox[Slot];
		if (Entry.EntityRequest && Entry.EntityRequest.GetValue() == Request)
		{
			Entry.EntityRequest.Reset();
			if (bEntityExists)
			{
				// Extend timeout
				Entry.Timestamp = FPlatformTime::Cycles64();
			}
			else
			{
				// Clear slot
				Entry.RPCId = 0;
				Entry.MergedCrossServerACK = 0;
			}
			return true;
		}
	}

	return false;
}

} // namespace SpatialGDK
