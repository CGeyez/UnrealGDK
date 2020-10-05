#include "Interop/SpatialRoutingSystem.h"
#include "Interop/Connection/SpatialWorkerConnection.h"
#include "Schema/ServerWorker.h"
#include "Utils/InterestFactory.h"

DEFINE_LOG_CATEGORY(LogSpatialRoutingSystem);

#pragma optimize("", off)

namespace SpatialGDK
{
void SpatialRoutingSystem::ProcessUpdate(Worker_EntityId Entity, const ComponentChange& Change, RoutingComponents& Components)
{
	switch (Change.ComponentId)
	{
	case SpatialConstants::CROSSSERVER_SENDER_ENDPOINT_COMPONENT_ID:
		switch (Change.Type)
		{
		case ComponentChange::COMPLETE_UPDATE:
		{
			Worker_ComponentData Data;
			Data.component_id = SpatialConstants::CROSSSERVER_SENDER_ENDPOINT_COMPONENT_ID;
			Data.schema_type = Change.CompleteUpdate.Data;
			Components.Sender.Emplace(CrossServerEndpointSender(Data));

			break;
		}
		case ComponentChange::UPDATE:
		{
			Worker_ComponentUpdate Update;
			Update.component_id = SpatialConstants::CROSSSERVER_SENDER_ENDPOINT_COMPONENT_ID;
			Update.schema_type = Change.Update;
			Components.Sender->ApplyComponentUpdate(Update);
			break;
		}
		}
		OnSenderChanged(Entity, Components);
		break;
	case SpatialConstants::CROSSSERVER_RECEIVER_ACK_ENDPOINT_COMPONENT_ID:
		switch (Change.Type)
		{
		case ComponentChange::COMPLETE_UPDATE:
		{
			Worker_ComponentData Data;
			Data.component_id = SpatialConstants::CROSSSERVER_RECEIVER_ACK_ENDPOINT_COMPONENT_ID;
			Data.schema_type = Change.CompleteUpdate.Data;
			Components.ReceiverACK.Emplace(CrossServerEndpointReceiverACK(Data));
			break;
		}

		case ComponentChange::UPDATE:
		{
			Worker_ComponentUpdate Update;
			Update.component_id = SpatialConstants::CROSSSERVER_RECEIVER_ACK_ENDPOINT_COMPONENT_ID;
			Update.schema_type = Change.Update;
			Components.ReceiverACK->ApplyComponentUpdate(Update);
			break;
		}
		}
		OnReceiverACKChanged(Entity, Components);
		break;
	default:
		checkNoEntry();
		break;
	}
}

void SpatialRoutingSystem::OnSenderChanged(Worker_EntityId SenderId, RoutingComponents& Components)
{
	CrossServer::RPCAllocMap SlotsToClear = Components.AllocMap;

	const RPCRingBuffer& Buffer = Components.Sender->ReliableRPCBuffer;

	// First loop to free Receiver slots.
	for (uint32 Slot = 0; Slot < RPCRingBufferUtils::GetRingBufferSize(ERPCType::CrossServerSender); ++Slot)
	{
		const TOptional<RPCPayload>& Element = Buffer.RingBuffer[Slot];
		if (Element.IsSet())
		{
			const TOptional<FUnrealObjectRef>& Counterpart = Buffer.Counterpart[Slot];

			Worker_EntityId Receiver = Counterpart->Entity;
			uint64 RPCId = Counterpart->Offset;
			CrossServer::RPCKey RPCKey = MakeTuple(SenderId, RPCId);
			SlotsToClear.Remove(RPCKey);
		}
	}

	for (auto const& SlotToClear : SlotsToClear)
	{
		const CrossServer::RPCSlots& Slots = SlotToClear.Value;

		{
			EntityComponentId SenderPair = MakeTuple(SenderId, SpatialConstants::CROSSSERVER_SENDER_ACK_ENDPOINT_COMPONENT_ID);
			Components.SenderACKAlloc.Occupied[Slots.SenderACKSlot] = false;
			Components.SenderACKAlloc.ToClear[Slots.SenderACKSlot] = true;

			GetOrCreateComponentUpdate(SenderPair);
		}

		if (RoutingComponents* ReceiverComps = RoutingWorkerView.Find(Slots.ReceiverSlot.Receiver))
		{
			EntityComponentId ReceiverPair =
				MakeTuple(Slots.ReceiverSlot.Receiver, SpatialConstants::CROSSSERVER_RECEIVER_ENDPOINT_COMPONENT_ID);
			uint32 SlotIdx = Slots.ReceiverSlot.Slot;

			CrossServer::SentRPCEntry* SentRPC = ReceiverComps->Receiver.Mailbox.Find(SlotToClear.Key);
			check(SentRPC != nullptr);

			ReceiverComps->Receiver.Mailbox.Remove(SlotToClear.Key);
			ReceiverComps->Receiver.Alloc.Occupied[SlotIdx] = false;
			ReceiverComps->Receiver.Alloc.ToClear[SlotIdx] = true;

			GetOrCreateComponentUpdate(ReceiverPair);
		}

		Components.AllocMap.Remove(SlotToClear.Key);
	}

	for (uint32 Slot = 0; Slot < RPCRingBufferUtils::GetRingBufferSize(ERPCType::CrossServerSender); ++Slot)
	{
		const TOptional<RPCPayload>& Element = Buffer.RingBuffer[Slot];
		if (Element.IsSet())
		{
			const TOptional<FUnrealObjectRef>& Counterpart = Buffer.Counterpart[Slot];

			Worker_EntityId Receiver = Counterpart->Entity;
			uint64 RPCId = Counterpart->Offset;

			if (RoutingComponents* ReceiverComps = RoutingWorkerView.Find(Receiver))
			{
				CrossServer::RPCKey RPCKey = MakeTuple(SenderId, RPCId);
				CrossServer::SentRPCEntry* SentRPC = ReceiverComps->Receiver.Mailbox.Find(RPCKey);
				if (SentRPC == nullptr)
				{
					TOptional<uint32> FreeSlot = ReceiverComps->Receiver.FindFreeSlot();
					if (FreeSlot)
					{
						Schema_ComponentUpdate* ReceiverUpdate =
							GetOrCreateComponentUpdate(MakeTuple(Receiver, SpatialConstants::CROSSSERVER_RECEIVER_ENDPOINT_COMPONENT_ID));
						Schema_Object* EndpointObject = Schema_GetComponentUpdateFields(ReceiverUpdate);

						uint32 SlotIdx = FreeSlot.GetValue();
						RPCRingBufferDescriptor Descriptor = RPCRingBufferUtils::GetRingBufferDescriptor(ERPCType::CrossServerReceiver);
						uint32 Field = Descriptor.GetRingBufferElementFieldId(ERPCType::CrossServerReceiver, SlotIdx + 1);

						Schema_Object* RPCObject = Schema_AddObject(EndpointObject, Field);
						Element->WriteToSchemaObject(RPCObject);

						FUnrealObjectRef SenderBackRef(SenderId, RPCId);
						AddObjectRefToSchema(EndpointObject, Field + 1, SenderBackRef);

						// Schema_ClearField(EndpointObject, Descriptor.LastSentRPCFieldId);
						// Schema_AddUint64(EndpointObject, Descriptor.LastSentRPCFieldId, SenderComp->ReliableRPCBuffer.LastSentRPCId);

						CrossServer::SentRPCEntry Entry;
						Entry.RPCId = RPCId;
						Entry.Target = SenderBackRef.Entity;
						Entry.Timestamp = FPlatformTime::Cycles64();
						Entry.Slot = SlotIdx;

						ReceiverComps->Receiver.Mailbox.Add(RPCKey, Entry);
						ReceiverComps->Receiver.Alloc.Occupied[SlotIdx] = true;
						ReceiverComps->Receiver.Alloc.ToClear[SlotIdx] = false;

						CrossServer::ACKSlot ReceiverSlot;
						ReceiverSlot.Receiver = Receiver;
						ReceiverSlot.Slot = SlotIdx;

						Components.AllocMap.Add(RPCKey).ReceiverSlot = ReceiverSlot;
					}
					else
					{
						// Out of receiver slots, schedule a scan once one is freed ?
						UE_LOG(LogTemp, Log, TEXT("Out of receiver slot"));
					}
				}
				else
				{
					// Skip, already delivered
				}
			}
			else
			{
				// Receiver dead, ACK ?
				UE_LOG(LogTemp, Log, TEXT("Receiver dead"));
			}
		}
	}
}

void SpatialRoutingSystem::OnReceiverACKChanged(Worker_EntityId EntityId, RoutingComponents& Components)
{
	CrossServerEndpointReceiverACK& ReceiverACK = Components.ReceiverACK.GetValue();
	for (int32 SlotIdx = 0; SlotIdx < ReceiverACK.ACKArray.Num(); ++SlotIdx)
	{
		if (ReceiverACK.ACKArray[SlotIdx])
		{
			const ACKItem& ReceiverACKItem = ReceiverACK.ACKArray[SlotIdx].GetValue();

			CrossServer::RPCKey RPCKey = MakeTuple(ReceiverACKItem.Sender, ReceiverACKItem.RPCId);
			CrossServer::SentRPCEntry* SentRPC = Components.Receiver.Mailbox.Find(RPCKey);
			if (SentRPC == nullptr)
			{
				continue;
			}

			RoutingComponents* SenderComponents = RoutingWorkerView.Find(ReceiverACKItem.Sender);
			if (SenderComponents != nullptr)
			{
				// Both SentRPC and Slots entries should be cleared at the same time.
				CrossServer::RPCSlots* Slots = SenderComponents->AllocMap.Find(RPCKey);
				check(Slots != nullptr);

				if (Slots->SenderACKSlot < 0)
				{
					Slots->SenderACKSlot = SenderComponents->SenderACKAlloc.Occupied.FindAndSetFirstZeroBit();
					if (Slots->SenderACKSlot >= 0)
					{
						SenderComponents->SenderACKAlloc.ToClear[Slots->SenderACKSlot] = false;

						EntityComponentId SenderPair =
							MakeTuple(ReceiverACKItem.Sender, SpatialConstants::CROSSSERVER_SENDER_ACK_ENDPOINT_COMPONENT_ID);
						Schema_ComponentUpdate* Update = GetOrCreateComponentUpdate(SenderPair);
						Schema_Object* UpdateObject = Schema_GetComponentUpdateFields(Update);

						ACKItem SenderACK;
						SenderACK.Sender = RPCKey.Get<0>();
						SenderACK.RPCId = RPCKey.Get<1>();

						Schema_Object* NewEntry = Schema_AddObject(UpdateObject, 2 + SlotIdx);
						SenderACK.WriteToSchema(NewEntry);
					}
					else
					{
						// Out of free slots, schedule update when one is freed ?
						UE_LOG(LogTemp, Log, TEXT("Out of sender ACK slot"));
					}
				}
				else
				{
					// Already taken care of.
				}
			}
			else
			{
				// Sender disappeared, clear Receiver slot.
				UE_LOG(LogTemp, Log, TEXT("Sender disappeared"));
			}
		}
	}
}

Schema_ComponentUpdate* SpatialRoutingSystem::GetOrCreateComponentUpdate(
	TPair<Worker_EntityId_Key, Worker_ComponentId> EntityComponentIdPair)
{
	Schema_ComponentUpdate** ComponentUpdatePtr = PendingComponentUpdatesToSend.Find(EntityComponentIdPair);
	if (ComponentUpdatePtr == nullptr)
	{
		ComponentUpdatePtr = &PendingComponentUpdatesToSend.Add(EntityComponentIdPair, Schema_CreateComponentUpdate());
	}
	return *ComponentUpdatePtr;
}

void SpatialRoutingSystem::TickDispatch(USpatialWorkerConnection* Connection)
{
	const TArray<Worker_Op>& Messages = Connection->GetWorkerMessages();
	for (const auto& Message : Messages)
	{
		switch (Message.op_type)
		{
		case WORKER_OP_TYPE_RESERVE_ENTITY_IDS_RESPONSE:
		{
			const Worker_ReserveEntityIdsResponseOp& Op = Message.op.reserve_entity_ids_response;
			if (Op.request_id == RoutingWorkerEntityRequest)
			{
				if (Op.first_entity_id == SpatialConstants::INVALID_ENTITY_ID)
				{
					UE_LOG(LogSpatialRoutingSystem, Error, TEXT("Reserve entity failed : %s"), UTF8_TO_TCHAR(Op.message));
					RoutingWorkerEntityRequest = 0;
				}
				else
				{
					RoutingWorkerEntity = Message.op.reserve_entity_ids_response.first_entity_id;
					CreateRoutingWorkerEntity(Connection);
				}
			}
			break;
		}
		case WORKER_OP_TYPE_CREATE_ENTITY_RESPONSE:
		{
			const Worker_CreateEntityResponseOp& Op = Message.op.create_entity_response;
			if (Op.request_id == RoutingWorkerEntityRequest)
			{
				if (Op.entity_id == SpatialConstants::INVALID_ENTITY_ID)
				{
					UE_LOG(LogSpatialRoutingSystem, Error, TEXT("Create entity failed : %s"), UTF8_TO_TCHAR(Op.message));
					RoutingWorkerEntityRequest = 0;
				}
			}
		}
		break;
		}
	}

	const FSubViewDelta& SubViewDelta = SubView.GetViewDelta();
	for (const EntityDelta& Delta : SubViewDelta.EntityDeltas)
	{
		switch (Delta.Type)
		{
		case EntityDelta::UPDATE:
		{
			RoutingComponents& Components = RoutingWorkerView.FindChecked(Delta.EntityId);
			for (const ComponentChange& Change : Delta.ComponentUpdates)
			{
				ProcessUpdate(Delta.EntityId, Change, Components);
			}

			for (const ComponentChange& Change : Delta.ComponentsRefreshed)
			{
				ProcessUpdate(Delta.EntityId, Change, Components);
			}
		}
		break;
		case EntityDelta::ADD:
		{
			const EntityViewElement& EntityView = SubView.GetView()->FindChecked(Delta.EntityId);
			RoutingComponents& Components = RoutingWorkerView.Add(Delta.EntityId);

			for (const auto& ComponentDesc : EntityView.Components)
			{
				switch (ComponentDesc.GetComponentId())
				{
				case SpatialConstants::CROSSSERVER_SENDER_ENDPOINT_COMPONENT_ID:
					Components.Sender = CrossServerEndpointSender(ComponentDesc.GetWorkerComponentData());
					break;
				case SpatialConstants::CROSSSERVER_SENDER_ACK_ENDPOINT_COMPONENT_ID:
				{
					CrossServerEndpointSenderACK TempView(ComponentDesc.GetWorkerComponentData());
					for (int32 SlotIdx = 0; SlotIdx < TempView.ACKArray.Num(); ++SlotIdx)
					{
						const auto& Slot = TempView.ACKArray[SlotIdx];
						if (Slot.IsSet())
						{
							CrossServer::RPCSlots& Slots = Components.AllocMap.FindOrAdd(MakeTuple(Slot->Sender, Slot->RPCId));
							Slots.SenderACKSlot = SlotIdx;
							Components.SenderACKAlloc.Occupied[SlotIdx] = true;
						}
					}
				}
				break;
				case SpatialConstants::CROSSSERVER_RECEIVER_ENDPOINT_COMPONENT_ID:
				{
					CrossServerEndpointReceiver TempView(ComponentDesc.GetWorkerComponentData());
					for (int32 SlotIdx = 0; SlotIdx < TempView.ReliableRPCBuffer.RingBuffer.Num(); ++SlotIdx)
					{
						const auto& Slot = TempView.ReliableRPCBuffer.RingBuffer[SlotIdx];
						if (Slot.IsSet())
						{
							const TOptional<FUnrealObjectRef>& SenderBackRef = TempView.ReliableRPCBuffer.Counterpart[SlotIdx];
							check(SenderBackRef.IsSet());

							CrossServer::SentRPCEntry NewEntry;
							NewEntry.RPCId = SenderBackRef->Offset;
							NewEntry.Slot = SlotIdx;
							NewEntry.Target = SenderBackRef->Entity;
							NewEntry.Timestamp = 0;
							NewEntry.EntityRequest = 0;

							CrossServer::RPCKey RPCKey = MakeTuple(NewEntry.Target, NewEntry.RPCId);

							Components.Receiver.Mailbox.Add(RPCKey, NewEntry);
							Components.Receiver.Alloc.Occupied[SlotIdx] = true;

							RoutingComponents& SenderComponents = RoutingWorkerView.FindOrAdd(NewEntry.Target);
							CrossServer::RPCSlots& Slots = SenderComponents.AllocMap.FindOrAdd(MakeTuple(NewEntry.Target, NewEntry.RPCId));
							Slots.ReceiverSlot.Receiver = Delta.EntityId;
							Slots.ReceiverSlot.Slot = SlotIdx;
						}
					}
				}
				break;
				case SpatialConstants::CROSSSERVER_RECEIVER_ACK_ENDPOINT_COMPONENT_ID:
					Components.ReceiverACK = CrossServerEndpointReceiverACK(ComponentDesc.GetWorkerComponentData());
					break;
				}
			}
		}
		break;
		case EntityDelta::REMOVE:
		{
			// Some cleanup would be in order.
			checkNoEntry();
			RoutingWorkerView.Remove(Delta.EntityId);
		}
		break;
		case EntityDelta::TEMPORARILY_REMOVED:
		{
		}
		break;
		default:
			break;
		}
	}
}

void SpatialRoutingSystem::TickFlush(USpatialWorkerConnection* Connection)
{
	for (auto& Entry : PendingComponentUpdatesToSend)
	{
		Worker_EntityId Entity = Entry.Key.Get<0>();
		Worker_ComponentId CompId = Entry.Key.Get<1>();

		if (RoutingComponents* Components = RoutingWorkerView.Find(Entity))
		{
			if (CompId == SpatialConstants::CROSSSERVER_RECEIVER_ENDPOINT_COMPONENT_ID)
			{
				RPCRingBufferDescriptor Descriptor = RPCRingBufferUtils::GetRingBufferDescriptor(ERPCType::CrossServerReceiver);

				for (int32 ToClear = Components->Receiver.Alloc.ToClear.Find(true); ToClear >= 0;
					 ToClear = Components->Receiver.Alloc.ToClear.Find(true))
				{
					uint32 Field = Descriptor.GetRingBufferElementFieldId(ERPCType::CrossServerReceiver, ToClear + 1);

					Schema_AddComponentUpdateClearedField(Entry.Value, Field);
					Schema_AddComponentUpdateClearedField(Entry.Value, Field + 1);

					Components->Receiver.Alloc.ToClear[ToClear] = false;
				}
			}

			if (CompId == SpatialConstants::CROSSSERVER_SENDER_ACK_ENDPOINT_COMPONENT_ID)
			{
				for (int32 ToClear = Components->SenderACKAlloc.ToClear.Find(true); ToClear >= 0;
					 ToClear = Components->SenderACKAlloc.ToClear.Find(true))
				{
					Schema_AddComponentUpdateClearedField(Entry.Value, ToClear + 2);

					Components->SenderACKAlloc.ToClear[ToClear] = false;
				}
			}
		}
		FWorkerComponentUpdate Update;
		Update.component_id = CompId;
		Update.schema_type = Entry.Value;
		Connection->SendComponentUpdate(Entity, &Update);
	}
	PendingComponentUpdatesToSend.Empty();
}

void SpatialRoutingSystem::Init(USpatialWorkerConnection* Connection)
{
	RoutingWorkerEntityRequest = Connection->SendReserveEntityIdsRequest(1);
}

void SpatialRoutingSystem::CreateRoutingWorkerEntity(USpatialWorkerConnection* Connection)
{
	const WorkerRequirementSet WorkerIdPermission{ { FString::Printf(TEXT("workerId:%s"), *RoutingWorkerId) } };

	WriteAclMap ComponentWriteAcl;
	ComponentWriteAcl.Add(SpatialConstants::POSITION_COMPONENT_ID, WorkerIdPermission);
	ComponentWriteAcl.Add(SpatialConstants::METADATA_COMPONENT_ID, WorkerIdPermission);
	ComponentWriteAcl.Add(SpatialConstants::ENTITY_ACL_COMPONENT_ID, WorkerIdPermission);
	ComponentWriteAcl.Add(SpatialConstants::INTEREST_COMPONENT_ID, WorkerIdPermission);
	// ComponentWriteAcl.Add(SpatialConstants::SERVER_WORKER_COMPONENT_ID, WorkerIdPermission);

	TArray<FWorkerComponentData> Components;
	Components.Add(Position().CreatePositionData());
	Components.Add(Metadata(FString::Printf(TEXT("WorkerEntity:%s"), *RoutingWorkerId)).CreateMetadataData());
	Components.Add(EntityAcl(WorkerIdPermission, ComponentWriteAcl).CreateEntityAclData());
	// Components.Add(ServerWorker(Connection->GetWorkerId(), false).CreateServerWorkerData());

	Components.Add(InterestFactory::CreateRoutingWorkerInterest().CreateInterestData());

	RoutingWorkerEntityRequest = Connection->SendCreateEntityRequest(Components, &RoutingWorkerEntity);
}

} // namespace SpatialGDK
