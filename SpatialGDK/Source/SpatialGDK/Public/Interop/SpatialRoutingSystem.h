// Copyright (c) Improbable Worlds Ltd, All Rights Reserved

#pragma once

#include "CoreMinimal.h"

#include "Schema/CrossServerEndpoint.h"
#include "SpatialView/SubView.h"
#include "Utils/CrossServerUtils.h"

DECLARE_LOG_CATEGORY_EXTERN(LogSpatialRoutingSystem, Log, All)

class USpatialWorkerConnection;

namespace SpatialGDK
{
struct SpatialRoutingSystem
{
	SpatialRoutingSystem(const FSubView& InSubView, FString InRoutingWorkerId)
		: SubView(InSubView)
		, RoutingWorkerId(InRoutingWorkerId)
	{
	}

	void Init(USpatialWorkerConnection* Connection);

	void TickDispatch(USpatialWorkerConnection* Connection);

	void TickFlush(USpatialWorkerConnection* Connection);

	const FSubView& SubView;

	struct RoutingComponents
	{
		TOptional<CrossServerEndpointSender> Sender;

		// Allocation slots for Sender ACKSlots
		CrossServer::SlotAlloc SenderACKAlloc;

		// Map of Receiver slots and Sender ACK slots to check when the sender buffer changes.
		CrossServer::RPCAllocMap AllocMap;

		CrossServer::SenderState Receiver;
		// CrossServerEndpointSenderACK SenderACK;
		// CrossServerEndpointReceiver Receiver;

		TOptional<CrossServerEndpointReceiverACK> ReceiverACK;
	};

	void ProcessUpdate(Worker_EntityId, const ComponentChange& Change, RoutingComponents& Components);

	void OnSenderChanged(Worker_EntityId, RoutingComponents& Components);
	void OnReceiverACKChanged(Worker_EntityId, RoutingComponents& Components);

	typedef TPair<Worker_EntityId_Key, Worker_ComponentId> EntityComponentId;

	Schema_ComponentUpdate* GetOrCreateComponentUpdate(EntityComponentId EntityComponentIdPair);

	TMap<Worker_EntityId_Key, RoutingComponents> RoutingWorkerView;

	TMap<EntityComponentId, Schema_ComponentUpdate*> PendingComponentUpdatesToSend;

	void CreateRoutingWorkerEntity(USpatialWorkerConnection* Connection);
	Worker_RequestId RoutingWorkerEntityRequest;
	Worker_EntityId RoutingWorkerEntity;
	FString RoutingWorkerId;
};
} // namespace SpatialGDK
