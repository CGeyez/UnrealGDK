// Copyright (c) Improbable Worlds Ltd, All Rights Reserved

#include "SpatialView/ViewCoordinator.h"
#include "SpatialView/OpList/ViewDeltaLegacyOpList.h"

namespace SpatialGDK
{
ViewCoordinator::ViewCoordinator(TUniquePtr<AbstractConnectionHandler> ConnectionHandler)
	: ConnectionHandler(MoveTemp(ConnectionHandler))
	, NextRequestId(1)
	, ReserveEntityIdRetryHandler(&View)
	, CreateEntityRetryHandler(&View)
	, DeleteEntityRetryHandler(&View)
	, EntityQueryRetryHandler(&View)
	, EntityCommandRetryHandler(&View)
{
}

ViewCoordinator::~ViewCoordinator()
{
	FlushMessagesToSend();
}

void ViewCoordinator::Advance(float DeltaTimeS)
{
	ConnectionHandler->Advance();
	const uint32 OpListCount = ConnectionHandler->GetOpListCount();
	for (uint32 i = 0; i < OpListCount; ++i)
	{
		View.EnqueueOpList(ConnectionHandler->GetNextOpList());
	}
	View.AdvanceViewDelta();
	Dispatcher.InvokeCallbacks(View.GetViewDelta().GetEntityDeltas());

	for (const TUniquePtr<FSubView>& SubviewToAdvance : SubViews)
	{
		SubviewToAdvance->Advance(View.GetViewDelta());
	}

	ReserveEntityIdRetryHandler.ProcessOps(DeltaTimeS);
	CreateEntityRetryHandler.ProcessOps(DeltaTimeS);
	DeleteEntityRetryHandler.ProcessOps(DeltaTimeS);
	EntityQueryRetryHandler.ProcessOps(DeltaTimeS);
	EntityCommandRetryHandler.ProcessOps(DeltaTimeS);
}

const ViewDelta& ViewCoordinator::GetViewDelta() const
{
	return View.GetViewDelta();
}

const EntityView& ViewCoordinator::GetView() const
{
	return View.GetView();
}

void ViewCoordinator::FlushMessagesToSend()
{
	ConnectionHandler->SendMessages(View.FlushLocalChanges());
}

FSubView& ViewCoordinator::CreateSubView(Worker_ComponentId Tag, const FFilterPredicate& Filter,
										 const TArray<FDispatcherRefreshCallback>& DispatcherRefreshCallbacks)
{
	const int Index = SubViews.Emplace(MakeUnique<FSubView>(Tag, Filter, View.GetViewPtr(), Dispatcher, DispatcherRefreshCallbacks));
	return *SubViews[Index];
}

void ViewCoordinator::RefreshEntityCompleteness(Worker_EntityId EntityId)
{
	for (const TUniquePtr<FSubView>& SubviewToRefresh : SubViews)
	{
		SubviewToRefresh->RefreshEntity(EntityId);
	}
}

void ViewCoordinator::SendAddComponent(Worker_EntityId EntityId, ComponentData Data)
{
	View.SendAddComponent(EntityId, MoveTemp(Data));
}

void ViewCoordinator::SendComponentUpdate(Worker_EntityId EntityId, ComponentUpdate Update)
{
	View.SendComponentUpdate(EntityId, MoveTemp(Update));
}

void ViewCoordinator::SendRemoveComponent(Worker_EntityId EntityId, Worker_ComponentId ComponentId)
{
	View.SendRemoveComponent(EntityId, ComponentId);
}

Worker_RequestId ViewCoordinator::SendReserveEntityIdsRequest(uint32 NumberOfEntityIds, TOptional<uint32> TimeoutMillis)
{
	View.SendReserveEntityIdsRequest({ NextRequestId, NumberOfEntityIds, TimeoutMillis });
	return NextRequestId++;
}

Worker_RequestId ViewCoordinator::SendCreateEntityRequest(TArray<ComponentData> EntityComponents, TOptional<Worker_EntityId> EntityId,
														  TOptional<uint32> TimeoutMillis)
{
	View.SendCreateEntityRequest({ NextRequestId, MoveTemp(EntityComponents), EntityId, TimeoutMillis });
	return NextRequestId++;
}

Worker_RequestId ViewCoordinator::SendDeleteEntityRequest(Worker_EntityId EntityId, TOptional<uint32> TimeoutMillis)
{
	View.SendDeleteEntityRequest({ NextRequestId, EntityId, TimeoutMillis });
	return NextRequestId++;
}

Worker_RequestId ViewCoordinator::SendEntityQueryRequest(EntityQuery Query, TOptional<uint32> TimeoutMillis)
{
	View.SendEntityQueryRequest({ NextRequestId, MoveTemp(Query), TimeoutMillis });
	return NextRequestId++;
}

Worker_RequestId ViewCoordinator::SendEntityCommandRequest(Worker_EntityId EntityId, CommandRequest Request,
														   TOptional<uint32> TimeoutMillis)
{
	View.SendEntityCommandRequest({ EntityId, NextRequestId, MoveTemp(Request), TimeoutMillis });
	return NextRequestId++;
}

void ViewCoordinator::SendEntityCommandResponse(Worker_RequestId RequestId, CommandResponse Response)
{
	View.SendEntityCommandResponse({ RequestId, MoveTemp(Response) });
}

void ViewCoordinator::SendEntityCommandFailure(Worker_RequestId RequestId, FString Message)
{
	View.SendEntityCommandFailure({ RequestId, MoveTemp(Message) });
}

void ViewCoordinator::SendMetrics(SpatialMetrics Metrics)
{
	View.SendMetrics(MoveTemp(Metrics));
}

void ViewCoordinator::SendLogMessage(Worker_LogLevel Level, const FName& LoggerName, FString Message)
{
	View.SendLogMessage({ Level, LoggerName, MoveTemp(Message) });
}

Worker_RequestId ViewCoordinator::SendReserveEntityIdsRequest(uint32 NumberOfEntityIds, FRetryData RetryData)
{
	ReserveEntityIdRetryHandler.SendRequest(NextRequestId, NumberOfEntityIds, RetryData);
	return NextRequestId++;
}

Worker_RequestId ViewCoordinator::SendCreateEntityRequest(TArray<ComponentData> EntityComponents, TOptional<Worker_EntityId> EntityId,
														  FRetryData RetryData)
{
	CreateEntityRetryHandler.SendRequest(NextRequestId, { MoveTemp(EntityComponents), EntityId }, RetryData);
	return NextRequestId++;
}

Worker_RequestId ViewCoordinator::SendDeleteEntityRequest(Worker_EntityId EntityId, FRetryData RetryData)
{
	DeleteEntityRetryHandler.SendRequest(NextRequestId, EntityId, RetryData);
	return NextRequestId++;
}

Worker_RequestId ViewCoordinator::SendEntityQueryRequest(EntityQuery Query, FRetryData RetryData)
{
	EntityQueryRetryHandler.SendRequest(NextRequestId, MoveTemp(Query), RetryData);
	return NextRequestId++;
}

Worker_RequestId ViewCoordinator::SendEntityCommandRequest(Worker_EntityId EntityId, CommandRequest Request, FRetryData RetryData)
{
	EntityCommandRetryHandler.SendRequest(NextRequestId, { EntityId, MoveTemp(Request) }, RetryData);
	return NextRequestId++;
}

CallbackId ViewCoordinator::RegisterComponentAddedCallback(Worker_ComponentId ComponentId, FComponentValueCallback Callback)
{
	return Dispatcher.RegisterComponentAddedCallback(ComponentId, MoveTemp(Callback));
}

CallbackId ViewCoordinator::RegisterComponentRemovedCallback(Worker_ComponentId ComponentId, FComponentValueCallback Callback)
{
	return Dispatcher.RegisterComponentRemovedCallback(ComponentId, MoveTemp(Callback));
}

CallbackId ViewCoordinator::RegisterComponentValueCallback(Worker_ComponentId ComponentId, FComponentValueCallback Callback)
{
	return Dispatcher.RegisterComponentValueCallback(ComponentId, MoveTemp(Callback));
}

CallbackId ViewCoordinator::RegisterAuthorityGainedCallback(Worker_ComponentId ComponentId, FEntityCallback Callback)
{
	return Dispatcher.RegisterAuthorityGainedCallback(ComponentId, MoveTemp(Callback));
}

CallbackId ViewCoordinator::RegisterAuthorityLostCallback(Worker_ComponentId ComponentId, FEntityCallback Callback)
{
	return Dispatcher.RegisterAuthorityLostCallback(ComponentId, MoveTemp(Callback));
}

CallbackId ViewCoordinator::RegisterAuthorityLostTempCallback(Worker_ComponentId ComponentId, FEntityCallback Callback)
{
	return Dispatcher.RegisterAuthorityLostTempCallback(ComponentId, MoveTemp(Callback));
}

void ViewCoordinator::RemoveCallback(CallbackId Id)
{
	Dispatcher.RemoveCallback(Id);
}

FDispatcherRefreshCallback ViewCoordinator::CreateComponentExistenceRefreshCallback(
	Worker_ComponentId ComponentId, const FComponentChangeRefreshPredicate& RefreshPredicate)
{
	return FSubView::CreateComponentExistenceRefreshCallback(Dispatcher, ComponentId, RefreshPredicate);
}

FDispatcherRefreshCallback ViewCoordinator::CreateComponentChangedRefreshCallback(Worker_ComponentId ComponentId,
																				  const FComponentChangeRefreshPredicate& RefreshPredicate)
{
	return FSubView::CreateComponentChangedRefreshCallback(Dispatcher, ComponentId, RefreshPredicate);
}

FDispatcherRefreshCallback ViewCoordinator::CreateAuthorityChangeRefreshCallback(Worker_ComponentId ComponentId,
																				 const FAuthorityChangeRefreshPredicate& RefreshPredicate)
{
	return FSubView::CreateAuthorityChangeRefreshCallback(Dispatcher, ComponentId, RefreshPredicate);
}

const FString& ViewCoordinator::GetWorkerId() const
{
	return ConnectionHandler->GetWorkerId();
}

const TArray<FString>& ViewCoordinator::GetWorkerAttributes() const
{
	return ConnectionHandler->GetWorkerAttributes();
}

} // namespace SpatialGDK
