// Copyright (c) Improbable Worlds Ltd, All Rights Reserved

#pragma once

#include "SpatialView/CommandRetryHandler.h"

#include "Algo/Transform.h"
#include "Misc/Optional.h"

namespace SpatialGDK
{
struct FDeleteEntityRetryHandlerImpl
{
	using CommandData = Worker_EntityId;

	static bool CanHandleOp(const Worker_Op& Op) { return Op.op_type == WORKER_OP_TYPE_DELETE_ENTITY_RESPONSE; }

	static Worker_RequestId GetRequestId(const Worker_Op& Op)
	{
		check(Op.op_type == WORKER_OP_TYPE_DELETE_ENTITY_RESPONSE);
		return Op.op.delete_entity_response.request_id;
	}

	static bool CanRetry(const Worker_Op& Op, FRetryData& RetryData)
	{
		RetryData = RetryData.Advance();
		return Op.op.delete_entity_response.status_code == WORKER_STATUS_CODE_TIMEOUT;
	}

	static void SendCommandRequest(Worker_RequestId RequestId, Worker_EntityId EntityId, uint32 TimeoutMillis, WorkerView& View)
	{
		View.SendDeleteEntityRequest(DeleteEntityRequest{ RequestId, EntityId, TimeoutMillis });
	}
};

struct FCreateEntityRetryHandlerImpl
{
	struct CommandData
	{
		TArray<ComponentData> Components;
		TOptional<Worker_EntityId> EntityId;
	};

	static bool CanHandleOp(const Worker_Op& Op) { return Op.op_type == WORKER_OP_TYPE_CREATE_ENTITY_RESPONSE; }

	static Worker_RequestId GetRequestId(const Worker_Op& Op)
	{
		check(Op.op_type == WORKER_OP_TYPE_CREATE_ENTITY_RESPONSE);
		return Op.op.create_entity_response.request_id;
	}

	static bool CanRetry(const Worker_Op& Op, FRetryData& RetryData)
	{
		RetryData = RetryData.Advance();
		return Op.op.create_entity_response.status_code == WORKER_STATUS_CODE_TIMEOUT;
	}

	static void SendCommandRequest(Worker_RequestId RequestId, const CommandData& Data, uint32 TimeoutMillis, WorkerView& View)
	{
		TArray<ComponentData> ComponentsCopy;
		ComponentsCopy.Reserve(Data.Components.Num());
		Algo::Transform(Data.Components, ComponentsCopy, [](const ComponentData& Component) {
			return Component.DeepCopy();
		});

		View.SendCreateEntityRequest(CreateEntityRequest{ RequestId, MoveTemp(ComponentsCopy), Data.EntityId, TimeoutMillis });
	}

	static void SendCommandRequest(Worker_RequestId RequestId, CommandData&& Data, uint32 TimeoutMillis, WorkerView& View)
	{
		View.SendCreateEntityRequest(CreateEntityRequest{ RequestId, MoveTemp(Data.Components), Data.EntityId, TimeoutMillis });
	}
};

struct FReserveEntityIdsRetryHandlerImpl
{
	using CommandData = uint32;

	static bool CanHandleOp(const Worker_Op& Op) { return Op.op_type == WORKER_OP_TYPE_RESERVE_ENTITY_IDS_RESPONSE; }

	static Worker_RequestId GetRequestId(const Worker_Op& Op)
	{
		check(Op.op_type == WORKER_OP_TYPE_RESERVE_ENTITY_IDS_RESPONSE);
		return Op.op.reserve_entity_ids_response.request_id;
	}

	static bool CanRetry(const Worker_Op& Op, FRetryData& RetryData)
	{
		RetryData = RetryData.Advance();
		return Op.op.reserve_entity_ids_response.status_code == WORKER_STATUS_CODE_TIMEOUT;
	}

	static void SendCommandRequest(Worker_RequestId RequestId, CommandData NumberOfIds, uint32 TimeoutMillis, WorkerView& View)
	{
		View.SendReserveEntityIdsRequest(ReserveEntityIdsRequest{ RequestId, NumberOfIds, TimeoutMillis });
	}
};

struct FEntityQueryRetryHandlerImpl
{
	using CommandData = EntityQuery;

	static bool CanHandleOp(const Worker_Op& Op) { return Op.op_type == WORKER_OP_TYPE_ENTITY_QUERY_RESPONSE; }

	static Worker_RequestId GetRequestId(const Worker_Op& Op)
	{
		check(Op.op_type == WORKER_OP_TYPE_ENTITY_QUERY_RESPONSE);
		return Op.op.entity_query_response.request_id;
	}

	static bool CanRetry(const Worker_Op& Op, FRetryData& RetryData)
	{
		RetryData = RetryData.Advance();
		return Op.op.entity_query_response.status_code == WORKER_STATUS_CODE_TIMEOUT;
	}

	static void SendCommandRequest(Worker_RequestId RequestId, const CommandData& Query, uint32 TimeoutMillis, WorkerView& View)
	{
		View.SendEntityQueryRequest(EntityQueryRequest{ RequestId, EntityQuery(Query.GetWorkerQuery()), TimeoutMillis });
	}

	static void SendCommandRequest(Worker_RequestId RequestId, CommandData&& Query, uint32 TimeoutMillis, WorkerView& View)
	{
		View.SendEntityQueryRequest(EntityQueryRequest{ RequestId, MoveTemp(Query), TimeoutMillis });
	}
};

struct FEntityCommandRetryHandlerImpl
{
	struct CommandData
	{
		Worker_EntityId EntityId;
		CommandRequest Request;
	};

	static bool CanHandleOp(const Worker_Op& Op) { return Op.op_type == WORKER_OP_TYPE_COMMAND_RESPONSE; }

	static Worker_RequestId GetRequestId(const Worker_Op& Op)
	{
		check(Op.op_type == WORKER_OP_TYPE_COMMAND_RESPONSE);
		return Op.op.command_response.request_id;
	}

	static bool CanRetry(const Worker_Op& Op, FRetryData& RetryData)
	{
		switch (static_cast<Worker_StatusCode>(Op.op.command_response.status_code))
		{
		case WORKER_STATUS_CODE_TIMEOUT:
			RetryData = RetryData.Advance();
			return true;
		case WORKER_STATUS_CODE_AUTHORITY_LOST:
			// Don't increase retry timer or reduce the number of retires.
			return true;
		default:
			return false;
		}
	}

	static void SendCommandRequest(Worker_RequestId RequestId, const CommandData& Query, uint32 TimeoutMillis, WorkerView& View)
	{
		View.SendEntityCommandRequest(EntityCommandRequest{ Query.EntityId, RequestId, Query.Request.DeepCopy(), TimeoutMillis });
	}

	static void SendCommandRequest(Worker_RequestId RequestId, CommandData&& Query, uint32 TimeoutMillis, WorkerView& View)
	{
		View.SendEntityCommandRequest(EntityCommandRequest{ Query.EntityId, RequestId, MoveTemp(Query.Request), TimeoutMillis });
	}
};
} // namespace SpatialGDK
