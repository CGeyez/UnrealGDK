// Copyright (c) Improbable Worlds Ltd, All Rights Reserved

#pragma once

#include "CoreMinimal.h"

#include "Misc/DateTime.h"
#include "SpatialView/EntityComponentId.h"

DECLARE_LOG_CATEGORY_EXTERN(LogSpatialSpanIdStore, Log, All);

namespace SpatialGDK
{
class SpatialSpanIdCache
{
public:
	void AddSpanId(const EntityComponentId& Id, const uint32 FieldId, worker::c::Trace_SpanId SpanId);
	bool DropSpanId(const EntityComponentId& Id, const uint32 FieldId);
	bool DropSpanIds(const EntityComponentId& Id);
	void ClearSpanIds();

	worker::c::Trace_SpanId GetSpanId(const EntityComponentId& Id, const uint32 FieldId) const;
	worker::c::Trace_SpanId GetMostRecentSpanId(const EntityComponentId& Id) const;

protected:
	struct EntityComponentFieldId
	{
		EntityComponentId EntityComponentId;
		uint32 FieldId;
	};

	struct EntityComponentFieldIdUpdateSpanId
	{
		worker::c::Trace_SpanId SpanId = worker::c::Trace_SpanId();
		FDateTime UpdateTime;
	};

	using FieldIdMap = TMap<uint32, EntityComponentFieldIdUpdateSpanId>;
	TMap<EntityComponentId, FieldIdMap> EntityComponentFieldSpanIds;

private:
	bool DropSpanIdInternal(FieldIdMap* SpanIdMap, const EntityComponentId& Id, const uint32 FieldId);
};

class SpatialTimedSpanIdCache : public SpatialSpanIdCache
{
public:
	SpatialTimedSpanIdCache();
	SpatialTimedSpanIdCache(float InDropFrequency, float InMinSpanIdLifetime, int32 InMaxSpanIdsToDrop);

	void DropOldSpanIds();

private:
	// Private Members

	const float DropFrequency = 5.0f;
	const float MinSpanIdLifetime = 10.0f;
	const int32 MaxSpanIdsToDrop = 1000;
	FDateTime NextClearTime;

	void UpdateNextClearTime();
};

class SpatialWorkerOpSpanIdCache : public SpatialSpanIdCache
{
public:
	struct FieldSpanIdUpdate
	{
		uint32 FieldId;
		worker::c::Trace_SpanId NewSpanId;
		worker::c::Trace_SpanId OldSpanId;
	};

	void ComponentAdd(const Worker_Op& Op);
	bool ComponentRemove(const Worker_Op& Op);

	// Returns a list of the field ids that already existed in the store
	TArray<FieldSpanIdUpdate> ComponentUpdate(const Worker_Op& Op);
};

} // namespace SpatialGDK