

#pragma once

#include "SpatialCommonTypes.h"
#include "Utils/RPCRingBuffer.h"

#include "Containers/BitArray.h"

namespace SpatialGDK
{
namespace CrossServer
{
struct SentRPCEntry
{
	bool operator==(SentRPCEntry const& iRHS) const { return FMemory::Memcmp(this, &iRHS, sizeof(SentRPCEntry)) == 0; }

	uint64 RPCId;
	uint64 Timestamp;
	uint32 Slot;
	Worker_EntityId Target;
	TOptional<Worker_RequestId> EntityRequest;
};

typedef TPair<Worker_EntityId_Key, uint64> RPCKey;

struct SlotAlloc
{
	SlotAlloc()
	{
		Occupied.Init(false, RPCRingBufferUtils::GetRingBufferSize(ERPCType::CrossServerSender));
		ToClear.Init(false, RPCRingBufferUtils::GetRingBufferSize(ERPCType::CrossServerSender));
	}

	TBitArray<FDefaultBitArrayAllocator> Occupied;
	TBitArray<FDefaultBitArrayAllocator> ToClear;
};

struct SenderState
{
	TOptional<uint32_t> FindFreeSlot()
	{
		int32 freeSlot = Alloc.Occupied.Find(false);
		if (freeSlot >= 0)
		{
			return freeSlot;
		}

		return {};
	}

	TMap<RPCKey, SentRPCEntry> Mailbox;
	SlotAlloc Alloc;
};

struct ACKSlot
{
	bool operator==(const ACKSlot& Other) const { return Receiver == Other.Receiver && Slot == Other.Slot; }

	Worker_EntityId Receiver = 0;
	uint32 Slot;
};

struct RPCSlots
{
	ACKSlot ReceiverSlot;
	int32 SenderACKSlot = -1;
};

typedef TMap<RPCKey, RPCSlots> RPCAllocMap;
} // namespace CrossServer
} // namespace SpatialGDK
