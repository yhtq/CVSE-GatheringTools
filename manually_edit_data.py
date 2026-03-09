import asyncio
import tqdm.asyncio
from bilibili_api import aid2bvid, bvid2aid
import capnp
from api_client import (
    Rank,
    CVSE_Client,
    RecordingNewEntry,
    ModifyEntry,
    RecordingNewEntry_to_capnp,
    ModifyEntry_to_capnp,
    asyncMapInBatch,
    Index,
    Index_to_capnp, RPCTime,
)

async def main():
    SERVER_HOST = "47.104.152.246"  # CVSE RPC服务端地址
    SERVER_PORT = "8663"  # CVSE RPC服务端端口

    # bvid = "BV1v6zDBvEem"
    # aid = bvid2aid(bvid)
    # avid = "av" + str(aid)

    aid = 116081070970152
    avid = "av" + str(aid)
    bvid = aid2bvid(aid)


    modify_entry = ModifyEntry(
        avid=avid,
        bvid=bvid,
        ranks=[Rank.SV],
        is_republish=None,
        staff=None,
        is_examined=True,
    )
    capnp_modify_entry = ModifyEntry_to_capnp(modify_entry)

    client = await CVSE_Client.create("47.104.152.246", "8663")
    await client.updateModifyEntry([capnp_modify_entry])
    print("Successfully updated video", bvid)
    return 0

if __name__ == '__main__':
    asyncio.run(capnp.run(main()))