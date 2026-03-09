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

    # bvid = "BV1dTzRBCEkg"
    # aid = bvid2aid(bvid)
    # avid = "av" + str(aid)
    aid = 116081070970152
    avid = "av" + str(aid)
    bvid = aid2bvid(aid)
    index = Index(avid=avid, bvid=bvid)
    index_capnp = Index_to_capnp(index)

    client = await CVSE_Client.create("47.104.152.246", "8663")
    res = await client.lookupMetaInfo([index_capnp])
    print(res)
    return 0

if __name__ == '__main__':
    asyncio.run(capnp.run(main()))