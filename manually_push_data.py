import asyncio
import tqdm.asyncio
from bilibili_api import aid2bvid, bvid2aid, video
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
    aid = 116080852928247
    avid = "av" + str(aid)
    bvid = aid2bvid(aid)
    video_rank = [Rank.SV]

    video_entry = video.Video(bvid)
    video_data = await video_entry.get_info()
    # print(video_data)

    new_entry: RecordingNewEntry = {
            "avid": "av" + str(video_data['aid']),
            "bvid": video_data['bvid'],
            "title": video_data['title'],
            "desc": video_data['desc'],
            "tags": [],
            "cover": video_data['pic'],
            "duration": video_data['duration'],
            "uploader": video_data['owner']['name'],
            "up_face": video_data['owner']['face'],
            "copyright": video_data['copyright'],
            "pubdate": RPCTime(video_data['pubdate'], 0),
            "page": 1,
            "is_examined": True,
            "ranks": video_rank,
            "is_republish": video_data['copyright'] == 2,
            "staff_info": "",
    }
    print(new_entry)
    new_entry_capnp = RecordingNewEntry_to_capnp(new_entry)

    client = await CVSE_Client.create("47.104.152.246", "8663")
    await client.updateNewEntry([new_entry_capnp])
    print('Successfully created data')
    return 0

if __name__ == '__main__':
    asyncio.run(capnp.run(main()))