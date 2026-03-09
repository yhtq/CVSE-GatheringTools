import asyncio
from datetime import datetime
from math import floor
from typing import List, Dict, Any, Optional
from bilibili_api import aid2bvid, video
import capnp
import pandas as pd
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

SERVER_HOST = "47.104.152.246"  # CVSE RPC服务端地址
SERVER_PORT = "8663"            # CVSE RPC服务端端口
BATCH_SIZE = 256               # 推送批次大小（≤4096）
LOOKUP_TIMEOUT = 5
XLSX_FILE_PATH = "WeeklyData/260228主副榜校审.xlsx"  # 你的XLSX文件路径
SHEET_NAME = "Sheet1"           # 表格sheet名称
examined = True

def read_xlsx_data(file_path: str, sheet_name: str = None) -> pd.DataFrame:
    """读取并清洗XLSX数据（按aid去重）"""
    try:
        df = pd.read_excel(file_path, sheet_name=sheet_name, engine="openpyxl")
        print(f"成功读取XLSX文件，共 {len(df)} 行数据")

        df = df[df["aid"].notna() & (df["aid"] != "")]
        df = df.drop_duplicates(subset=["aid"], keep="last")
        print(f"数据清洗后剩余 {len(df)} 行有效数据")
        return df
    except Exception as e:
        print(f"读取XLSX文件失败：{str(e)}")
        raise


def get_ranks(data_row) -> List[Rank]:
    if pd.isna(data_row["synthesizer"]) or len(str(data_row["synthesizer"])) == 0:
        ranks = []
    else:
        ranks = [int(x) for x in str(data_row["synthesizer"]).split("、")]
        ranks = [Rank.SV if x == 1 else (Rank.DOMESTIC if x == 2 else Rank.UTAU) for x in ranks]
    return ranks



async def process_batch(client: CVSE_Client, data_rows: List[Dict[str, Any]]):
    global examined
    try:
        listexist = []
        listnew = []
        listskipped = []
        row = "pre"
        # 预处理是否已经全部入库

        indexes = [Index_to_capnp(Index(avid="av" + str(data_row["aid"]),
                                        bvid=aid2bvid(int(data_row["aid"]))
                                        ))
                   for data_row in data_rows]
        result = await asyncio.wait_for(client.lookupMetaInfo(indexes), timeout=LOOKUP_TIMEOUT)

        listExistAvid = [entry.avid for entry in result]


        for row in data_rows:

            already_exist = str(row['aid']) in listExistAvid or ("av" + str(row['aid'])) in listExistAvid

            if already_exist is None:
                listskipped.append(row['aid'])
            elif already_exist:
                modify_entry = ModifyEntry(
                    avid="av" + str(row['aid']),
                    bvid=str(row['bvid']),
                    ranks=get_ranks(row),
                    is_republish=row['is_republish'] if pd.notna(row['is_republish']) else False,
                    staff=None,
                    is_examined=examined,
                )
                capnp_modify_entry = ModifyEntry_to_capnp(modify_entry)
                listexist.append(capnp_modify_entry)
            else:
                if "pubdate" in row.keys():
                    pubdate_str = row["pubdate"]
                    # convert to RPCTime: seconds and nanoseconds
                    if isinstance(pubdate_str, float):
                        if pd.isna(pubdate_str):
                            pt_rpc = RPCTime.minValue()
                        else:
                            pt_sec = pubdate_str*86400
                            pt_rpc = RPCTime(floor(pt_sec), int((pt_sec - floor(pt_sec))*1e9))
                    else:
                        try:
                            pt = datetime.strptime(pubdate_str, "%Y-%m-%d %H:%M:%S")
                            pt_rpc = RPCTime.from_datetime(pt)
                        except ValueError:
                            pt_rpc = RPCTime.minValue()
                else:
                    pt_rpc = RPCTime.minValue()
                check = lambda entry: "" if pd.isna(entry) else entry
                new_entry = RecordingNewEntry(
                    avid="av" + str(row['aid']),
                    bvid=str(row['bvid']),
                    title=check(str(row['title'])),
                    desc=check(str(row['intro'])),
                    tags=[],
                    cover=check(row["image_url"]),
                    duration=0, #int(row["duration"]),
                    uploader=check(str(row["uploader"])),
                    up_face=check(str(row["up_face"])),
                    copyright=row['copyright'] if pd.notna(row['copyright']) else 1,
                    pubdate=pt_rpc,
                    page=int(row["page"]),
                    is_examined=examined,
                    ranks=get_ranks(row),
                    is_republish=row['is_republish'] if pd.notna(row['is_republish']) else False,
                    staff_info="",
                )
                capnp_new_entry = RecordingNewEntry_to_capnp(new_entry)
                listnew.append(capnp_new_entry)
                await client.reconnect()

        task1 = asyncio.create_task(client.updateModifyEntry(listexist))
        task2 = asyncio.create_task(client.updateNewEntry(listnew))
        await asyncio.gather(task1, task2)
        return len(listexist), len(listnew), listskipped
    except Exception as e:
        print(row)
        raise

async def main():

    df = read_xlsx_data(XLSX_FILE_PATH, SHEET_NAME)
    data_rows = df.to_dict("records")
    n = 0
    total = len(data_rows) // BATCH_SIZE + 1
    client = await CVSE_Client.create("47.104.152.246", "8663")
    skipped = []
    async for entry in asyncMapInBatch(
        lambda x: process_batch(client, x),
        data_rows,
        batch_size=BATCH_SIZE,
    ):
        skipped += entry[2]
        n += 1
        print(f"batch {n}/{total}: {entry[0]} old, {entry[1]} new")
    print('all skipped videos:', skipped)


if __name__ == '__main__':
    asyncio.run(capnp.run(main()))