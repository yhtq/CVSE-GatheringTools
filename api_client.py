# CVSE 数据 RPC 服务客户端接口
# 使用方法参考 test() 函数，但不要运行（否则会向数据库中加入无用数据）
# 所有函数的参数和返回值都使用序列化格式
# （也就是类型标注中的 CVSE_capnp.Cvse.xxx）
# 内存效率比 Python 原生格式更高，因此建议在爬虫运行过程中也使用该格式记录数据
# 具体 field 参考 CVSE-API/CVSE.capnp，或者本文件中 TypedDict 实现
# 服务器进程在 ps -aux | grep CVSE-exe
# 数据库样式可以使用 mongosh 查看，数据库名为 cvse_db
# RPC 服务未限制调用端，该文件应当在远程网络也可执行
import asyncio
import enum
import os
import sys
from datetime import datetime, timedelta
from typing import AsyncGenerator, Iterable, NoReturn, Protocol, Sequence, TypedDict, TypeVar, Callable, Awaitable
import socket

import capnp

current_script_path = os.path.abspath(__file__)
auth_key_path = os.path.join(os.path.dirname(current_script_path), "auth_key")
subdir = "CVSE-API"
sys.path.append(os.path.join(os.path.dirname(current_script_path), subdir))
import CVSE_capnp

T = TypeVar("T", covariant=True)
T1 = TypeVar("T1")
T2 = TypeVar("T2", covariant=True)


class RankProtocol(Protocol[T]):
    value: str


class Rank(enum.Enum):
    DOMESTIC = 1
    SV = 2
    UTAU = 3


class RPCTime:
    def __init__(self, seconds: int, nanoseconds: int) -> None:
        self.seconds = seconds
        self.nanoseconds = nanoseconds

    @staticmethod
    def from_datetime(dt: datetime) -> "RPCTime":
        return RPCTime(int(dt.timestamp()), dt.microsecond * 1000)

    @staticmethod
    def minValue() -> "RPCTime":
        return RPCTime(0, 0)

    @staticmethod
    def now() -> "RPCTime":
        return RPCTime.from_datetime(datetime.now())

    def to_datetime(self) -> datetime:
        return datetime.fromtimestamp(self.seconds + self.nanoseconds / 1e9)


class RPCTimeProtocol(Protocol[T]):
    seconds: int
    nanoseconds: int


class ModifyEntry(TypedDict):
    avid: str
    bvid: str
    ranks: list[Rank] | None
    is_republish: bool | None
    staff: str | None
    is_examined: bool | None


class ModifyEntryProtocol(Protocol[T]):
    avid: str
    bvid: str
    ranks: list[Rank]
    hasRanks: bool
    isRepublish: bool
    hasIsRepublish: bool
    staffInfo: str
    hasStaffInfo: bool
    is_examined: bool
    hasIsExamined: bool


class RecordingNewEntry(TypedDict):
    avid: str
    bvid: str
    title: str
    uploader: str
    up_face: str
    copyright: int
    pubdate: RPCTime
    duration: int
    page: int
    cover: str
    desc: str
    tags: list[str]
    is_examined: bool
    ranks: list[Rank]
    is_republish: bool
    staff_info: str


class RecordingNewEntryProtocol(Protocol[T, T1]):
    avid: str
    bvid: str
    title: str
    uploader: str
    upFace: str
    copyright: int
    pubdate: RPCTimeProtocol[T1]
    duration: int
    page: int
    cover: str
    desc: str
    tags: list[str]
    isExamined: bool
    ranks: list[RankProtocol[T1]]
    isRepublish: bool
    staffInfo: str


class RecordingDataEntry(TypedDict):
    avid: str
    bvid: str
    view: int
    favorite: int
    coin: int
    like: int
    danmaku: int
    reply: int
    share: int
    date: RPCTime


class RecordingDataEntryProtocol(Protocol[T, T1]):
    avid: str
    bvid: str
    view: int
    favorite: int
    coin: int
    like: int
    danmaku: int
    reply: int
    share: int
    date: RPCTimeProtocol[T1]

class RankingInfoEntryProtocol(Protocol[T1, T2]):
    avid: str
    bvid: str
    prev: RecordingDataEntryProtocol[T1, T2]
    curr: RecordingDataEntryProtocol[T1, T2]
    isNew: bool
    view: int
    like: int
    share: int
    favorite: int
    coin: int
    reply: int
    danmaku: int
    pointA: float
    pointB: float
    pointC: float
    fixA: float
    fixB: float
    fixC: float
    scoreA: float
    scoreB: float
    scoreC: float
    totalScore: float
    rank: int
    specialRank: str
    rankPosition: str
    onMainCountInTenWeeks: int

special_rank_normal = "normal"
special_rank_sh = "sh"
special_rank_hot = "hot"

rank_position_main = "main"
rank_position_side = "side"
rank_position_none = "none"

class RankingMetaInfoStatProtocol(Protocol):
    count: int
    totalView: int
    totalLike: int
    totalCoin: int
    totalFavorite: int
    totalShare: int
    totalReply: int
    totalDanmaku: int
    totalNew: int
    startTime: RPCTimeProtocol
    endTime: RPCTimeProtocol

class Index(TypedDict):
    avid: str
    bvid: str


class IndexProtocol(Protocol[T]):
    avid: str
    bvid: str

XOR_CODE = 23442827791579
MASK_CODE = 2251799813685247
MAX_AID = 1 << 51
ALPHABET = "FcwAPNKTMug3GV5Lj7EJnHpWsx4tb8haYeviqBz6rkCy12mUSDQX9RdoZf"
ENCODE_MAP = 8, 7, 0, 5, 1, 3, 2, 4, 6
DECODE_MAP = tuple(reversed(ENCODE_MAP))

BASE = len(ALPHABET)
PREFIX = "BV1"
PREFIX_LEN = len(PREFIX)
CODE_LEN = len(ENCODE_MAP)


def av2bv(aid: int) -> str:
    bvid = [""] * 9
    tmp = (MAX_AID | aid) ^ XOR_CODE
    for i in range(CODE_LEN):
        bvid[ENCODE_MAP[i]] = ALPHABET[tmp % BASE]
        tmp //= BASE
    return PREFIX + "".join(bvid)


def bv2av(bvid: str) -> int:
    assert bvid[:3] == PREFIX

    bvid = bvid[3:]
    tmp = 0
    for i in range(CODE_LEN):
        idx = ALPHABET.index(bvid[DECODE_MAP[i]])
        tmp = tmp * BASE + idx
    return (tmp & MASK_CODE) ^ XOR_CODE

def bv_to_index(bvid: str) -> Index:
    return Index(avid=f"av{bv2av(bvid)}", bvid=bvid)

def av_to_index(avid: str) -> Index:
    assert avid.startswith("av")
    aid = int(avid[2:])
    return Index(avid=avid, bvid=av2bv(aid))

def build_list_to_capnp(l: Iterable[T], builder) -> None:
    capnp_list = builder
    for i, item in enumerate(l):
        capnp_list[i] = item
    return capnp_list


def RPCTime_to_capnp(obj: RPCTime) -> RPCTimeProtocol[T]:
    time = CVSE_capnp.Cvse.Time.new_message()
    time.seconds = obj.seconds
    time.nanoseconds = obj.nanoseconds
    return time


def capnp_to_RPCTime(obj: RPCTimeProtocol[T]) -> RPCTime:
    return RPCTime(obj.seconds, obj.nanoseconds)


def datetime_to_capnp(obj: datetime) -> RPCTimeProtocol[T]:
    return RPCTime_to_capnp(RPCTime.from_datetime(obj))


def Rank_to_capnp(obj: Rank) -> RankProtocol[T]:
    rank = CVSE_capnp.Cvse.Rank.new_message()
    match obj:
        case Rank.DOMESTIC:
            rank.value = "domestic"
        case Rank.SV:
            rank.value = "sv"
        case Rank.UTAU:
            rank.value = "utau"
    return rank


def capnp_to_Rank(obj: RankProtocol[T]) -> Rank:
    match obj.value:
        case "domestic":
            return Rank.DOMESTIC
        case "sv":
            return Rank.SV
        case "utau":
            return Rank.UTAU
    raise ValueError(f"Unknown rank value: {obj.value}")


def ModifyEntry_to_capnp(obj: ModifyEntry) -> ModifyEntryProtocol[T]:
    entry = CVSE_capnp.Cvse.ModifyEntry.new_message()
    entry.avid = obj["avid"]
    entry.bvid = obj["bvid"]
    entry.hasRanks = obj["ranks"] is not None
    entry.hasStaffInfo = obj["staff"] is not None
    entry.hasIsExamined = obj["is_examined"] is not None
    entry.hasIsRepublish = obj["is_republish"] is not None
    if obj["ranks"] is not None:
        ranks = map(Rank_to_capnp, obj["ranks"])
        build_list_to_capnp(ranks, entry.init("ranks", len(obj["ranks"])))
    else:
        entry.init("ranks", 0)
    if obj["is_republish"] is not None:
        entry.isRepublish = obj["is_republish"]
    else:
        entry.isRepublish = False
    if obj["staff"] is not None:
        entry.staffInfo = obj["staff"]
    else:
        entry.staffInfo = ""
    if obj["is_examined"] is not None:
        entry.isExamined = obj["is_examined"]
    else:
        entry.isExamined = False
    return entry



async def asyncMapInBatch(
    f: Callable[[list[T1]], Awaitable[T2]],
    inputs: Sequence[T1],
    batch_size: int = 4096
) -> AsyncGenerator[T2, NoReturn]:
    inputs = list(inputs)
    total: int = len(inputs)
    index: int = 0
    while index < total:
        result = await f(list(inputs[index: index + batch_size]))
        yield result
        index += batch_size



def RecordingNewEntry_to_capnp(
    obj: RecordingNewEntry,
) -> RecordingNewEntryProtocol[T, T1]:
    entry = CVSE_capnp.Cvse.RecordingNewEntry.new_message()
    entry.avid = obj["avid"]
    entry.bvid = obj["bvid"]
    entry.title = obj["title"]
    entry.uploader = obj["uploader"]
    entry.upFace = obj["up_face"]
    entry.copyright = obj["copyright"]
    entry.pubdate = RPCTime_to_capnp(obj["pubdate"])
    entry.duration = obj["duration"]
    entry.page = obj["page"]
    entry.cover = obj["cover"]
    entry.desc = obj["desc"]
    build_list_to_capnp(obj["tags"], entry.init("tags", len(obj["tags"])))
    entry.staffInfo = obj["staff_info"]
    ranks = map(Rank_to_capnp, obj["ranks"])
    build_list_to_capnp(ranks, entry.init("ranks", len(obj["ranks"])))
    entry.isExamined = obj["is_examined"]
    entry.isRepublish = obj["is_republish"]
    return entry


def RecordingDataEntry_to_capnp(
    obj: RecordingDataEntry,
) -> RecordingDataEntryProtocol[T, T1]:
    entry = CVSE_capnp.Cvse.RecordingDataEntry.new_message()
    entry.avid = obj["avid"]
    entry.bvid = obj["bvid"]
    entry.view = obj["view"]
    entry.favorite = obj["favorite"]
    entry.coin = obj["coin"]
    entry.like = obj["like"]
    entry.danmaku = obj["danmaku"]
    entry.reply = obj["reply"]
    entry.share = obj["share"]
    entry.date = RPCTime_to_capnp(obj["date"])
    return entry


def Index_to_capnp(obj: Index) -> IndexProtocol[T]:
    index = CVSE_capnp.Cvse.Index.new_message()
    index.avid = obj["avid"]
    index.bvid = obj["bvid"]
    return index


class CVSE_Client:
    def __init__(self, host, port, connection, client, cvse, auth_key=None):
        self.host = host
        self.port = port
        self.connection = connection
        self.client = client
        self.cvse = cvse
        if auth_key:
            self.auth_key = auth_key
        elif os.path.exists(auth_key_path):
            with open(auth_key_path, "r") as f:
                self.auth_key = f.read().strip()
        else:
            self.auth_key = None

    @staticmethod
    async def create(host, port, auth_key=None) -> "CVSE_Client":
        # connection = await capnp.AsyncIoStream.create_connection(host=host, port=port)
        # sock = connection.transport.get_extra_info("socket")
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        await asyncio.get_event_loop().sock_connect(sock, (host, int(port)))
        connection = await capnp.AsyncIoStream.create_connection(sock=sock)
        try:
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
            sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPIDLE, 60)
            sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPINTVL, 5)
            sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPCNT, 3)
        except AttributeError:
            pass
        client = capnp.TwoPartyClient(connection)
        cvse = client.bootstrap().cast_as(CVSE_capnp.Cvse)
        self = CVSE_Client(host, port, connection, client, cvse, auth_key)
        return self

    async def reconnect(self):
        try:
            self.connection = await capnp.AsyncIoStream.create_connection(host=self.host, port=self.port)
            self.client = capnp.TwoPartyClient(self.connection)
            self.cvse = self.client.bootstrap().cast_as(CVSE_capnp.Cvse)
        except Exception as e:
            print(f"Failed to reconnect: {e}")

    # 所有函数的参数和返回值都使用序列化格式
    # （也就是类型标注中的 xxxxxProtocol）
    # 内存效率比 Python 原生格式更高
    # 具体 field 参考 Protocol 定义即可
    # 由于暂时没搞明白的原因，一次性请求过多的 entry （20000左右？）可能会报错
    # 建议使用之前的 asyncMapInBatch 方法分批处理

    async def updateModifyEntry(self, entries: Sequence[ModifyEntryProtocol[T]]) -> None:
        size = len(entries)
        request = self.cvse.updateModifyEntry_request()
        build_list_to_capnp(entries, request.init("entries", size))
        assert self.auth_key is not None, "Auth key is required for updateModifyEntry"
        request.auth_token = self.auth_key
        await request.send()

    # 注意我们以 bvid 作为数据库中的唯一 id
    # 因此相同 bvid 的项只会被插入一次，之后不会再插入（不影响其他项）
    # 如果发生了重复插入，该函数会抛出异常，但其余的项仍然被正常插入
    async def updateNewEntry(
        self,
        entries: Sequence[RecordingNewEntryProtocol[T, T1]],
        replace: bool = False
    ) -> None:
        size = len(entries)
        request = self.cvse.updateNewEntry_request()
        build_list_to_capnp(entries, request.init("entries", size))
        request.replace = replace
        assert self.auth_key is not None, "Auth key is required for updateNewEntry"
        request.auth_token = self.auth_key
        await request.send()

    async def updateRecordingDataEntry(
        self, entries: Sequence[RecordingDataEntryProtocol[T, T1]]
    ) -> None:
        size = len(entries)
        request = self.cvse.updateRecordingDataEntry_request()
        build_list_to_capnp(entries, request.init("entries", size))
        assert self.auth_key is not None, "Auth key is required for updateRecordingDataEntry"
        request.auth_token = self.auth_key
        await request.send()

    async def getAll(
        self,
        get_unexamined: bool,
        get_unincluded: bool,
        from_date: RPCTime,
        to_date: RPCTime,
    ) -> list[IndexProtocol[T]]:
        """
        获取 pubdate 处于 [from_date, to_date) 的项的索引
        若 get_unexamined 为 True，则返回值包含未经过收录的索引
        若 get_unincluded 为 True，则返回值包含未收录在任何刊的索引
        """
        request = self.cvse.getAll_request()
        request.get_unexamined = get_unexamined
        request.get_unincluded = get_unincluded
        request.from_date = RPCTime_to_capnp(from_date)
        request.to_date = RPCTime_to_capnp(to_date)
        response = await request.send()
        return response.indices

    async def getAllInBatch(
        self,
        get_unexamined: bool,
        get_unincluded: bool,
        duration: timedelta,
        unbatch_at: datetime = datetime.strptime("2009-01-01", "%Y-%m-%d"),
    ) -> AsyncGenerator[list[IndexProtocol[T]], NoReturn]:
        """
        获取所有索引，分批获取，每批的时间间隔为 duration。
        迭代 end_time < unbatch_at 时，剩下的索引合并成一批。
        返回值为一个异步生成器，生成每一批的索引列表。
        """
        end_time = datetime.now()
        start_time = end_time - duration
        while end_time >= unbatch_at:
            indices = await self.getAll(
                get_unexamined,
                get_unincluded,
                RPCTime.from_datetime(start_time),
                RPCTime.from_datetime(end_time),
            )
            yield indices
            end_time = start_time
            start_time -= duration
        last_batch_indices = await self.getAll(
            get_unexamined,
            get_unincluded,
            RPCTime.minValue(),
            RPCTime.from_datetime(end_time),
        )
        yield last_batch_indices
        raise StopAsyncIteration

    async def lookupMetaInfo(
        self,
        indices: list[IndexProtocol[T]],
    ) -> Sequence[RecordingNewEntryProtocol[T, T1]]:
        request = self.cvse.lookupMetaInfo_request()
        build_list_to_capnp(indices, request.init("indices", len(indices)))
        response = await request.send()
        return response.entries

    async def lookupDataInfo(
        self,
        indices: list[IndexProtocol[T]],
        from_date: RPCTime,
        to_date: RPCTime,
    ) -> list[list[RecordingDataEntryProtocol[T, T1]]]:
        request = self.cvse.lookupDataInfo_request()
        build_list_to_capnp(indices, request.init("indices", len(indices)))
        request.from_date = RPCTime_to_capnp(from_date)
        request.to_date = RPCTime_to_capnp(to_date)
        response = await request.send()
        return response.entries

    async def lookupOneDataInfo(
        self,
        indices: list[CVSE_capnp.Cvse.Index],
        from_date: RPCTime,
        to_date: RPCTime,
    ) -> Sequence[RecordingDataEntryProtocol[T, T1]]:
        request = self.cvse.lookupOneDataInfo_request()
        build_list_to_capnp(indices, request.init("indices", len(indices)))
        request.from_date = RPCTime_to_capnp(from_date)
        request.to_date = RPCTime_to_capnp(to_date)
        response = await request.send()
        return response.entries

    # 重新对某期排行榜计算排名信息
    # 自动排除 is_examined 为 True 以及 in 该 rank 为 False 的视频
    # 如果 contain_unexamined 为 True，则也包含 is_examined 为 False 并被判定为该 rank 的视频
    # 否则，只包含 is_examined 为 True 的视频
    # 注意 include_unexamined 参数不同的计算结果不会互相覆盖
    # 如果 lock 为 True，则在计算完成后锁定该期排行榜
    # 锁定后无法再次计算，再次调用该函数会报错
    # 全部重新计算开销比较大（需要运行大约半分钟），不要过于频繁的调用
    async def reCalculateRankings(
        self,
        rank: Rank,
        index: int,
        contain_unexamined: bool,
        lock: bool,
    ) -> None:
        request = self.cvse.reCalculateRankings_request()
        request.rank = Rank_to_capnp(rank)
        request.index = index
        request.contain_unexamined = contain_unexamined
        request.lock = lock
        assert self.auth_key is not None, "Auth key is required for reCalculateRankings"
        request.auth_token = self.auth_key
        await request.send()

    # 得到参数完全相同的，上一个接口计算的信息
    # 涵盖排名 [from_rank, to_rank)
    # 若尚未计算，则会返回空列表
    async def getAllRankingInfo(
        self,
        rank: Rank,
        index: int,
        contain_unexamined: bool,
        from_rank: int,
        to_rank: int,
    ) -> Sequence[IndexProtocol[T]]:
        request = self.cvse.getAllRankingInfo_request()
        request.rank = Rank_to_capnp(rank)
        request.index = index
        request.contain_unexamined = contain_unexamined
        request.from_rank = from_rank
        request.to_rank = to_rank
        response = await request.send()
        return response.entries

    # 得到参数完全相同的，上一个接口计算的信息中，排名 [from_rank, to_rank) 的详细信息
    # 注意，不保证每个 index 都找到结果。如果未找到，则跳过对应项
    # 如果未计算，则返回空列表
    async def lookupRankingInfo(
        self,
        rank: Rank,
        index: int,
        contain_unexamined: bool,
        indices: list[IndexProtocol[T]]
    ) -> Sequence[RankingInfoEntryProtocol[T1, T2]]:
        request = self.cvse.lookupRankingInfo_request()
        request.rank = Rank_to_capnp(rank)
        request.index = index
        request.contain_unexamined = contain_unexamined
        build_list_to_capnp(indices, request.init("indices", len(indices)))
        response = await request.send()
        return response.entries

    async def lookupRankingMetaInfo(
        self,
        rank: Rank,
        index: int,
        contain_unexamined: bool,
    ) -> RankingMetaInfoStatProtocol:
        request = self.cvse.lookupRankingMetaInfo_request()
        request.rank = Rank_to_capnp(rank)
        request.index = index
        request.contain_unexamined = contain_unexamined
        response = await request.send()
        return response.stat

async def __test() -> None:
    # 仅作用法示例，不要运行，防止向数据库中加入无用数据
    if 2 * 2 * 2 * 2 == 16:  # 骗过 IDE
        raise RuntimeError("This is only a usage example, do not run it.")
    client = await CVSE_Client.create("47.104.152.246", "8613")
    test_new_entries: list[RecordingNewEntry] = [
        {
            "avid": "av1",
            "bvid": "bv1",
            "title": "title1",
            "desc": "description1",
            "tags": ["tag1", "tag2"],
            "cover": "cover1",
            "duration": 100,
            "uploader": "uploader1",
            "up_face": "face1",
            "copyright": 1,
            "pubdate": RPCTime.from_datetime(datetime.now()),
            "page": 1,
            "is_examined": False,
            "ranks": [Rank.DOMESTIC, Rank.SV],
            "is_republish": True,
            "staff_info": "staff1",
        },
        {
            "avid": "av2",
            "bvid": "bv2",
            "title": "title2",
            "desc": "description2",
            "tags": ["tag3", "tag4"],
            "cover": "cover2",
            "duration": 200,
            "uploader": "uploader2",
            "up_face": "face2",
            "copyright": 2,
            "pubdate": RPCTime.from_datetime(datetime.now()),
            "page": 2,
            "is_examined": True,
            "ranks": [Rank.UTAU],
            "is_republish": False,
            "staff_info": "staff2",
        },
    ]
    test_data_entries: list[RecordingDataEntry] = [
        {
            "avid": "av1",
            "bvid": "bv1",
            "view": 100,
            "danmaku": 50,
            "reply": 20,
            "favorite": 30,
            "coin": 40,
            "share": 50,
            "like": 60,
            "date": RPCTime.from_datetime(datetime.now()),
        },
        {
            "avid": "av2",
            "bvid": "bv2",
            "view": 200,
            "danmaku": 100,
            "reply": 40,
            "favorite": 60,
            "coin": 80,
            "share": 100,
            "like": 120,
            "date": RPCTime.from_datetime(datetime.now()),
        },
    ]
    test_modify_entries: list[ModifyEntry] = [
        {
            "avid": "av1",
            "bvid": "BV1",
            "is_republish": True,
            "ranks": [],
            "staff": "sss",
            "is_examined": True,
        },
        {
            "avid": "av2",
            "bvid": "BV2",
            "is_republish": False,
            "ranks": [Rank.DOMESTIC],
            "staff": None,
            "is_examined": False,
        },
    ]
    test_new_entries1 = list(map(RecordingNewEntry_to_capnp, test_new_entries))
    await client.updateNewEntry(test_new_entries1)
    test_data_entries1 = list(map(RecordingDataEntry_to_capnp, test_data_entries))
    test_data_entries2 = test_data_entries1 * 100
    test_modify_entries1 = list(map(ModifyEntry_to_capnp, test_modify_entries))
    task1 = asyncio.create_task(client.updateRecordingDataEntry(test_data_entries2))
    task2 = asyncio.create_task(client.updateModifyEntry(test_modify_entries1))
    await asyncio.gather(task1, task2)
    all_info: list[IndexProtocol] = []
    async for batch in client.getAllInBatch(True, True, timedelta(days=60)):
        all_info.extend(batch)
    all_meta_info = await client.lookupMetaInfo(all_info)
    for index in all_info:
        print(f"avid: {index.avid}, bvid: {index.bvid}")
    for meta_info in all_meta_info:
        print(
            f"avid: {meta_info.avid}, bvid: {meta_info.bvid}, title: {meta_info.title}"
        )
    all_data = await client.lookupDataInfo(
        all_info, RPCTime.minValue(), RPCTime.from_datetime(datetime.now())
    )
    for datas in all_data:
        for data in datas:
            print(
                f"{data.bvid}: {data.view} at {capnp_to_RPCTime(data.date).to_datetime()}"
            )

    return


def test():
    asyncio.run(capnp.run(__test()))
