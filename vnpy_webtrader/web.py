import asyncio
import json
import secrets
from dataclasses import dataclass
from datetime import datetime, timedelta
from enum import Enum
from pathlib import Path
from typing import Any, List, Optional, Union

from fastapi import (
    FastAPI,
    HTTPException,
    Query,
    WebSocket,
    WebSocketDisconnect,
    status,
)
from fastapi.responses import HTMLResponse
from pydantic import BaseModel
from vnpy.rpc import RpcClient
from vnpy.rpc.common import HEARTBEAT_TOPIC
from vnpy.trader.constant import Direction, Exchange, Offset, OrderType
from vnpy.trader.object import (
    AccountData,
    CancelRequest,
    ContractData,
    OrderData,
    OrderRequest,
    PositionData,
    SubscribeRequest,
    TickData,
    TradeData,
)
from vnpy_rpcservice.rpc_service.engine import EVENT_RPC_LOG
from vnpy.trader.event import EVENT_LOG
from vnpy.trader.utility import get_file_path, load_json

# Web服务运行配置
SETTING_FILENAME = "web_trader_setting.json"
SETTING_FILEPATH = get_file_path(SETTING_FILENAME)

setting: dict = load_json(SETTING_FILEPATH)
REQ_ADDRESS = setting["req_address"]  # 请求服务地址
SUB_ADDRESS = setting["sub_address"]  # 订阅服务地址


# RPC客户端
rpc_client: RpcClient = None


def to_dict(o: dataclass) -> dict:
    """将对象转换为字典"""
    data: dict = {}
    for k, v in o.__dict__.items():
        if isinstance(v, Enum):
            data[k] = v.value
        elif isinstance(v, datetime):
            data[k] = str(v)
        else:
            data[k] = v
    return data


# 创建FastAPI应用
app: FastAPI = FastAPI()


@app.get("/")
def index() -> HTMLResponse:
    """获取主页面"""
    index_path: Path = Path(__file__).parent.joinpath("static/index.html")
    with open(index_path) as f:
        content: str = f.read()

    return HTMLResponse(content)


@app.get("/apps")
def get_all_apps() -> HTMLResponse:
    return [app.app_name for app in rpc_client.get_all_apps()]


@app.post("/tick/{vt_symbol}")
def subscribe(vt_symbol: str) -> None:
    """订阅行情"""
    contract: Optional[ContractData] = rpc_client.get_contract(vt_symbol)
    if not contract:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"找不到合约{vt_symbol}",
            headers={"WWW-Authenticate": "Bearer"},
        )

    req: SubscribeRequest = SubscribeRequest(contract.symbol, contract.exchange)
    rpc_client.subscribe(req, contract.gateway_name)


@app.get("/tick")
def get_all_ticks() -> list:
    """查询行情信息"""
    ticks: List[TickData] = rpc_client.get_all_ticks()
    return [to_dict(tick) for tick in ticks]


class OrderRequestModel(BaseModel):
    """委托请求模型"""

    symbol: str
    exchange: Exchange
    direction: Direction
    type: OrderType
    volume: float
    price: float = 0
    offset: Offset = Offset.NONE
    reference: str = ""


@app.post("/order")
def send_order(model: OrderRequestModel) -> str:
    """委托下单"""
    req: OrderRequest = OrderRequest(**model.__dict__)

    contract: Optional[ContractData] = rpc_client.get_contract(req.vt_symbol)
    if not contract:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"找不到合约{req.symbol} {req.exchange.value}",
            headers={"WWW-Authenticate": "Bearer"},
        )

    vt_orderid: str = rpc_client.send_order(req, contract.gateway_name)
    return vt_orderid


@app.delete("/order/{vt_orderid}")
def cancel_order(vt_orderid: str) -> None:
    """委托撤单"""
    order: Optional[OrderData] = rpc_client.get_order(vt_orderid)
    if not order:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"找不到委托{vt_orderid}",
            headers={"WWW-Authenticate": "Bearer"},
        )

    req: CancelRequest = order.create_cancel_request()
    rpc_client.cancel_order(req, order.gateway_name)


@app.get("/order")
def get_all_orders() -> list:
    """查询委托信息"""
    orders: List[OrderData] = rpc_client.get_all_orders()
    return [to_dict(order) for order in orders]


@app.get("/trade")
def get_all_trades() -> list:
    """查询成交信息"""
    trades: List[TradeData] = rpc_client.get_all_trades()
    return [to_dict(trade) for trade in trades]


@app.get("/position")
def get_all_positions() -> list:
    """查询持仓信息"""
    positions: List[PositionData] = rpc_client.get_all_positions()
    return [to_dict(position) for position in positions]


@app.get("/account")
def get_all_accounts() -> list:
    """查询账户资金"""
    accounts: List[AccountData] = rpc_client.get_all_accounts()
    return [to_dict(account) for account in accounts]


@app.get("/contract")
def get_all_contracts() -> list:
    """查询合约信息"""
    contracts: List[ContractData] = rpc_client.get_all_contracts()
    return [to_dict(contract) for contract in contracts]


# 活动状态的Websocket连接
active_websockets: List[WebSocket] = []

# 全局事件循环
event_loop: asyncio.AbstractEventLoop = asyncio.get_event_loop()


# websocket传递数据
@app.websocket("/ws/")
async def websocket_endpoint(websocket: WebSocket) -> None:
    """Weboskcet连接处理"""
    await websocket.accept()
    active_websockets.append(websocket)

    try:
        while True:
            await websocket.receive_text()
    except WebSocketDisconnect:
        active_websockets.remove(websocket)


async def websocket_broadcast(msg: str) -> None:
    """Websocket数据广播"""
    for websocket in active_websockets:
        await websocket.send_text(msg)


def rpc_callback(topic: str, data: Any) -> None:
    """RPC回调函数"""
    if not active_websockets:
        return

    data: dict = {"topic": topic, "data": to_dict(data)}
    msg: str = json.dumps(data, ensure_ascii=False)
    asyncio.run_coroutine_threadsafe(websocket_broadcast(msg), event_loop)


@app.on_event("startup")
def startup_event() -> None:
    """应用启动事件"""
    global rpc_client
    rpc_client = RpcClient()
    rpc_client.callback = rpc_callback
    rpc_client.subscribe_topic("")
    rpc_client.subscribe_topic(EVENT_LOG)
    rpc_client.subscribe_topic(EVENT_RPC_LOG)
    rpc_client.start(REQ_ADDRESS, SUB_ADDRESS)

    from vnpy_algotrading.rpc import AlgoWebAPI

    app.include_router(AlgoWebAPI(rpc_client))


@app.on_event("shutdown")
def shutdown_event() -> None:
    """应用停止事件"""
    rpc_client.stop()
