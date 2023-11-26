from importlib import import_module
from collections import defaultdict
from typing import Callable, Dict, List

from vnpy.rpc import RpcServer
from vnpy.trader.engine import BaseEngine, MainEngine
from vnpy.trader.event import (
    EVENT_TICK,
    EVENT_ORDER,
    EVENT_TRADE,
    EVENT_POSITION,
    EVENT_ACCOUNT
)
from vnpy.event import EventEngine, Event

APP_NAME = "RpcService"


class WebEngine(BaseEngine):
    """Web服务引擎"""

    def __init__(self, main_engine: MainEngine, event_engine: EventEngine) -> None:
        """"""
        super().__init__(main_engine, event_engine, APP_NAME)

        self.server: RpcServer = RpcServer()
        self.rpc_servers = {}

        self.init_apps()
        self.init_server()
        self.register_event()

    def init_server(self) -> None:
        """初始化RPC服务器"""
        self.server.register(self.main_engine.connect)
        self.server.register(self.main_engine.subscribe)
        self.server.register(self.main_engine.send_order)
        self.server.register(self.main_engine.cancel_order)

        self.server.register(self.main_engine.get_contract)
        self.server.register(self.main_engine.get_order)
        self.server.register(self.main_engine.get_all_ticks)
        self.server.register(self.main_engine.get_all_orders)
        self.server.register(self.main_engine.get_all_trades)
        self.server.register(self.main_engine.get_all_positions)
        self.server.register(self.main_engine.get_all_accounts)
        self.server.register(self.main_engine.get_all_contracts)
        self.server.register(self.main_engine.get_all_apps)

    def init_apps(self) -> None:
        all_apps: List[BaseApp] = self.main_engine.get_all_apps()
        for app in all_apps:
            rpc_module: ModuleType = import_module(app.app_module + ".rpc")
            rpc_class = getattr(rpc_module, app.rpc_server)
            rpc_server = rpc_class(self.main_engine, self.event_engine)
            for handler in RpcServerHandler.get_handlers(rpc_server):
                self.server.register(handler)
            self.rpc_servers[app.app_name] = rpc_server

    def start_server(
        self,
        rep_address: str,
        pub_address: str,
    ) -> None:
        """启动RPC服务器"""
        if self.server.is_active():
            return

        self.server.start(rep_address, pub_address)

    def register_event(self) -> None:
        """注册事件监听"""
        self.event_engine.register(EVENT_TICK, self.process_event)
        self.event_engine.register(EVENT_TRADE, self.process_event)
        self.event_engine.register(EVENT_ORDER, self.process_event)
        self.event_engine.register(EVENT_POSITION, self.process_event)
        self.event_engine.register(EVENT_ACCOUNT, self.process_event)

    def process_event(self, event: Event) -> None:
        """处理事件"""
        self.server.publish(event.type, event.data)

    def close(self):
        """关闭"""
        self.server.stop()
        self.server.join()


class RpcServerHandler:

    def __init__(self, handler, handler_name=None):
        handler.__rpc_handler__ = True
        self.handler = handler
        self.handler_name = handler_name or handler.__name__
        setattr(handler, "__name__", self.handler_name)

    def __get__(self, obj, objtype=None):
        return self.handler.__get__(obj, objtype)

    @staticmethod
    def get_handlers(rpc_server_instance):
        handlers = []
        for attr_name in dir(rpc_server_instance):
            attr = getattr(rpc_server_instance, attr_name)
            if callable(attr) and hasattr(attr, "__rpc_handler__"):
                handlers.append(attr)
        return handlers


def rpc_handler(handler_name=None):
    def wrapper(handler):
        return RpcServerHandler(handler, handler_name)
    return wrapper