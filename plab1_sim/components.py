from __future__ import annotations
from enum import Enum
from typing import List, Any, Tuple, Optional, Callable, Deque, Dict
import pickle
import copy
from collections import defaultdict
import networkx as nx  # type: ignore
import warnings


class Scheduler(object):
    def __init__(self) -> None:
        pass

    def schedule(self, w: Callable[[], None]) -> None:
        raise NotImplementedError


class PacketType(Enum):
    Data = 1
    Control = 2


class Packet(object):
    def __init__(self, type: PacketType, ttl: int, data: Any) -> None:
        self.type = type
        self.data = pickle.dumps(data)
        self.ttl = ttl


def to_control_packet(data: Any) -> Packet:
    # We do not check TTL on control packets, since
    # they do not propagate.
    return Packet(PacketType.Control, 0, data)


def to_data_packet(data: Any, ttl: int = 32) -> Packet:
    return Packet(PacketType.Data, ttl, data)


def from_packet(pkt: Packet) -> Any:
    return pickle.loads(pkt.data)


class PortState(Enum):
    Up = 1
    Down = 2


class ControlPlane(object):
    def __init__(self) -> None:
        pass

    def initialize(self, switch: SwitchRep) -> None:
        raise NotImplementedError

    def process_control_packet(self, switch: SwitchRep, port_id, data: Any) -> None:
        raise NotImplementedError


class NetNode(object):
    def __init__(self) -> None:
        pass

    def recv(self, port_id: int, packet: Packet) -> None:
        raise NotImplementedError

    def get_id(self) -> str:
        raise NotImplementedError

    def get_ports(self) -> List[Port]:
        raise NotImplementedError


class Host(NetNode):
    def __init__(self, id: str, tracer: Optional[Tracer] = None) -> None:
        self.id = id
        self.port = Port(0, id, self)
        if tracer:
            tracer.add_node(self)

    def recv(self, port_id: int, packet: Packet) -> None:
        if packet.type == PacketType.Data:
            print("%s: host received packet %s" % (self.id, from_packet(packet)))

    def send(self, packet: Packet) -> None:
        print("%s: host sent packet %s" % (self.id, from_packet(packet)))
        self.port.send(packet)

    def get_id(self) -> str:
        return self.id

    def get_ports(self) -> List[Port]:
        return [self.port]


class SwitchRep(object):
    def __init__(self, sw: DumbSwitch):
        self.id = sw.id
        self.port_state = [p.state for p in sw.ports]
        self._sw = sw

    def send_control(self, port_id: int, data: Any):
        self._sw.send(port_id, to_control_packet(data))

    def port_down(self, port_id: int):
        self._sw.set_port_down(port_id)

    def port_up(self, port_id: int):
        self._sw.set_port_up(port_id)


class DumbSwitch(NetNode):
    def __init__(
        self,
        id: str,
        ports: int,
        control_plane: ControlPlane,
        tracer: Optional[Tracer] = None,
    ) -> None:
        self.id = id
        self.ports = [Port(i, id, self) for i in range(ports)]
        self.control = control_plane
        self.rep = SwitchRep(self)
        self.tracer = tracer
        if self.tracer:
            self.tracer.add_node(self)
        self.is_initialized = False

    def get_id(self) -> str:
        return self.id

    def get_ports(self) -> List[Port]:
        return self.ports

    def set_port_up(self, port_id: int) -> PortState:
        self.rep.port_state[port_id] = PortState.Up
        if self.is_initialized and self.tracer:
            self.tracer.set_port_up(self.ports[port_id])
        return self.ports[port_id].set_up()

    def set_port_down(self, port_id: int) -> PortState:
        self.rep.port_state[port_id] = PortState.Down
        if self.is_initialized and self.tracer:
            self.tracer.set_port_down(self.ports[port_id])
        return self.ports[port_id].set_down()

    def initialized(self) -> None:
        self.is_initialized = True
        self.control.initialize(self.rep)

    def recv(self, port_id: int, packet: Packet) -> None:
        if packet.type == PacketType.Data:
            if packet.ttl == 0:
                warnings.warn("Dropping packet because TTL exceeded")
                return
            print("%s: forwarding data packet (%d)" % (self.id, packet.ttl))
            for idx, p in enumerate(self.ports):
                if idx != port_id:
                    pkt = copy.deepcopy(packet)
                    pkt.ttl -= 1
                    p.send(pkt)
        elif packet.type == PacketType.Control:
            data = from_packet(packet)
            if self.tracer:
                cause = "%s (@%s)"%(repr(data), self.id)
                self.tracer.process_control_event(cause)
            self.control.process_control_packet(self.rep, port_id, from_packet(packet))

    def send(self, port_id: int, packet: Packet) -> None:
        self.ports[port_id].send(packet)


class Port(object):
    def __init__(self, id: int, sw_id: str, swtch: NetNode) -> None:
        self.id = id
        self.sw_id = sw_id
        self.link = None  # type: Optional[Link]
        self.state = PortState.Up
        self.swtch = swtch

    def attach(self, link: Link) -> None:
        self.link = link
        link.connect(self)

    def set_up(self) -> PortState:
        current = self.state
        self.state = PortState.Up
        return self.state

    def set_down(self) -> PortState:
        current = self.state
        self.state = PortState.Down
        return self.state

    def send(self, packet: Packet) -> bool:
        if self.link and self.state == PortState.Up:
            self.link.send(self, packet)
            return True
        else:
            return False

    def recv(self, packet: Packet):
        if self.state == PortState.Up:
            self.swtch.recv(self.id, packet)


class Link(object):
    _Count = 0

    def __init__(
        self, id: str, sched: Scheduler, tracer: Optional[Tracer] = None
    ) -> None:
        self.id = id
        self.connects = (None, None)  # type: Tuple[Optional[Port], Optional[Port]]
        self.sched = sched
        self.uniq = Link._Count
        Link._Count += 1
        self.tracer = tracer

    def connect(self, port: Port) -> None:
        if self.connects[0] is None:
            self.connects = (port, self.connects[1])
        elif self.connects[1] is None:
            self.connects = (self.connects[0], port)
        else:
            raise (TooManyConnections(self.id))
        if (
            self.tracer is not None
            and self.connects[0] is not None
            and self.connects[1] is not None
        ):
            self.tracer.add_link(
                self.uniq, self.connects[0].sw_id, self.connects[1].sw_id
            )

    def send(self, port: Port, packet: Packet) -> None:
        if port is self.connects[0] and self.connects[1] is not None:
            p = self.connects[1]
            self.sched.schedule(lambda: p.recv(packet))
        elif port is self.connects[1] and self.connects[0] is not None:
            p = self.connects[0]
            self.sched.schedule(lambda: p.recv(packet))
        else:
            raise (BadSender(self.id, port))


class TooManyConnections(Exception):
    def __init__(self, id: str):
        self.message = "Too many ends for %s" % id


class BadSender(Exception):
    def __init__(self, id: str, port: Port):
        self.message = (
            "Port %d in switch %s tried sending through link %s to which it is not connected"
            % (port.id, port.sw_id, id)
        )


class Tracer(object):
    def __init__(self):
        self.nodes = []
        self.cause = ["initial"]
        self.graph = nx.MultiGraph()
        self.color = [defaultdict(lambda: "black")]  # type: List[Dict[int, str]]
        self.time = 0

    def add_node(self, ob: NetNode) -> None:
        self.nodes.append(ob)
        self.graph.add_node(ob.get_id())

    def add_link(self, link_id: int, node0: str, node1: str) -> None:
        self.graph.add_edge(node0, node1, key=link_id)

    def process_control_event(self, cause: str) -> None:
        self.color.append(self.color[-1].copy())
        self.cause.append(cause)
        self.time += 1

    def set_port_down(self, port: Port) -> None:
        if port.link is not None:
            self.color[self.time][port.link.uniq] = "red"

    def set_port_up(self, port: Port) -> None:
        if (
            port.link is not None
            and port.link.connects[0] is not None
            and port.link.connects[1] is not None
            and port.link.connects[0].state == PortState.Up
            and port.link.connects[1].state == PortState.Up
        ):
            self.color[self.time][port.link.uniq] = "black"

    def draw_graph_at_time(self, time: int, axs: Optional[Any] = None) -> None:
        nx.draw(
            self.graph,
            with_labels=True,
            edge_color=[
                self.color[time][key] for (_, _, key) in self.graph.edges(keys=True)
            ],
            node_size=500,
            font_size=16,
            node_color="white",
            ax = axs,
        )

    def get_nx_graph_at_time(self, time: int) -> nx.MultiGraph:
        return nx.MultiGraph(
            [
                (u, v, k)
                for u, v, k in self.graph.edges(keys=True)
                if self.color[time][k] == "black"
            ]
        )

    def get_cause_at_time(self, time: int) -> str:
        return self.cause[time]

    def get_total_time(self) -> int:
        return self.time + 1

    def __iter__(self):
        for t in range(self.get_total_time()):
            yield self.cause[t], self.get_nx_graph_at_time(t)


class SimObjectHolder(object):
    def __init__(self):
        self.net_objects = []  # type: List[NetNode]

    def add_net_object(self, ob: NetNode):
        self.net_objects.append(ob)

    def identify_links(self):
        all_links = {}
        link_state = defaultdict(lambda: PortState.Up)
        waiting = {}
        for ob in self.net_objects:
            id = ob.get_id()
            for port in ob.get_ports():
                if not port.link:
                    continue
                if port.link.uniq in waiting:
                    all_links[port.link.uniq] = (id, waiting[port.link.uniq])
                    del waiting[port.link.uniq]
                else:
                    waiting[port.link.uniq] = id
                if port.state == PortState.Down:
                    link_state[port.link.uniq] = PortState.Down
        for k in sorted(all_links):

            print(
                "%s---%s (%s)" % (all_links[k][0], all_links[k][1], str(link_state[k]))
            )

    def create_nx_graph(self) -> nx.MultiGraph:
        g = nx.MultiGraph()
        g.add_nodes_from(map(lambda o: o.get_id(), self.net_objects))
        all_links = {}
        link_state = defaultdict(lambda: PortState.Up)  # type: Dict[int, PortState]
        waiting = {}  # type: Dict[int, str]
        for ob in self.net_objects:
            id = ob.get_id()
            for port in ob.get_ports():
                if not port.link:
                    continue
                if port.link.uniq in waiting:
                    all_links[port.link.uniq] = (id, waiting[port.link.uniq])
                    del waiting[port.link.uniq]
                else:
                    waiting[port.link.uniq] = id
                if port.state == PortState.Down:
                    link_state[port.link.uniq] = PortState.Down
        for k in sorted(all_links):
            if link_state[k] == PortState.Up:
                g.add_edge(
                    all_links[k][0], all_links[k][1], color="black", style="solid"
                )
        return g
