from __future__ import annotations
from enum import Enum
from typing import List, Any, Tuple, Optional, Callable, Deque, Dict
import pickle
import copy
from collections import defaultdict
import networkx as nx  # type: ignore
import warnings


class Scheduler(object):
    """An abstract scheduler object that is used by links and others to schedule
    packet sends"""

    def __init__(self) -> None:
        pass

    def schedule(self, w: Callable[[], None]) -> None:
        raise NotImplementedError


class PacketType(Enum):
    """The type of a packet"""

    Data = 1
    Control = 2


class Packet(object):
    """A packet object"""

    def __init__(self, type: PacketType, ttl: int, data: Any) -> None:
        self.type = type
        self.data = pickle.dumps(data)
        self.ttl = ttl


def to_control_packet(data: Any) -> Packet:
    """Construct a control packet from an arbitrary variable"""
    # We do not check TTL on control packets, since
    # they do not propagate.
    return Packet(PacketType.Control, 0, data)


def to_data_packet(data: Any, ttl: int = 32) -> Packet:
    """Construct a data packet from an arbitrary variable"""
    return Packet(PacketType.Data, ttl, data)


def from_packet(pkt: Packet) -> Any:
    """Extract data from a packet"""
    return pickle.loads(pkt.data)


class InterfaceState(Enum):
    """Enumeration that indicates whether an interface is Up or Down."""

    Up = (
        1
    )  #: Interface is UP, i.e., data packets will be sent and received on the interface.
    Down = (
        2
    )  #: Interface is DOWN, i.e. data packets **will not** be send and received on the interface.


class ControlPlane(object):
    """An abstract class that users extend to receive control signals from the data plane.."""

    def __init__(self) -> None:
        pass

    def initialize(self, switch: SwitchRep) -> None:
        """Called when the switch being controlled is connected and ready to forward. It is
        required that at least one switch in the network generate a control packet in this
        method, since otherwise the ControlPlane object will never regain control."""
        raise NotImplementedError

    def process_control_packet(self, switch: SwitchRep, iface_id, data: Any) -> None:
        """Called when the switch receives a control packet on iface `iface_id`.
        `switch` can be used to perform operations on the underlying switch.
        Data here is a Python object sent (e.g., by calling the `send_control` method on `SwitchRep`).
        When called user code has a few options:

        * Do nothing
        * Set an interface up (i.e., allow data to be sent and received through it) by calling sw.iface_up(id)
        * Set an interface down by calling sw.iface_down(id)
        * Send packets by calling sw.send_control.
        * Update internal state.
        """
        raise NotImplementedError


class NetNode(object):
    """Represents a network node: a host or a switch."""

    def __init__(self) -> None:
        pass

    def recv(self, iface_id: int, packet: Packet) -> None:
        raise NotImplementedError

    def get_id(self) -> str:
        raise NotImplementedError

    def get_ifaces(self) -> List[Interface]:
        raise NotImplementedError


class Host(NetNode):
    """ A host"""

    def __init__(self, id: str, tracer: Optional[Tracer] = None) -> None:
        self.id = id
        self.iface = Interface(0, id, self)
        if tracer:
            tracer.add_node(self)

    def recv(self, iface_id: int, packet: Packet) -> None:
        if packet.type == PacketType.Data:
            print("%s: host received packet %s" % (self.id, from_packet(packet)))

    def send(self, packet: Packet) -> None:
        print("%s: host sent packet %s" % (self.id, from_packet(packet)))
        self.iface.send(packet)

    def get_id(self) -> str:
        return self.id

    def get_ifaces(self) -> List[Interface]:
        return [self.iface]


class SwitchRep(object):
    """SwitchRep is a switch abstraction supplied to the control code. It allows
    the control code to send control messages, set interfaces up and down, and query
    for interface state."""

    def __init__(self, sw: DumbSwitch):
        self._id = sw.id
        self.iface_state = [p.state for p in sw.ifaces]
        self._sw = sw

    @property
    def id(self) -> str:
        """Get a unique switch ID. The ID is both unique and can be compared to
        IDs for other switches. The ID is also guaranteed to have a total order."""
        return self._id

    def send_control(self, iface_id: int, data: Any):
        """Send a control message out interface `iface_id`. Note
        messages sent out an interface marked down are silently
        dropped."""
        self._sw.send(iface_id, to_control_packet(data))

    def iface_down(self, iface_id: int):
        """Mark interface `iface_id` as down"""
        self._sw.set_iface_down(iface_id)

    def iface_up(self, iface_id: int):
        """Mark interface `iface_id` as up"""
        self._sw.set_iface_up(iface_id)

    def iface_status(self, iface_id: int):
        """Get status for interface `iface_id`"""
        return self.iface_state[iface_id]

    def iface_count(self) -> int:
        """Get the number of interfacess in this switch"""
        return len(self.iface_state)


class DumbSwitch(NetNode):
    def __init__(
        self,
        id: str,
        ifaces: int,
        control_plane: ControlPlane,
        tracer: Optional[Tracer] = None,
    ) -> None:
        self.id = id
        self.ifaces = [Interface(i, id, self) for i in range(ifaces)]
        self.control = control_plane
        self.rep = SwitchRep(self)
        self.tracer = tracer
        if self.tracer:
            self.tracer.add_node(self)
        self.is_initialized = False

    def get_id(self) -> str:
        return self.id

    def get_ifaces(self) -> List[Interface]:
        return self.ifaces

    def set_iface_up(self, iface_id: int) -> InterfaceState:
        self.rep.iface_state[iface_id] = InterfaceState.Up
        if self.is_initialized and self.tracer:
            self.tracer.set_iface_up(self.ifaces[iface_id])
        return self.ifaces[iface_id].set_up()

    def set_iface_down(self, iface_id: int) -> InterfaceState:
        self.rep.iface_state[iface_id] = InterfaceState.Down
        if self.is_initialized and self.tracer:
            self.tracer.set_iface_down(self.ifaces[iface_id])
        return self.ifaces[iface_id].set_down()

    def initialized(self) -> None:
        self.is_initialized = True
        self.control.initialize(self.rep)

    def recv(self, iface_id: int, packet: Packet) -> None:
        if packet.type == PacketType.Data:
            if self.ifaces[iface_id].state == InterfaceState.Down:
                return
            if packet.ttl == 0:
                warnings.warn("Dropping packet because TTL exceeded")
                return
            print("%s: forwarding data packet (%d)" % (self.id, packet.ttl))
            for idx, p in enumerate(self.ifaces):
                if idx != iface_id and p.state == InterfaceState.Up:
                    pkt = copy.deepcopy(packet)
                    pkt.ttl -= 1
                    p.send(pkt)
        elif packet.type == PacketType.Control:
            data = from_packet(packet)
            if self.tracer:
                cause = "%s (@%s)" % (repr(data), self.id)
                self.tracer.process_control_event(cause)
            self.control.process_control_packet(self.rep, iface_id, from_packet(packet))

    def send(self, iface_id: int, packet: Packet) -> None:
        self.ifaces[iface_id].send(packet)


class Interface(object):
    def __init__(self, id: int, sw_id: str, swtch: NetNode) -> None:
        self.id = id
        self.sw_id = sw_id
        self.link = None  # type: Optional[Link]
        self.state = InterfaceState.Up
        self.swtch = swtch

    def attach(self, link: Link) -> None:
        self.link = link
        link.connect(self)

    def set_up(self) -> InterfaceState:
        current = self.state
        self.state = InterfaceState.Up
        return self.state

    def set_down(self) -> InterfaceState:
        current = self.state
        self.state = InterfaceState.Down
        return self.state

    def send(self, packet: Packet) -> bool:
        if self.link:
            self.link.send(self, packet)
            return True
        else:
            return False

    def recv(self, packet: Packet):
        self.swtch.recv(self.id, packet)


class Link(object):
    _Count = 0

    def __init__(
        self, id: str, sched: Scheduler, tracer: Optional[Tracer] = None
    ) -> None:
        self.id = id
        self.connects = (
            None,
            None,
        )  # type: Tuple[Optional[Interface], Optional[Interface]]
        self.sched = sched
        self.uniq = Link._Count
        Link._Count += 1
        self.tracer = tracer

    def connect(self, iface: Interface) -> None:
        if self.connects[0] is None:
            self.connects = (iface, self.connects[1])
        elif self.connects[1] is None:
            self.connects = (self.connects[0], iface)
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

    def send(self, iface: Interface, packet: Packet) -> None:
        if iface is self.connects[0] and self.connects[1] is not None:
            p = self.connects[1]
            self.sched.schedule(lambda: p.recv(packet))
        elif iface is self.connects[1] and self.connects[0] is not None:
            p = self.connects[0]
            self.sched.schedule(lambda: p.recv(packet))
        else:
            raise (BadSender(self.id, iface))


class TooManyConnections(Exception):
    def __init__(self, id: str):
        self.message = "Too many ends for %s" % id


class BadSender(Exception):
    def __init__(self, id: str, iface: Interface):
        self.message = (
            "Interface %d in switch %s tried sending through link %s to which it is not connected"
            % (iface.id, iface.sw_id, id)
        )


class Tracer(object):
    """The tracer class provides a mechanism for debugging the simulation after the fact. What the
    tracer refers to as time really is an ordering of control messages processed by the network."""

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

    def set_iface_down(self, iface: Interface) -> None:
        if iface.link is not None:
            self.color[self.time][iface.link.uniq] = "red"

    def set_iface_up(self, iface: Interface) -> None:
        if (
            iface.link is not None
            and iface.link.connects[0] is not None
            and iface.link.connects[1] is not None
            and iface.link.connects[0].state == InterfaceState.Up
            and iface.link.connects[1].state == InterfaceState.Up
        ):
            self.color[self.time][iface.link.uniq] = "black"

    def draw_graph_at_n(self, n: int, axs: Optional[Any] = None) -> None:
        """Draw the set of active links (in black) and inactive links (in red) after control
        message `n` was processed."""
        nx.draw(
            self.graph,
            with_labels=True,
            edge_color=[
                self.color[n][key] for (_, _, key) in self.graph.edges(keys=True)
            ],
            node_size=500,
            font_size=16,
            node_color="white",
            ax=axs,
        )

    def get_nx_graph_at_n(self, n: int) -> nx.MultiGraph:
        """Get a connectivity graph after processing control message `n` is processed"""
        return nx.MultiGraph(
            [
                (u, v, k)
                for u, v, k in self.graph.edges(keys=True)
                if self.color[n][k] == "black"
            ]
        )

    def get_cause_at_time(self, n: int) -> str:
        """Get what the n^{th} control message was. This returns a string of the form
        `__repr(data) @ switch ID`, and is why having a good `__repr__` string for your
        Data class is useful."""
        return self.cause[n]

    def get_total_time(self) -> int:
        """Get the total number of control messages"""
        return self.time + 1

    def __iter__(self):
        for t in range(self.get_total_time()):
            yield self.cause[t], self.get_nx_graph_at_n(t)


class SimObjectHolder(object):
    """The SimObjectHolder is just a simple object we can use to check the final result
    from the simulation. In particular it is responsible for constructing the final
    forwarding graph produced by your simulation."""

    def __init__(self):
        self.net_objects = []  # type: List[NetNode]

    def add_net_object(self, ob: NetNode):
        self.net_objects.append(ob)

    def identify_links(self):
        all_links = {}
        link_state = defaultdict(lambda: InterfaceState.Up)
        waiting = {}
        for ob in self.net_objects:
            id = ob.get_id()
            for iface in ob.get_ifaces():
                if not iface.link:
                    continue
                if iface.link.uniq in waiting:
                    all_links[iface.link.uniq] = (id, waiting[iface.link.uniq])
                    del waiting[iface.link.uniq]
                else:
                    waiting[iface.link.uniq] = id
                if iface.state == InterfaceState.Down:
                    link_state[iface.link.uniq] = InterfaceState.Down
        for k in sorted(all_links):

            print(
                "%s---%s (%s)" % (all_links[k][0], all_links[k][1], str(link_state[k]))
            )

    def create_nx_graph(self) -> nx.MultiGraph:
        """Return the final connectivity graph for this simulation"""
        g = nx.MultiGraph()
        g.add_nodes_from(map(lambda o: o.get_id(), self.net_objects))
        all_links = {}
        link_state = defaultdict(
            lambda: InterfaceState.Up
        )  # type: Dict[int, InterfaceState]
        waiting = {}  # type: Dict[int, str]
        for ob in self.net_objects:
            id = ob.get_id()
            for iface in ob.get_ifaces():
                if not iface.link:
                    continue
                if iface.link.uniq in waiting:
                    all_links[iface.link.uniq] = (id, waiting[iface.link.uniq])
                    del waiting[iface.link.uniq]
                else:
                    waiting[iface.link.uniq] = id
                if iface.state == InterfaceState.Down:
                    link_state[iface.link.uniq] = InterfaceState.Down
        for k in sorted(all_links):
            if link_state[k] == InterfaceState.Up:
                g.add_edge(
                    all_links[k][0], all_links[k][1], color="black", style="solid"
                )
        return g
