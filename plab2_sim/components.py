from __future__ import annotations
from enum import Enum
from typing import List, Any, Tuple, Optional, Callable, Deque, Dict, Iterator, Set
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


Address = int  #: Addresses are represented as integers here.
BroadcastAddress = (
    0xFFFFFFFF
)  # type: Address #: Represents a packet designed to be broadcast


class AddressablePacket(Packet):
    """A packet object with source and destination addresses"""

    def __init__(
        self,
        source: Address,
        destination: Address,
        type: PacketType,
        ttl: int,
        data: Any,
    ) -> None:
        super().__init__(type, ttl, data)
        self.source = source
        self.destination = destination


def to_control_packet(data: Any) -> Packet:
    """Construct a control packet from an arbitrary variable"""
    # We do not check TTL on control packets, since
    # they do not propagate.
    return Packet(PacketType.Control, 0, data)


def to_data_packet(
    source: Address, destination: Address, data: Any, ttl: int = 32
) -> AddressablePacket:
    """Construct a data packet from an arbitrary variable"""
    return AddressablePacket(source, destination, PacketType.Data, ttl, data)


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


class LinkState(Enum):
    """Enumeration that indicates whether a link is usable or not."""

    Up = 1  #: Link is UP (i.e., can forward packets)
    Down = 2  #: Link is DOWN (i.e., link cannot forward packet)


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

    def process_link_up(self, switch: SwitchRep, port_id: int) -> None:
        """Called when the link connected to port `port_id` recovers and thus becomes available."""
        raise NotImplementedError

    def process_link_down(self, switch: SwitchRep, port_id: int) -> None:
        """Called when the link connected to port `port_id` fails and thus becomes unavailable."""
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

    def notify_link_up(self, port_id: int) -> None:
        raise NotImplementedError

    def notify_link_down(self, port_id: int) -> None:
        raise NotImplementedError

    def how_forward(self, a: Address) -> Optional[int]:
        raise NotImplementedError


class HostIdentification(object):
    """A message sent by hosts when first initialized. This message allows switches to discover
    the set of hosts they are connected to and construct forwarding entries for hosts to which
    they are directly connected."""

    def __init__(self, host_id: str, address: Address) -> None:
        self._sender_id = host_id
        self._sender_address = address

    @property
    def sender_id(self) -> str:
        """ The ID of the host that sent this ID information"""
        return self._sender_id

    @property
    def address(self) -> Address:
        """The sender's address"""
        return self._sender_address


class Host(NetNode):
    """ A host"""

    def __init__(
        self, id: str, address: Address, tracer: Optional[Tracer] = None
    ) -> None:
        self.id = id
        self.iface = Interface(0, id, self)
        if tracer:
            tracer.add_node(self)
        self.address = address

    def recv(self, iface_id: int, packet: Packet) -> None:
        if packet.type == PacketType.Data:
            if isinstance(packet, AddressablePacket):
                if packet.destination != self.address:
                    warnings.warn(
                        "Host %s received a packet not destined for it" % self.id
                    )
                else:
                    print(
                        "%s: host received packet %s" % (self.id, from_packet(packet))
                    )
            else:
                warnings.warn(
                    "%s: host received a non-addressed packet %s"
                    % (self.id, from_packet(packet))
                )

    def send(self, packet: AddressablePacket) -> None:
        packet.source = self.address
        print("%s: host sent packet %s" % (self.id, from_packet(packet)))
        self.iface.send(packet)

    def get_id(self) -> str:
        return self.id

    def get_ifaces(self) -> List[Interface]:
        return [self.iface]

    def get_address(self) -> Address:
        return self.address

    def send_host_identification(self) -> None:
        packet = to_control_packet(HostIdentification(self.id, self.address))
        self.port.send(packet)

    def notify_link_up(self, port_id: int) -> None:
        return  # Nothing to really do here, hosts are not handling this.

    def notify_link_down(self, port_id: int) -> None:
        return  # Nothing to really do here.

    def how_forward(self, a: Address) -> Optional[int]:
        return None


class SwitchRep(object):
    """SwitchRep is a switch abstraction supplied to the control code. It allows
    the control code to send control messages, set interfaces up and down, and query
    for interface state."""

    def __init__(self, sw: ForwardingSwitch):
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

    def iface_status(self, iface_id: int) -> InterfaceState:
        """Get status for interface `iface_id`"""
        return self.iface_state[iface_id]

    def iface_count(self) -> int:
        """Get the number of interfacess in this switch"""
        return len(self.iface_state)

    def update_forwarding_table(self, address: Address, port: int) -> None:
        """Update forwarding table so packets destined to `address` will
        be forwarded out `port`"""
        self._sw.update_forwarding_table(address, port)

    def get_forwarding_for_address(self, address: Address) -> Optional[int]:
        """Retrieve how packets destined to `address` are forwarded. In case
        the address is unknown this function returns None."""
        return self._sw.get_forwarding_for_address(address)

    def get_known_addresses(self) -> List[Address]:
        """Get all addresses that this switch can forward"""
        return self._sw.get_known_addresses()

    def get_forwarding_table(self) -> Dict[Address, int]:
        """Return the full forwarding table for this switch."""
        return self._sw.get_forwarding_table()

    def del_forwarding_entry(self, addr: Address) -> None:
        """Delete entry for address a"""
        self._sw.delete_forwarding_entry(addr)


class ForwardingSwitch(NetNode):
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
        self.forwarding_table = {}  # type: Dict[Address, int]

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

    def update_forwarding_table(self, address: Address, port: int) -> None:
        self.forwarding_table[address] = port
        if self.tracer:
            self.tracer.update_forwarding_table(self.id, address, port)

    def get_forwarding_for_address(self, address: Address) -> Optional[int]:
        return self.forwarding_table.get(address, None)

    def get_forwarding_table(self) -> Dict[Address, int]:
        return dict(self.forwarding_table)

    def delete_forwarding_entry(self, address: Address) -> None:
        if address in self.forwarding_table:
            del self.forwarding_table[address]
            if self.tracer:
                self.tracer.delete_forwarding_table_entry(self.id, address)

    def get_known_addresses(self) -> List[Address]:
        return list(self.forwarding_table.keys())

    def initialized(self) -> None:
        self.is_initialized = True
        self.control.initialize(self.rep)

    def recv(self, iface_id: int, packet: Packet) -> None:
        def broadcast():
            for idx, p in enumerate(self.ports):
                if (idx != port_id and
                    self.ifaces[iface_id].state == InterfaceState.Up):
                    pkt = copy.deepcopy(packet)
                    pkt.ttl -= 1
                    p.send(pkt)

        if packet.type == PacketType.Data:
            if packet.ttl == 0:
                warnings.warn("Dropping packet because TTL exceeded")
                return
            if self.ifaces[iface_id].state != InterfaceState.Up:
                return
            if isinstance(packet, AddressablePacket):
                if packet.destination in self.forwarding_table:
                    out_port = self.forwarding_table[packet.destination]
                    if out_port == port_id:
                        warnings.warn(
                            "Switch %s is forwarding packet to destination %d out the port it was received"
                            % (self.id, packet.destination)
                        )
                    if self.ifaces[out_port].state != InterfaceState.Up:
                        warnings.warn(
                            "Switch %s dropping packet out disabled interface"%(self.id))
                        return
                    # Do not need to deepcopy here since only one copy :)
                    packet.ttl -= 1
                    self.ports[out_port].send(packet)
                elif packet.destination == BroadcastAddress:
                    broadcast()
            else:
                warnings.warn("Received a DATA packet without an address")
                broadcast()
        elif packet.type == PacketType.Control:
            data = from_packet(packet)
            if self.tracer:
                cause = "%s (@%s)" % (repr(data), self.id)
                self.tracer.process_control_event(cause)
            self.control.process_control_packet(self.rep, iface_id, from_packet(packet))

    def send(self, iface_id: int, packet: Packet) -> None:
        self.ifaces[iface_id].send(packet)

    def notify_link_up(self, port_id: int) -> None:
        self.control.process_link_up(self.rep, port_id)

    def notify_link_down(self, port_id: int) -> None:
        self.control.process_link_down(self.rep, port_id)

    def how_forward(self, a: Address) -> Optional[int]:
        return self.forwarding_table.get(a, None)


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

    def __str__(self):
        return "(%s, %d)" % (self.sw_id, self.id)

    def notify_link_up(self) -> None:
        self.swtch.notify_link_up(self.id)

    def notify_link_down(self) -> None:
        self.swtch.notify_link_down(self.id)


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
        self.state = LinkState.Up

    def __str__(self) -> str:
        return self.id

    def __repr__(self) -> str:
        return self.id

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

    def get_other_end(self, port: Port) -> Optional[Port]:
        if port is self.connects[0]:
            return self.connects[1]
        elif port is self.connects[1]:
            return self.connects[0]
        else:
            raise (BadSender(self.id, port))

    def send(self, iface: Interface, packet: Packet) -> None:
        if iface is self.connects[0] and self.connects[1] is not None:
            p = self.connects[1]
            self.sched.schedule(lambda: p.recv(packet))
        elif iface is self.connects[1] and self.connects[0] is not None:
            p = self.connects[0]
            self.sched.schedule(lambda: p.recv(packet))
        else:
            raise (BadSender(self.id, iface))

    def set_link_up(self) -> None:
        if self.state == LinkState.Down:
            self.state = LinkState.Up
            if (
                self.tracer is not None
                and self.connects[0] is not None
                and self.connects[1] is not None
            ):
                self.tracer.process_control_event(
                    "Link %s--%s UP" % (str(self.connects[0]), str(self.connects[1]))
                )
            if self.connects[0] is not None:
                self.connects[0].notify_link_up()
            if self.connects[1] is not None:
                self.connects[1].notify_link_up()

    def set_link_down(self) -> None:
        if self.state == LinkState.Up:
            self.state = LinkState.Down
            if (
                self.tracer is not None
                and self.connects[0] is not None
                and self.connects[1] is not None
            ):
                self.tracer.process_control_event(
                    "Link %s--%s DOWN" % (str(self.connects[0]), str(self.connects[1]))
                )
            if self.connects[0] is not None:
                self.connects[0].notify_link_down()
            if self.connects[1] is not None:
                self.connects[1].notify_link_down()

    def link_is_active(self) -> bool:
        return (
            self.state == LinkState.Up
            and self.connects[0] is not None
            and self.connects[1] is not None
            and self.connects[0].state == PortState.Up
            and self.connects[1].state == PortState.Up
        )


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
        self.forwarding_tables = [
            defaultdict(lambda: {})
        ]  # type: List[Dict[str, Dict[Address, int]]]

    def add_node(self, ob: NetNode) -> None:
        self.nodes.append(ob)
        self.graph.add_node(ob.get_id())

    def add_link(self, link_id: int, node0: str, node1: str) -> None:
        self.graph.add_edge(node0, node1, key=link_id)

    def process_control_event(self, cause: str) -> None:
        self.color.append(self.color[-1].copy())
        self.forwarding_tables.append(self.forwarding_tables[-1].copy())
        self.cause.append(cause)
        self.time += 1

    def update_forwarding_table(self, switch_id: str, address: Address, port: int):
        self.forwarding_tables[self.time][switch_id][address] = port

<<<<<<< HEAD
    def set_iface_down(self, iface: Interface) -> None:
        if iface.link is not None:
            self.color[self.time][iface.link.uniq] = "red"
=======
    def delete_forwarding_table_entry(self, switch_id: str, address: Address):
        del self.forwarding_tables[self.time][switch_id][address]

    def set_port_down(self, port: Port) -> None:
        if port.link is not None:
            self.color[self.time][port.link.uniq] = "red"
>>>>>>> More debugging

    def set_iface_up(self, iface: Interface) -> None:
        if (
            iface.link is not None
            and iface.link.connects[0] is not None
            and iface.link.connects[1] is not None
            and iface.link.connects[0].state == InterfaceState.Up
            and iface.link.connects[1].state == InterfaceState.Up
        ):
            self.color[self.time][iface.link.uniq] = "black"

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
        self.hosts = []  # type: List[Host]
        self.host_map = {}  # type: Dict[str, Host]
        self.links = []  # type: List[Link]

    def add_net_object(self, ob: NetNode):
        self.net_objects.append(ob)
        if isinstance(ob, Host):
            self.hosts.append(ob)
            self.host_map[ob.get_id()] = ob

    def add_link(self, l: Link):
        self.links.append(l)

    def get_all_addresses(self) -> Iterator[Address]:
        return map(lambda o: o.get_address(), self.hosts)

    def get_address_map(self) -> Dict[str, Address]:
        return dict(map(lambda o: (o.get_id(), o.get_address()), self.hosts))

    def get_next_hop(self, a: Address, c: NetNode) -> Optional[NetNode]:
        o_p = c.how_forward(a)
        if o_p is not None:
            o = c.get_ports()[o_p]
            if o.link and o.link.link_is_active():
                e = o.link.get_other_end(o)
                if e and e.state == PortState.Up:
                    return e.swtch
        return None

    def get_first_hop(self, h: Host) -> Optional[NetNode]:
        h_port = h.get_ports()[0]
        if h_port.state == PortState.Down or not h_port.link:
            # Nothing is connected
            return None
        h_link = h_port.link
        h_other_port = h_link.get_other_end(h_port)
        if not h_other_port or not h_link.link_is_active():
            return None
        return h_other_port.swtch

    def get_forwarding_graph_for_host(self, host: str) -> nx.MultiDiGraph:
        g = nx.MultiDiGraph()
        g.add_nodes_from(map(lambda o: o.get_id(), self.net_objects))

        def add_links(s: NetNode, a: Address) -> None:
            c = s
            n = self.get_next_hop(a, c)
            visited = set(c.get_id())  # type: Set[str]
            while n is not None:
                g.add_edge(c.get_id(), n.get_id(), address=a)
                if n.get_id() in visited:
                    # We found a loop, so just terminate
                    # here.
                    return
                visited.add(n.get_id())
                c = n
                n = self.get_next_hop(a, c)

        addresses = self.get_all_addresses()
        h_obj = self.host_map[host]
        h_address = h_obj.get_address()
        first_hop = self.get_first_hop(h_obj)
        if not first_hop or not isinstance(first_hop, ForwardingSwitch):
            raise Exception(
                "Topology with host connected to something other than a forwarding switch"
            )

        for a in addresses:
            g.add_edge(h_obj.get_id(), first_hop.get_id(), address=a)
            add_links(first_hop, a)
        return g

    def get_physically_connected_hosts(self) -> List[List[str]]:
        """Get the set of all hosts that are physically connected"""
        g = nx.MultiGraph()
        g.add_nodes_from(map(lambda o: o.get_id(), self.net_objects))
        for l in self.links:
            if (
                l.link_is_active()
                and l.connects[0] is not None
                and l.connects[1] is not None
            ):
                g.add_edge(l.connects[0].swtch.get_id(), l.connects[1].swtch.get_id())
        components = nx.connected_components(g)
        return list(
            map(lambda c: list(filter(lambda n: n in self.host_map, c)), components)
        )

    def get_connectivity_matrix(self) -> Dict[str, Dict[Address, str]]:
        """Get the node to which packets sent by a host with a given address are
        forwarded."""

        def get_other_end(s: NetNode, a: Address) -> NetNode:
            c = s
            n = self.get_next_hop(a, c)
            visited = set([c.get_id()])  # type: Set[str]
            while n is not None:
                if n.get_id() in visited:
                    # We found a loop, so just terminate
                    # here.
                    return c
                visited.add(n.get_id())
                c = n
                n = self.get_next_hop(a, c)
            return c

        addresses = list(self.get_all_addresses())
        output = {}  # type: Dict[str, Dict[Address, str]]
        for h in self.hosts:
            fh = self.get_first_hop(h)
            id = h.get_id()
            output[id] = {}
            for address in addresses:
                if fh is not None:
                    oe = get_other_end(fh, address)
                    output[id][address] = oe.get_id()
        return output

    def get_paths(self) -> Dict[str, Dict[Address, List[str]]]:
        """Get the paths from any host given an address"""

        def explore_path(s: NetNode, a: Address) -> List[str]:
            c = s
            n = self.get_next_hop(a, c)
            visited = set([c.get_id()])  # type: Set[str]
            path = [c.get_id()]
            while n is not None:
                path.append(n.get_id())
                if n.get_id() in visited:
                    # We found a loop, so just terminate
                    # here.
                    return path
                visited.add(n.get_id())
                c = n
                n = self.get_next_hop(a, c)
            return path

        addresses = list(self.get_all_addresses())
        output = {}  # type: Dict[str, Dict[Address, List[str]]]
        for h in self.hosts:
            fh = self.get_first_hop(h)
            id = h.get_id()
            output[id] = {}
            for address in addresses:
                if fh is not None:
                    oe = explore_path(fh, address)
                    output[id][address] = oe
        return output

    def get_distance_matrix(self) -> Dict[str, Dict[Address, int]]:
        """Get number of hops a packet sent by a node is forwarded before
        delivery. We return -1 for cases where a loop is encountered"""

        def get_distance(s: NetNode, a: Address, h: str) -> int:
            c = s
            n = self.get_next_hop(a, c)
            visited = set(c.get_id())  # type: Set[str]
            d = 1  # Start at 1 since we are already
            # one hop away.
            while n is not None:
                d += 1
                if n.get_id() in visited:
                    # We found a loop, so just terminate
                    # here.
                    return -1
                visited.add(n.get_id())
                c = n
                n = self.get_next_hop(a, c)
            if c.get_id() == h:
                return d
            else:
                return -1

        # addresses = list(self.get_all_addresses())
        addresses = self.get_address_map()
        output = {}  # type: Dict[str, Dict[Address, int]]
        for h in self.hosts:
            fh = self.get_first_hop(h)
            id = h.get_id()
            output[id] = {}
            for host, address in addresses.items():
                if fh is not None:
                    output[id][address] = get_distance(fh, address, host)
        return output

    def check_loop_freedom(self) -> bool:
        """Check if forwarding is loop free. If so we return True, else
        return False"""

        def is_loopy_forward(s: NetNode, a: Address) -> bool:
            c = s
            n = self.get_next_hop(a, c)
            visited = set([c.get_id()])  # type: Set[str]
            while n is not None:
                if n.get_id() in visited:
                    # We found a loop, so just terminate
                    # here.
                    return True
                visited.add(n.get_id())
                c = n
                n = self.get_next_hop(a, c)
            return False

        addresses = list(self.get_all_addresses())
        for h in self.hosts:
            fh = self.get_first_hop(h)
            for address in addresses:
                if fh is not None:
                    if is_loopy_forward(fh, address):
                        return False  # We found a loop
        return True

    def check_complete(self) -> bool:
        """Check that all hosts are connected to each other"""
        cmatrix = self.get_connectivity_matrix()
        address_map = self.get_address_map()
        components = self.get_physically_connected_hosts()
        for c in components:
            for h in c:
                for h2 in c:
                    a = address_map[h2]
                    if a not in cmatrix[h] or cmatrix[h][a] != h2:
                        return False  # We found a disturbing lack of completeness
        return True
