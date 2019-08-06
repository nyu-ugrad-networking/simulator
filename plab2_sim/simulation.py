from __future__ import annotations
from typing import List, Tuple, Callable, Optional, Dict, Deque, Iterator, Any
from . import components
from collections import defaultdict, deque
import yaml
import warnings
import networkx as nx  # type: ignore
import pprint


class SimpleScheduler(components.Scheduler):
    """This class provides a simple bounded scheduler. By default we limit
    both the number of events that can be scheduled at a time, and the total
    number of events we process, thus ensuring completion."""

    def __init__(self, len_limit: int, event_limit: int) -> None:
        """len_limit here indicates the number of simultaneous outstanding events,
        while event_limit is the number of total events executed."""
        self.len_limit = len_limit
        self.event_limit = event_limit
        self.events_executed = 0
        self.events = deque()  # type: Deque[Callable[[], None]]

    def schedule(self, w: Callable[[], None]) -> None:
        """Schedule an event to be executed. We execute everything FIFO, and represent
        events by a callable."""
        if len(self.events) < self.len_limit:
            self.events.append(w)
        else:
            warnings.warn("Cannot schedule more events, too much to do")

    def run(self) -> None:
        """Execute events until there is either nothing to do or the execution budget is
        exhausted."""
        while len(self.events) > 0:
            self.events.popleft()()
            self.events_executed += 1
            if self.events_executed >= self.event_limit:
                break
        if len(self.events) > 0:
            warnings.warn("There are unexecuted events left.")

    def step_execution(self) -> None:
        """Single step and run a single event. This does not enforce the execution budget
        since we assume you know what you are doing when using this method"""
        if len(self.events) > 0:
            self.events.popleft()()
            self.events_executed += 1

    def execution_steps(self) -> int:
        """Return the total number of events executed"""
        return self.events_executed

    def has_exhausted_limit(self) -> bool:
        """Check whether more events can be executed"""
        return self.events_executed >= self.event_limit

    def reset_execution_steps(self) -> None:
        self.events_executed = 0


class SimulationSetup(object):
    """SimulationSetup objects represent a network simulation environment. They are
    used to both create and set up the network topology, run simulation and analyze
    results"""

    def __init__(
        self,
        hosts: Dict[str, components.Address],
        switches: List[str],
        nifaces: int,
        edges: List[Tuple[str, str]],
        enable_trace: bool,
        control: Callable[[], components.ControlPlane],
        simulataneous_events: int = 1024,
        total_event_budget: int = 10000,
        failure_events=[],
    ):
        """We do not recommend directly calling this function, and instead recommend calling
        the static initializers"""
        self.scheduler = SimpleScheduler(simulataneous_events, total_event_budget)
        self.edges = edges
        self.control_factory = control
        if enable_trace:
            self.tracer = components.Tracer()  # type: Optional[components.Tracer]
        else:
            self.tracer = None
        self.holder = components.SimObjectHolder()
        self.switches = []  # type: List[components.ForwardingSwitch]
        self.nodes = {}  # type: Dict[str, components.NetNode]
        self.hosts = []  # type: List[components.Host]
        self.failure_events = deque(failure_events)  # type: Deque[Tuple[str, str]]
        self.run_between_failures = []  # type: List[Callable[[], None]]
        for switch in switches:
            sw = components.ForwardingSwitch(switch, nifaces, control(), self.tracer)
            self.nodes[switch] = sw
            self.switches.append(sw)
            self.holder.add_net_object(sw)
        for host_id, addr in hosts.items():
            ho = components.Host(host_id, addr, self.tracer)
            self.nodes[host_id] = ho
            self.holder.add_net_object(ho)
            self.hosts.append(ho)
        connected_iface_counts = defaultdict(lambda: 0)  # type: Dict[str, int]
        self.links = []  # type: List[components.Link]
        for (a, b, lid) in edges:
            link = components.Link("%s--%s" % (a, b), self.scheduler, self.tracer)
            iface_idx_a = connected_iface_counts[a]
            connected_iface_counts[a] += 1
            self.nodes[a].get_ifaces()[iface_idx_a].attach(link)

            iface_idx_b = connected_iface_counts[b]
            connected_iface_counts[b] += 1
            self.nodes[b].get_ifaces()[iface_idx_b].attach(link)

            self.links[lid] = link
            self.holder.add_link(link)
        for sw in self.switches:
            # Initialize the switches
            sw.initialized()
        for ho in self.hosts:
            # Once switches are initialized send host ID messages to inform
            # switches of host connectivity.
            ho.send_host_identification()

    def add_hook_between_events(self, hook: Callable[[], None]):
        """Add a hook function that should be run between link events"""
        self.run_between_failures.append(hook)

    def run(self):
        """Run the scheduling loop to completion"""
        self.scheduler.run()
        if self.scheduler.execution_steps() == 0:
            warnings.warn("No simulation steps executed.")
        for hook in self.run_between_failures:
            hook()
        while len(self.failure_events) > 0:
            (act, link) = self.failure_events.popleft()
            if act == "up":
                self.links[link].set_link_up()
            elif act == "down":
                self.links[link].set_link_down()
            else:
                warning.warn("Unknown event")
            self.scheduler.reset_execution_steps()
            self.scheduler.run()
            if self.scheduler.execution_steps() == 0:
                warnings.warn("No simulation steps executed.")
            if self.scheduler.has_exhausted_limit():
                warnings.warn("Exhausted simulation step limit")
            for hook in self.run_between_failures:
                hook()

    def control_events(self) -> int:
        """When tracing is enabled this function returns the total number of control packets processed in
        the network"""
        if self.tracer is not None:
            return self.tracer.get_total_time()
        else:
            raise Exception("Tracing was not enabled")

    def get_cause_at_time(self, time: int) -> str:
        """When tracing is enabled this function will written a string representation of the control packet
        processed at time `time`"""
        if self.tracer is not None:
            return self.tracer.get_cause_at_time(time)
        else:
            raise Exception("Tracing was not enabled")

    def check_algorithm(self) -> bool:
        """This is a simple function to test whether, at the end of simulation, the algorithm behaved correctly, i.e.,
        all connectivity loops were eliminated and the graph remained connected."""
        return self.holder.check_loop_freedom() and self.holder.check_complete()

    def send_host_ping(self, src_host: str, dest_host: str, ttl: int = 32) -> None:
        """Sends a ping from `src_host` to `dest_host`"""
        s = self.nodes[src_host]
        d = self.nodes[dest_host]
        if isinstance(d, components.Host):
            d_a = d.get_address()
        else:
            raise Exception("%s is not a host" % dest_host)
        if isinstance(s, components.Host):
            s.send(components.to_data_packet(s.get_address(), d_a, "ping", ttl))
        else:
            raise Exception("%s is not a host" % src_host)

    def get_connectivity_matrix(self) -> Dict[str, Dict[components.Address, str]]:
        """Returns a dictionary of dictionaries indicating where packets sent by a host with a given address end up."""
        return self.holder.get_connectivity_matrix()

    def get_distance_matrix(self) -> Dict[str, Dict[components.Address, int]]:
        """Returns a dictionary of dictionaries containing the Path length from each  host to the destination indicated
        by an address. In case the destination is not reached we report a path length of -1"""
        return self.holder.get_distance_matrix()

    def get_paths(self) -> Dict[str, Dict[components.Address, List[str]]]:
        """A dictionary of dictionaries indicating the path traversed by a packet leaving a host and addressed to an
        address. In case of loops we merely include the path upto the point at which a previously visited node is hit"""
        return self.holder.get_paths()

    def physically_connected_components(self) -> List[List[str]]:
        """Returns a list of list indicating the set of hosts in the graph which are physically connected"""
        return self.holder.get_physically_connected_hosts()

    def get_forwarding_graph_for_host(self, host: str) -> nx.MultiDiGraph:
        """Return a networkx digraph with how packets are forwarded starting at the given `host`"""
        return self.holder.get_forwarding_graph_for_host(host)

    @staticmethod
    def from_yml_string(
        s: str, enable_trace: bool, control: Callable[[], components.ControlPlane]
    ) -> SimulationSetup:
        """This is a helper function that constructs a simulation from a topology yml file"""
        obj = yaml.safe_load(s)
        obj["edges"] = map(tuple, obj["edges"])
        obj["enable_trace"] = enable_trace
        obj["control"] = control
        return SimulationSetup(**obj)

    @staticmethod
    def from_yml_file(
        name: str, enable_trace: bool, control: Callable[[], components.ControlPlane]
    ) -> SimulationSetup:
        """Construct a simulation for the file named `name`"""
        with open(name) as f:
            return SimulationSetup.from_yml_string(f.read(), enable_trace, control)
