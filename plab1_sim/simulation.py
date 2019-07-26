from __future__ import annotations
from typing import List, Tuple, Callable, Optional, Dict, Deque, Iterator, Any
from . import components
from collections import defaultdict, deque
import yaml
import warnings
import networkx as nx  # type: ignore


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


class SimulationSetup(object):
    """SimulationSetup objects represent a network simulation environment. They are
    used to both create and set up the network topology, run simulation and analyze
    results"""

    def __init__(
        self,
        hosts: List[str],
        switches: List[str],
        nports: int,
        edges: List[Tuple[str, str]],
        enable_trace: bool,
        control: Callable[[], components.ControlPlane],
        simulataneous_events: int = 1024,
        total_event_budget: int = 10000,
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
        self.switches = []  # type: List[components.DumbSwitch]
        self.nodes = {}  # type: Dict[str, components.NetNode]
        for switch in switches:
            sw = components.DumbSwitch(switch, nports, control(), self.tracer)
            self.nodes[switch] = sw
            self.switches.append(sw)
            self.holder.add_net_object(sw)
        for host in hosts:
            ho = components.Host(host, self.tracer)
            self.nodes[host] = ho
            self.holder.add_net_object(ho)
        connected_port_counts = defaultdict(lambda: 0)  # type: Dict[str, int]
        self.links = []  # type: List[components.Link]
        for (a, b) in edges:
            link = components.Link("%s--%s" % (a, b), self.scheduler, self.tracer)
            port_idx_a = connected_port_counts[a]
            connected_port_counts[a] += 1
            self.nodes[a].get_ports()[port_idx_a].attach(link)

            port_idx_b = connected_port_counts[b]
            connected_port_counts[b] += 1
            self.nodes[b].get_ports()[port_idx_b].attach(link)

            self.links.append(link)
        for sw in self.switches:
            sw.initialized()

    def run(self):
        """Run the scheduling loop to completion"""
        self.scheduler.run()
        if self.scheduler.execution_steps() == 0:
            warnings.warn(
                "No simulation steps executed. Did you send packets during initialization?"
            )

    def cycles_over_time(
        self
    ) -> Iterator[Tuple[str, Optional[List[Tuple[str, str, int]]]]]:
        """Returns an iterator (assuming enable_trace was true) that represents cycles in the topology
        as control packets were processed."""

        def cycles_or_none(g: nx.MultiGraph) -> Optional[List[Tuple[str, str, int]]]:
            try:
                return nx.algorithms.cycles.find_cycle(g)
            except nx.exception.NetworkXNoCycle:
                return None

        if self.tracer is not None:
            return map(lambda ce: (ce[0], cycles_or_none(ce[1])), iter(self.tracer))
        else:
            return iter([])

    def multigraphs_over_time(self) -> Iterator[Tuple[str, nx.MulitGraph]]:
        """When tracing is enable, this returns an iterator that can be used to look at graph evolution
        over time."""
        if self.tracer is not None:
            return iter(self.tracer)
        else:
            return iter([])

    def control_events(self) -> int:
        """When tracing is enabled this function returns the total number of control packets processed in
        the network"""
        if self.tracer is not None:
            return self.tracer.get_total_time()
        else:
            raise Exception("Tracing was not enabled")

    def draw_graph_at_n(self, time: int, axs: Optional[Any] = None) -> None:
        """When tracing is enabled this function will draw (using matplotlib) the network graph after control message
        `n` was processed. We assume that someone has already set up matplotlib's plotting environment."""
        if self.tracer is not None:
            self.tracer.draw_graph_at_n(time, axs=axs)
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
        final_graph = self.holder.create_nx_graph()
        p = False
        cycle = None
        try:
            cycle = nx.algorithms.cycles.find_cycle(final_graph)
            return False
        except nx.exception.NetworkXNoCycle:
            return nx.is_connected(final_graph)

    def send_host_ping(self, host: str, ttl: int = 32) -> None:
        """Sends a ping from a single host"""
        h = self.nodes[host]
        if isinstance(h, components.Host):
            h.send(components.to_data_packet("ping", ttl))
        else:
            raise Exception("%s is not a host" % host)

    @staticmethod
    def from_yml_string(
        s: str, enable_trace: bool, control: Callable[[], components.ControlPlane]
    ) -> SimulationSetup:
        """This is a helper function that can be used to construct a simulation from a
        topology yml file"""
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
