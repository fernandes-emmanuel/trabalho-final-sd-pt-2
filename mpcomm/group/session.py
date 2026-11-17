from __future__ import annotations

import logging
import threading
import time
from collections.abc import Callable, Mapping
from enum import Enum, auto
from typing import Any

from mpcomm.model import Address, TransportStats
from mpcomm.transport.channel import Channel
from mpcomm.transport.messages import DataPacket, Kind, Packet
from mpcomm.transport.reliable import PeriodicTask, ReliableFifoMulticast, RetransmissionPolicy

log = logging.getLogger(__name__)

RETRANSMISSION_TICK = 0.01

DeliverCallback = Callable[[int, Any], None]


class SessionState(Enum):
    CREATED = auto()
    OPEN = auto()
    CLOSING = auto()
    CLOSED = auto()


class SessionClosed(Exception):
    pass


class GroupSession:
    """Group communication session with reliable FIFO multicast between a fixed set of members.

    Opening multicasts HELLO and waits until every member's HELLO is delivered. Closing
    multicasts END and waits until every member's END is delivered and every message sent
    by this member is acknowledged; since delivery is FIFO, receiving a member's END means
    every message it multicast in the session has already been delivered.
    """

    def __init__(
        self,
        *,
        session_id: int,
        self_id: int,
        members: Mapping[int, Address],
        channel: Channel,
        deliver: DeliverCallback,
        policy: RetransmissionPolicy | None = None,
    ) -> None:
        self.session_id = session_id
        self.self_id = self_id
        self._group = frozenset(members)
        self._deliver = deliver
        self._state = SessionState.CREATED
        self._send_lock = threading.Lock()
        self._lock = threading.Lock()
        self._greeted: set[int] = set()
        self._finished: set[int] = set()
        self._everyone_ready = threading.Event()
        self._everyone_finished = threading.Event()
        self._protocol = ReliableFifoMulticast(
            round_id=session_id,
            self_id=self_id,
            members=members,
            channel=channel,
            deliver=self._on_deliver,
            policy=policy,
        )
        self._retransmitter = PeriodicTask(
            RETRANSMISSION_TICK, self._protocol.retransmit_due, f"retransmitter-{session_id}"
        )

    @property
    def size(self) -> int:
        return len(self._group)

    @property
    def state(self) -> SessionState:
        return self._state

    def handle(self, packet: Packet) -> None:
        self._protocol.handle(packet)

    def open(self, timeout: float) -> bool:
        with self._send_lock:
            if self._state is not SessionState.CREATED:
                raise RuntimeError(f"session {self.session_id} was already opened")
            self._retransmitter.start()
            self._protocol.multicast(Kind.HELLO)
        if not self._everyone_ready.wait(timeout):
            log.error("session %s: members did not join in time", self.session_id)
            return False
        with self._send_lock:
            if self._state is SessionState.CREATED:
                self._state = SessionState.OPEN
        return True

    def multicast(self, payload: Any) -> None:
        with self._send_lock:
            if self._state is not SessionState.OPEN:
                raise SessionClosed(f"session {self.session_id} is {self._state.name.lower()}")
            self._protocol.multicast(Kind.DATA, payload)

    def close(self, timeout: float) -> bool:
        with self._send_lock:
            if self._state in (SessionState.CLOSING, SessionState.CLOSED):
                raise RuntimeError(f"session {self.session_id} is already closing")
            self._state = SessionState.CLOSING
            self._protocol.multicast(Kind.END)
        deadline = time.monotonic() + timeout
        finished = self._everyone_finished.wait(timeout)
        acknowledged = self._protocol.wait_until_acknowledged(max(0.0, deadline - time.monotonic()))
        self._retransmitter.stop()
        self._state = SessionState.CLOSED
        if not (finished and acknowledged):
            log.error("session %s: did not close cleanly", self.session_id)
        return finished and acknowledged and not self._protocol.unreachable

    def stats(self, dropped_by_fault_injector: int = 0) -> TransportStats:
        counters = self._protocol.counters
        return TransportStats(
            data_sent=counters.data_sent,
            retransmissions=counters.retransmissions,
            acks_sent=counters.acks_sent,
            duplicates_discarded=counters.duplicates_discarded,
            held_back=counters.held_back,
            dropped_by_fault_injector=dropped_by_fault_injector,
            unreachable_peers=tuple(sorted(self._protocol.unreachable)),
        )

    def _on_deliver(self, packet: DataPacket) -> None:
        match packet.kind:
            case Kind.HELLO:
                with self._lock:
                    self._greeted.add(packet.sender)
                    if self._greeted >= self._group:
                        self._everyone_ready.set()
            case Kind.DATA:
                self._deliver(packet.sender, packet.payload)
            case Kind.END:
                with self._lock:
                    self._finished.add(packet.sender)
                    if self._finished >= self._group:
                        self._everyone_finished.set()
