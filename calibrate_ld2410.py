#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.11"
# dependencies = ["httpx>=0.27"]
# ///
"""Calibrate the HLK-LD2410 radar behind presence_sensor.yaml from a phone.

The device's built-in web server is the whole interface: a REST call to set a
number or flip a switch, and one server-sent events stream to read entity state
back. Per-gate energies only publish while engineering mode is on, so that
switch is forced on for the session and forced off on every exit path: finish,
cancel, failure, and Ctrl-C.
"""

from collections import deque
from collections.abc import Callable
from dataclasses import dataclass
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any
from urllib.parse import urlsplit
import json
import math
import socket
import sys
import threading
import time

import httpx

PORT = 8765
EMPTY_SECONDS = 120
WALK_SECONDS = 60
STILL_SECONDS = 60
GATE_COUNT = 9
# The module has no still detection on the first two gates, so there is nothing
# to calibrate there and writing their still thresholds only wastes a round trip.
STILL_GATE_START = 2
GATE_WIDTH_METRES = 0.75
MIN_GATE = 2
MAX_GATE = 8
ENERGY_OFFSET = 10
TIMEOUT_SECONDS = 10
CONNECT_TIMEOUT_SECONDS = 10.0
WRITE_VERIFY_TIMEOUT_SECONDS = 5.0
MODULE_RESTART_SECONDS = 2.0

PHASE_SETUP = "setup"
PHASE_CONNECTING = "connecting"
PHASE_EMPTY_READY = "empty_ready"
PHASE_EMPTY = "empty"
PHASE_WALK_READY = "walk_ready"
PHASE_WALK = "walk"
PHASE_STILL_READY = "still_ready"
PHASE_STILL = "still"
PHASE_RESULTS = "results"
PHASE_WRITING = "writing"
PHASE_DONE = "done"
PHASE_ERROR = "error"

PHASE_DURATIONS: dict[str, int] = {
    PHASE_EMPTY: EMPTY_SECONDS,
    PHASE_WALK: WALK_SECONDS,
    PHASE_STILL: STILL_SECONDS,
}
PHASE_PREFIXES: dict[str, str] = {PHASE_EMPTY: "empty", PHASE_WALK: "walk", PHASE_STILL: "still"}
# Phases that only make sense while the event stream is up; a stream that has
# ended in one of these must fail the session rather than serve stale state. The
# connecting and writing phases are excluded because their workers own failure:
# the connect worker times out on missing entities and the write worker stops on
# an unconfirmed echo.
LIVE_PHASES = {
    PHASE_EMPTY_READY,
    PHASE_EMPTY,
    PHASE_WALK_READY,
    PHASE_WALK,
    PHASE_STILL_READY,
    PHASE_STILL,
    PHASE_RESULTS,
}


class DeviceError(Exception):
    """The device could not be reached or does not expose the expected entities."""


@dataclass
class Entity:
    entity_id: str
    name: str
    value: float | bool | str | None
    state: str | None


def object_id(entity_id: str) -> str:
    """Strip the domain prefix, because web API URLs use the short object id."""
    return entity_id.split("-", 1)[1]


def normalize_hostname(hostname: str) -> str:
    hostname = hostname.strip().rstrip("/")
    return hostname if hostname.startswith(("http://", "https://")) else f"http://{hostname}"


def propose_threshold(empty_max: float, occupied_max: float) -> tuple[int, str]:
    """Pick a threshold from the empty and occupied energy maxima.

    A fixed offset beats a multiplier here: near gates idle at 40 to 60 still
    energy, so scaling the empty maximum would push the threshold to 100 and the
    radar would never trigger.
    """
    if occupied_max > empty_max + ENERGY_OFFSET:
        return min(100, int(empty_max) + ENERGY_OFFSET), "normal"
    if occupied_max > empty_max:
        return min(100, round((empty_max + occupied_max) / 2)), "tight"
    return min(100, int(empty_max) + ENERGY_OFFSET), "no detection"


class Device:
    """One ESPHome web server: an SSE reader plus a separate control client.

    Control requests get their own httpx client so an in-flight stream never has
    to share a connection with a write.
    """

    def __init__(self, hostname: str, logger: Callable[[str], None]) -> None:
        self.base_url = normalize_hostname(hostname)
        self.logger = logger
        self.connected = False
        self.stopping = False
        self.error: str | None = None
        self.entities: dict[str, Entity] = {}
        self.logged_dumps: set[str] = set()
        self.logged_updates: set[str] = set()
        self.on_state: Callable[[Entity], None] | None = None
        self._entities_lock = threading.Lock()
        self._control_lock = threading.Lock()
        # ESPHome pings the SSE stream, so 90 s without a byte means the stream
        # is dead even though the socket has not errored.
        self._stream_client = httpx.Client(timeout=httpx.Timeout(10.0, read=90.0))
        self._control_client = httpx.Client(timeout=10.0)

    def start(self) -> None:
        threading.Thread(target=self._read_stream, daemon=True).start()

    def _read_stream(self) -> None:
        try:
            with self._stream_client.stream("GET", f"{self.base_url}/events") as response:
                response.raise_for_status()
                self.connected = True
                event_name: str | None = None
                data_lines: list[str] = []
                for line in response.iter_lines():
                    if line == "":
                        if event_name == "state" and data_lines:
                            self._handle_state("\n".join(data_lines))
                        event_name, data_lines = None, []
                    elif line.startswith("event:"):
                        event_name = line[len("event:"):].strip()
                    elif line.startswith("data:"):
                        data_lines.append(line[len("data:"):].strip())
        except Exception as error:
            # A deliberate close also ends the iterator, and logging that as a
            # failure would be a false alarm during shutdown.
            if self.stopping:
                return
            self.error = str(error)
            self.logger(f"stream ended: {error!r}")
        finally:
            # Any end, a clean server close included, means live state has
            # stopped arriving, so the session must notice without an exception.
            if not self.stopping:
                self.connected = False
                if self.error is None:
                    self.error = "device closed the event stream"
                    self.logger(f"stream ended: {self.error}")

    def _handle_state(self, data: str) -> None:
        payload = json.loads(data)
        entity_id = payload["id"]
        domain = entity_id.split("-", 1)[0]
        # The initial dump carries "name", later updates do not, so log the
        # first raw sample of each shape per domain.
        logged = self.logged_dumps if "name" in payload else self.logged_updates
        if domain not in logged:
            logged.add(domain)
            label = " " if "name" in payload else " update "
            self.logger(f"raw SSE{label}{domain}: {data}")
        with self._entities_lock:
            known = self.entities.get(entity_id)
        name = payload.get("name")
        if name is None:
            # The dump carries names but updates do not, so keep the name the
            # entity was first discovered with, or derive it from name_id.
            name = known.name if known is not None else payload["name_id"].split("/", 1)[1]
        value = payload.get("value")
        if domain == "number" and isinstance(value, str):
            # Numbers arrive as strings ({"value":"50"}), switches send bool,
            # sensors send a number or null, and text sensors send free text
            # such as the firmware version, which must stay a string.
            value = float(value)
        entity = Entity(
            entity_id=entity_id,
            name=name,
            value=value,
            state=payload.get("state"),
        )
        with self._entities_lock:
            self.entities[entity.entity_id] = entity
        if self.on_state is not None:
            self.on_state(entity)

    def _control_request(self, method: str, path: str) -> None:
        with self._control_lock:
            response = self._control_client.request(method, f"{self.base_url}{path}")
        response.raise_for_status()

    def switch(self, entity_id: str, on: bool) -> None:
        action = "turn_on" if on else "turn_off"
        self._control_request("POST", f"/switch/{object_id(entity_id)}/{action}")

    def set_number(self, entity_id: str, value: int) -> None:
        self.logger(f"set number {object_id(entity_id)} = {value}")
        self._control_request("POST", f"/number/{object_id(entity_id)}/set?value={value}")

    def get_entity(self, entity_id: str) -> Entity | None:
        with self._entities_lock:
            return self.entities.get(entity_id)

    def get_value(self, entity_id: str) -> float | None:
        entity = self.get_entity(entity_id)
        return entity.value if entity is not None else None

    def wait_for_entity(self, suffix: str, timeout: float) -> Entity:
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            # Entity names carry the room title ("Office Gate 3 Move Energy"),
            # so match on the suffix and never assume the room.
            with self._entities_lock:
                for entity in self.entities.values():
                    if entity.name.endswith(suffix):
                        return entity
            # A dead stream can never deliver the entity, so report the real
            # cause now instead of a misleading "not found" ten seconds later.
            if self.error is not None:
                raise DeviceError(f"event stream ended: {self.error}")
            time.sleep(0.1)
        raise DeviceError(f"no entity name ending in '{suffix}'")

    def close(self) -> None:
        self.stopping = True
        self._stream_client.close()
        self._control_client.close()


class Calibration:
    """Session state machine plus the measurement and write logic."""

    GATE_KEYS = (
        ("move_energy", "Move Energy"),
        ("still_energy", "Still Energy"),
        ("move_threshold", "Move Threshold"),
        ("still_threshold", "Still Threshold"),
    )

    def __init__(self) -> None:
        self.lock = threading.Lock()
        self.phase = PHASE_SETUP
        self.phase_started_at: float | None = None
        self.error_message: str | None = None
        self.message: str | None = None
        self.room_length_metres = 0.0
        self.max_gate = MIN_GATE
        self.device: Device | None = None
        self.cleaned_up = False
        self.gate_maxima: dict[str, float] = {}
        self.results: list[dict[str, Any]] = []
        self.results_by_gate: dict[int, dict[str, Any]] = {}
        self.writes: list[dict[str, Any]] = []
        self.write_index: dict[str, dict[str, Any]] = {}
        self.energy_roles: dict[str, tuple[str, int]] = {}
        self.gate_ids: dict[str, dict[int, str]] = {key: {} for key, _ in self.GATE_KEYS}
        self.engineering_switch_id: str | None = None
        self.timeout_id: str | None = None
        self.max_move_id: str | None = None
        self.max_still_id: str | None = None
        self.log_lines: deque[str] = deque(maxlen=200)

    def log(self, message: str) -> None:
        # deque.append is atomic in CPython and log() must stay callable while
        # self.lock is held, so it deliberately takes no lock of its own.
        line = f"{time.strftime('%H:%M:%S')} {message}"
        self.log_lines.append(line)
        print(line, file=sys.stderr)

    def _log_entity(self, entity: Entity) -> None:
        self.log(f"{entity.name} -> {entity.entity_id}")

    def connect(self, hostname: str, room_length_metres: float) -> None:
        with self.lock:
            if self.phase not in (PHASE_SETUP, PHASE_ERROR, PHASE_DONE):
                return
            # A session that reached done still owns a device (and, after a
            # connection failure, possibly a half-open one), so close it before
            # replacing it with the new session's device.
            self._close_device()
            self.room_length_metres = room_length_metres
            self.max_gate = max(MIN_GATE, min(MAX_GATE, math.ceil(room_length_metres / GATE_WIDTH_METRES)))
            self.error_message = self.message = None
            self.cleaned_up = False
            self.engineering_switch_id = None
            self.gate_maxima = {}
            self.results = []
            self.results_by_gate = {}
            self.writes = []
            self.write_index = {}
            self.energy_roles = {}
            self.gate_ids = {key: {} for key, _ in self.GATE_KEYS}
            self.phase = PHASE_CONNECTING
            self.phase_started_at = None
        self.device = Device(hostname, self.log)
        self.device.on_state = self.record_state
        threading.Thread(target=self._connect_worker, daemon=True).start()

    def _connect_worker(self) -> None:
        try:
            self.log(f"connecting to {self.device.base_url}")
            self.device.start()
            switch = self.device.wait_for_entity("Engineering Mode", CONNECT_TIMEOUT_SECONDS)
            self.engineering_switch_id = switch.entity_id
            self._log_entity(switch)
            self._discover_entities()
            self.device.switch(self.engineering_switch_id, True)
            self._wait_for_engineering_on()
        except Exception as error:
            # A failed connect has to show on the page, and _fail logs the repr
            # for anyone debugging the device side.
            self._fail(f"Could not connect to {self.device.base_url}: {error!r}")
            return
        self.set_phase(PHASE_EMPTY_READY)

    def _discover_entities(self) -> None:
        device = self.device
        timeout = device.wait_for_entity("Timeout", CONNECT_TIMEOUT_SECONDS)
        self.timeout_id = timeout.entity_id
        self._log_entity(timeout)
        max_move = device.wait_for_entity("Max Move Distance Gate", CONNECT_TIMEOUT_SECONDS)
        self.max_move_id = max_move.entity_id
        self._log_entity(max_move)
        max_still = device.wait_for_entity("Max Still Distance Gate", CONNECT_TIMEOUT_SECONDS)
        self.max_still_id = max_still.entity_id
        self._log_entity(max_still)
        gate_ids: dict[str, dict[int, str]] = {key: {} for key, _ in self.GATE_KEYS}
        for gate in range(GATE_COUNT):
            for key, suffix in self.GATE_KEYS:
                entity = device.wait_for_entity(f"Gate {gate} {suffix}", CONNECT_TIMEOUT_SECONDS)
                gate_ids[key][gate] = entity.entity_id
                self._log_entity(entity)
        energy_roles: dict[str, tuple[str, int]] = {}
        for gate in range(GATE_COUNT):
            energy_roles[gate_ids["move_energy"][gate]] = ("move", gate)
            energy_roles[gate_ids["still_energy"][gate]] = ("still", gate)
        # Assign only once every lookup succeeded, so build_status never reads a
        # half-populated map and raises while discovery is still running.
        self.gate_ids = gate_ids
        self.energy_roles = energy_roles

    def _wait_for_engineering_on(self) -> None:
        deadline = time.monotonic() + WRITE_VERIFY_TIMEOUT_SECONDS
        while time.monotonic() < deadline:
            entity = self.device.get_entity(self.engineering_switch_id)
            if entity is not None and (entity.value == 1 or (entity.state or "").upper() in ("ON", "TRUE")):
                self.log(f"engineering mode on confirmed: {entity.value}")
                return
            time.sleep(0.2)
        raise DeviceError("engineering mode did not turn on, so per-gate energies would not update")

    def _fail(self, message: str) -> None:
        self.log(f"fail: {message}")
        # Clean up before publishing PHASE_ERROR: once the phase is error the
        # page can offer Start again, and a connect that lands meanwhile swaps in
        # a new device that a later _close_device would wrongly close.
        turned_off = self._turn_engineering_off()
        self._close_device()
        if not turned_off:
            message += " Could not turn engineering mode off. Turn it off in the device web UI."
        with self.lock:
            self.error_message = message
            self.phase = PHASE_ERROR
            self.phase_started_at = None

    def _close_device(self) -> None:
        if self.device is not None and not self.cleaned_up:
            self.device.close()
        self.cleaned_up = True

    def record_state(self, entity: Entity) -> None:
        """Track the peak energy each gate sees during the active phase."""
        role = self.energy_roles.get(entity.entity_id)
        if role is None or entity.value is None:
            return
        energy_type, gate = role
        with self.lock:
            prefix = PHASE_PREFIXES.get(self.phase)
            started = self.phase_started_at
            duration = PHASE_DURATIONS.get(self.phase)
            if prefix is None or started is None or duration is None:
                return
            # A phase only ends when the page polls /status, so a phone that
            # locks mid-measurement leaves the phase nominally active; without
            # this cutoff the SSE thread would keep folding later samples into
            # that phase's maxima.
            if time.monotonic() - started >= duration:
                return
            key = f"{prefix}_{energy_type}_{gate}"
            if entity.value > self.gate_maxima.get(key, -1.0):
                self.gate_maxima[key] = entity.value

    def start_phase(self) -> None:
        # ESPHome's per-gate energy sensors only publish when the value changes,
        # so a gate that sits flat for the whole phase (still gates 0 and 1 are
        # always 0, the module has no still detection there) never sends a
        # sample during the phase. Its last published value is still its current
        # value, so seed the phase's maxima from it before any live sample lands.
        # Seeding happens under the lock: record_state drops samples while the
        # phase is still "ready", so a value read before the lock could be
        # overtaken by an update that is then lost for the whole phase.
        # Read and change the phase in one critical section so two quick taps
        # cannot both start a phase or start the wrong one.
        with self.lock:
            next_phase = {
                PHASE_EMPTY_READY: PHASE_EMPTY,
                PHASE_WALK_READY: PHASE_WALK,
                PHASE_STILL_READY: PHASE_STILL,
            }.get(self.phase)
            if next_phase is None:
                return
            prefix = PHASE_PREFIXES[next_phase]
            for entity_id, (energy_type, gate) in self.energy_roles.items():
                value = self.device.get_value(entity_id)
                if value is not None:
                    self.gate_maxima[f"{prefix}_{energy_type}_{gate}"] = value
            self.phase = next_phase
            self.phase_started_at = time.monotonic()
            self.log(f"phase {next_phase} start")

    def advance_phase(self) -> None:
        device = self.device
        compute_still = False
        failure: str | None = None
        # Check and change the phase in one critical section, so two concurrent
        # /status polls cannot both act on the same elapsed phase.
        with self.lock:
            if (
                device is not None
                and not device.connected
                and device.error is not None
                and self.phase in LIVE_PHASES
            ):
                # Early in connecting the stream may not be up yet with no error
                # recorded, so only a real stream failure ends the session.
                failure = f"Lost connection to the device: {device.error}"
            else:
                started = self.phase_started_at
                duration = PHASE_DURATIONS.get(self.phase)
                if duration is not None and started is not None and time.monotonic() - started >= duration:
                    prefix = PHASE_PREFIXES[self.phase]
                    missing = [
                        f"{energy_type} gate {gate}"
                        for gate in range(GATE_COUNT)
                        for energy_type in ("move", "still")
                        if f"{prefix}_{energy_type}_{gate}" not in self.gate_maxima
                    ]
                    if missing:
                        failure = f"{self.phase} phase ended without samples for {', '.join(missing)}"
                    elif self.phase == PHASE_EMPTY:
                        self.log(f"phase {PHASE_EMPTY} end: {self._maxima_line(prefix)}")
                        self.phase = PHASE_WALK_READY
                        self.phase_started_at = None
                    elif self.phase == PHASE_WALK:
                        self.log(f"phase {PHASE_WALK} end: {self._maxima_line(prefix)}")
                        self.phase = PHASE_STILL_READY
                        self.phase_started_at = None
                    elif self.phase == PHASE_STILL:
                        self.log(f"phase {PHASE_STILL} end: {self._maxima_line(prefix)}")
                        compute_still = True
        if failure is not None:
            # Clean up before publishing the error so a Start-again click that
            # lands during cleanup cannot have its new device closed by this one.
            turned_off = self._turn_engineering_off()
            self._close_device()
            if not turned_off:
                failure += " Could not turn engineering mode off. Turn it off in the device web UI."
            with self.lock:
                self.error_message = failure
                self.phase = PHASE_ERROR
                self.phase_started_at = None
            return
        if compute_still:
            # Building rows queries the device, which must not happen under the
            # lock; re-check the phase afterwards so a disconnect or reset that
            # landed meanwhile cannot stash results into a session that moved on.
            results = self.compute_results()
            with self.lock:
                if self.phase == PHASE_STILL:
                    self.results = results
                    self.results_by_gate = {row["gate"]: row for row in results}
                    self.phase = PHASE_RESULTS
                    self.phase_started_at = None

    def set_phase(self, phase: str, message: str | None = None) -> None:
        with self.lock:
            self.phase = phase
            self.phase_started_at = None
            if message is not None:
                self.message = message

    def _maxima_line(self, prefix: str) -> str:
        return " ".join(
            f"{energy_type[0]}{gate}={self.gate_maxima.get(f'{prefix}_{energy_type}_{gate}', '?')}"
            for gate in range(GATE_COUNT)
            for energy_type in ("move", "still")
        )

    def compute_results(self) -> list[dict[str, Any]]:
        with self.lock:
            maxima, max_gate = dict(self.gate_maxima), self.max_gate
        results: list[dict[str, Any]] = []
        for gate in range(GATE_COUNT):
            row: dict[str, Any] = {
                "gate": gate,
                "beyond": gate > max_gate,
                "move": self._threshold_row("move", gate, maxima),
                "still": self._threshold_row("still", gate, maxima) if gate >= STILL_GATE_START else None,
            }
            results.append(row)
        return results

    def _threshold_row(self, energy_type: str, gate: int, maxima: dict[str, float]) -> dict[str, Any]:
        # advance_phase refuses to build results unless every one of these keys
        # was sampled, so a missing key here is a bug and must not read as 0.
        empty_max = maxima[f"empty_{energy_type}_{gate}"]
        occupied_prefix = "walk" if energy_type == "move" else "still"
        occupied_max = maxima[f"{occupied_prefix}_{energy_type}_{gate}"]
        proposed, status = propose_threshold(empty_max, occupied_max)
        current = self.device.get_value(self.gate_ids[f"{energy_type}_threshold"][gate])
        return {
            "current": current,
            "empty_max": empty_max,
            "occupied_max": occupied_max,
            "proposed": proposed,
            "status": status,
        }

    def start_writing(self) -> None:
        with self.lock:
            if self.phase != PHASE_RESULTS:
                return
            self.writes = []
            self.write_index = {}
            for gate in range(GATE_COUNT):
                self._add_write(
                    f"Gate {gate} move threshold",
                    self.gate_ids["move_threshold"][gate],
                    self.results_by_gate[gate]["move"]["proposed"],
                )
                if gate >= STILL_GATE_START:
                    self._add_write(
                        f"Gate {gate} still threshold",
                        self.gate_ids["still_threshold"][gate],
                        self.results_by_gate[gate]["still"]["proposed"],
                    )
            self._add_write("Timeout", self.timeout_id, TIMEOUT_SECONDS)
            self._add_write("Max move distance gate", self.max_move_id, self.max_gate)
            self._add_write("Max still distance gate", self.max_still_id, self.max_gate)
            self.phase = PHASE_WRITING
            self.phase_started_at = None
        threading.Thread(target=self._write_all, daemon=True).start()

    def _add_write(self, label: str, entity_id: str, target: int) -> None:
        record: dict[str, Any] = {"label": label, "target": target, "actual": None, "verified": None}
        self.writes.append(record)
        self.write_index[entity_id] = record

    def _write_all(self) -> None:
        try:
            for gate in range(GATE_COUNT):
                # ESPHome sends a gate's move and still thresholds together using
                # the current number states, so confirm each echo before the next
                # write changes the state the module reads.
                move_id = self.gate_ids["move_threshold"][gate]
                self.device.set_number(move_id, self.results_by_gate[gate]["move"]["proposed"])
                if not self._wait_for_write(move_id):
                    raise DeviceError(f"{self.write_index[move_id]['label']} was not confirmed by the device")
                if gate >= STILL_GATE_START:
                    still_id = self.gate_ids["still_threshold"][gate]
                    self.device.set_number(still_id, self.results_by_gate[gate]["still"]["proposed"])
                    if not self._wait_for_write(still_id):
                        raise DeviceError(f"{self.write_index[still_id]['label']} was not confirmed by the device")
            for entity_id, target in (
                (self.timeout_id, TIMEOUT_SECONDS),
                (self.max_move_id, self.max_gate),
                (self.max_still_id, self.max_gate),
            ):
                self.device.set_number(entity_id, target)
                if not self._wait_for_write(entity_id):
                    raise DeviceError(f"{self.write_index[entity_id]['label']} was not confirmed by the device")
                # ESPHome's set_max_distances_timeout echoes the value at once,
                # then restarts the module 200 ms later and re-reads every
                # parameter about 1 s after that. A write sent during the restart
                # is lost and the re-read publishes the module's old value, which
                # is how max still gate stayed at 8 while max move landed at 7.
                # So wait out the restart before the next write, then confirm
                # the re-read did not revert this one.
                time.sleep(MODULE_RESTART_SECONDS)
                if not self._wait_for_write(entity_id):
                    raise DeviceError(f"{self.write_index[entity_id]['label']} was reverted by the module after restart")
            message = "Calibration written."
        except Exception as error:
            self.log(f"write failed: {error!r}")
            message = f"Write failed: {error}"
        finally:
            turned_off = self._turn_engineering_off()
            self._close_device()
            if turned_off:
                message += " Engineering mode is off."
            else:
                message += " Could not turn engineering mode off. Turn it off in the device web UI."
            # Phase and message are published together, so no poll can observe a
            # done phase with the previous phase's message.
            self.set_phase(PHASE_DONE, message)

    def _wait_for_write(self, entity_id: str) -> bool:
        record = self.write_index[entity_id]
        deadline = time.monotonic() + WRITE_VERIFY_TIMEOUT_SECONDS
        while True:
            value = self.device.get_value(entity_id)
            with self.lock:
                record["actual"] = value
                if value is not None and int(round(value)) == record["target"]:
                    record["verified"] = True
                    self.log(f"echo {object_id(entity_id)}: verified actual={value}")
                    return True
            if time.monotonic() >= deadline:
                with self.lock:
                    record["verified"] = False
                self.log(f"echo {object_id(entity_id)}: not confirmed actual={value}")
                return False
            time.sleep(0.2)

    def _turn_engineering_off(self) -> bool:
        if self.device is None or self.engineering_switch_id is None or self.cleaned_up:
            return True
        try:
            self.device.switch(self.engineering_switch_id, False)
        except Exception as error:
            # Cleanup must not stop the session reaching a done state, but a
            # failure here is still worth a line in the log.
            self.log(f"engineering mode off failed: {error!r}")
            return False
        self.log("engineering mode off: ok")
        return True

    def cancel(self) -> None:
        # Cancelling during connecting or writing would race the worker that
        # owns the device, so the request is refused and the page hides the
        # button for those phases.
        with self.lock:
            if self.phase in (PHASE_CONNECTING, PHASE_WRITING):
                return
            self.phase = PHASE_DONE
            self.phase_started_at = None
        turned_off = self._turn_engineering_off()
        self._close_device()
        if turned_off:
            message = "Cancelled. Engineering mode is off."
        else:
            message = "Cancelled. Could not turn engineering mode off. Turn it off in the device web UI."
        with self.lock:
            self.message = message

    def shutdown(self) -> None:
        self._turn_engineering_off()
        self._close_device()

    def build_status(self) -> dict[str, Any]:
        self.advance_phase()
        device = self.device
        with self.lock:
            duration = PHASE_DURATIONS.get(self.phase)
            started = self.phase_started_at
            elapsed = min(duration, time.monotonic() - started) if duration and started else 0.0
            status = {
                "phase": self.phase,
                "connected": device.connected if device is not None else False,
                "error": self.error_message,
                "message": self.message,
                "room_length_metres": self.room_length_metres,
                "max_gate": self.max_gate,
                "timeout_seconds": TIMEOUT_SECONDS,
                "phase_duration_seconds": duration,
                "phase_elapsed_seconds": elapsed,
                "phase_remaining_seconds": max(0.0, duration - elapsed) if duration else 0.0,
                "gates": [],
                "results": self.results,
                "writes": [dict(record) for record in self.writes],
                "verified_count": sum(1 for record in self.writes if record["verified"] is True),
                "write_count": len(self.writes),
                "log": list(self.log_lines),
            }
        # Discovery assigns gate_ids in one go, but guard completeness anyway so
        # a poll that races it can never raise a KeyError mid-discovery.
        gate_ids = self.gate_ids
        if device is not None and all(len(gate_ids[key]) == GATE_COUNT for key, _ in self.GATE_KEYS):
            status["gates"] = [
                {
                    "gate": gate,
                    **{key: device.get_value(gate_ids[key][gate]) for key, _ in self.GATE_KEYS},
                }
                for gate in range(GATE_COUNT)
            ]
        return status


PAGE = """<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>LD2410 calibration</title>
<!-- An empty data URI stops the browser asking for /favicon.ico, which the
     server has no answer for. Declaring it here rather than adding a route
     means there is no request at all, and _send_bytes keeps its single
     hardcoded 200 status instead of growing a 204 case for one icon. -->
<link rel="icon" href="data:,">
<style>
:root { color-scheme: dark; }
* { box-sizing: border-box; }
body { margin: 0; font-family: system-ui, sans-serif; background: #111; color: #eee; font-size: 18px; }
main { max-width: 720px; margin: 0 auto; padding: 12px 12px 40px; }
h1 { font-size: 1.25rem; } h2 { font-size: 1.05rem; margin: 0 0 8px; }
section, details { margin-top: 16px; } .hidden { display: none; }
label { display: block; margin-top: 12px; }
input { width: 100%; font-size: 1.1rem; padding: 12px; border-radius: 8px; border: 1px solid #555; background: #1c1c1c; color: #eee; }
button { font-size: 1.15rem; padding: 15px 18px; border-radius: 10px; border: 0; background: #2f6df6; color: #fff; width: 100%; margin-top: 10px; }
button.secondary { background: #444; } button:disabled { opacity: .5; }
.muted { color: #aaa; font-size: .9rem; } .error { color: #ff6b6b; }
.gate { border: 1px solid #333; border-radius: 10px; padding: 8px 10px; margin-top: 8px; }
.bar { height: 16px; background: #333; border-radius: 8px; overflow: hidden; margin: 2px 0 6px; }
.bar span { display: block; height: 100%; background: #2f6df6; }
.gates-live { border: 1px solid #333; border-radius: 10px; padding: 6px 8px; }
.legend { display: flex; align-items: center; height: 22px; color: #aaa; font-size: .75rem; }
.legend-track { flex: 1; display: flex; }
.legend-half { width: 50%; display: flex; align-items: center; gap: 6px; }
.legend-half.still { justify-content: flex-end; }
.swatch { width: 12px; height: 12px; border-radius: 3px; }
.swatch.move { background: #2f6df6; }
.swatch.still { background: #9c5bd6; }
.gate.live { display: flex; align-items: stretch; height: 36px; margin: 0; padding: 0; border: 0; border-radius: 0; }
.gate.live.beyond { opacity: .45; }
.gate-info { flex: none; width: 2.5rem; padding-right: 6px; display: flex; flex-direction: column; justify-content: center; gap: 1px; }
.gate-num { font-size: .75rem; line-height: 1; color: #aaa; }
.gate-dist { font-size: .65rem; line-height: 1; color: #666; }
.track { position: relative; flex: 1; height: 36px; }
.rail { position: absolute; left: 0; right: 0; bottom: 0; height: 24px; background: #2a2a2a; border-radius: 4px; }
.fill { position: absolute; top: 0; bottom: 0; }
.fill.move { right: 50%; background: #2f6df6; border-radius: 4px 0 0 4px; }
.fill.still { left: 50%; background: #9c5bd6; border-radius: 0 4px 4px 0; }
.fill.blank { left: 50%; width: 50%; background: #242424; border-radius: 0 4px 4px 0; }
.centre { position: absolute; left: 50%; top: 0; bottom: 0; width: 1px; background: #ddd; transform: translateX(-50%); }
.tick { position: absolute; top: 0; height: 11px; width: 2px; background: #ffd166; transform: translateX(-50%); }
.lbl { position: absolute; font-size: .75rem; line-height: 1; white-space: nowrap; pointer-events: none; }
.lbl.energy { bottom: 0; color: #fff; }
.lbl.thr { top: 0; color: #ffd166; }
.verified { color: #4caf50; } .failed { color: #ff6b6b; }
table { width: 100%; border-collapse: collapse; } th, td { text-align: left; padding: 4px 6px; border-bottom: 1px solid #333; font-size: .95rem; }
pre { margin: 6px 0 0; max-height: 40vh; overflow: auto; white-space: pre-wrap; word-break: break-all; font-size: .72rem; font-family: ui-monospace, monospace; }
</style>
</head>
<body>
<main>
<h1>LD2410 calibration</h1>
<section id="instructions"></section>
<section id="setup">
  <p class="muted">Tune the radar on the ESPHome device from your phone.</p>
  <label for="hostname">Sensor hostname</label>
  <input id="hostname" placeholder="presence-sensor-office.local" autocapitalize="off" autocorrect="off" spellcheck="false">
  <label for="roomLength">Room length, sensor to far wall (m)</label>
  <input id="roomLength" type="number" inputmode="decimal" step="0.1" value="4">
  <button id="connectButton">Connect</button>
  <p id="setupError" class="error"></p>
</section>
<section id="progress" class="hidden"><div class="bar"><span id="progressBar"></span></div><p id="progressText" class="muted"></p></section>
<section id="gates"></section>
<details id="gatesHelp" class="hidden">
  <summary>How to read this</summary>
  <p class="muted">A person is detected in a gate when its energy is above that gate's threshold, shown as the yellow tick. Empty-room noise should stay below the ticks; a person should push the bar past them. Dimmed gates are past your room length; writing the calibration tells the module to ignore them.</p>
</details>
<section id="results"></section>
<section id="writes"></section>
<section id="actions">
  <button id="startButton" class="hidden">Start</button>
  <button id="writeButton" class="hidden">Write to radar</button>
  <button id="cancelButton" class="secondary hidden">Cancel</button>
</section>
<details>
  <summary>Log</summary>
  <button id="copyLogButton" class="secondary">Copy log</button>
  <pre id="logView"></pre>
</details>
</main>
<script>
const instructions = {
  connecting: "Connecting to the device...",
  empty_ready: "Leave the room. Leave fans and devices as they normally are. Press Start from outside.",
  empty: "Measuring the empty room. Stay out.",
  walk_ready: "Press Start, then walk slowly from the sensor to the far wall and back for 60 s, so every distance gets covered.",
  walk: "Keep walking slowly between the sensor and the far wall.",
  still_ready: "Sit where you usually sit. Press Start, then do not move for 60 s.",
  still: "Stay still.",
  results: "Review the proposed thresholds, then write them.",
  writing: "Writing to the radar. Waiting for the device to echo each value.",
  done: "Done.", error: "Connection failed."
};
const statusWords = {
  normal: "normal",
  tight: "tight: a person here is only just above noise.",
  "no detection": "no person seen here."
};
const byId = (id) => document.getElementById(id);
const show = (id, on) => byId(id).classList.toggle("hidden", !on);
const post = (path, body) => fetch(path, { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify(body || {}) });
// Energies and thresholds are 0..100 of one half of the track, so halving them
// turns them into a percentage of the whole track.
const halfPercent = (value) => Math.max(0, Math.min(100, Number(value) || 0)) / 2;
// A gate covers one 0.75 m band, so gate 0 ends at 0.8 m and gate 3 at 3.0 m.
const gateMetres = (gate) => `${((gate + 1) * 0.75).toFixed(1)} m`;

function gateRow(gate, maxGate) {
  const moveWidth = halfPercent(gate.move_energy);
  const stillWidth = halfPercent(gate.still_energy);
  const moveTick = halfPercent(gate.move_threshold);
  const stillTick = halfPercent(gate.still_threshold);
  // The module has no still detection on the first two gates, so their right
  // half is a plain darker panel with no fill, tick or labels.
  const hasStill = gate.gate >= 2;
  // A fill starts at the centre and covers its half, so moveEnd and stillEnd are
  // the outer tips of the move and still bars.
  const moveEnd = 50 - moveWidth;
  const stillEnd = 50 + stillWidth;
  // A short fill has no room for the number inside it, so the number sits just
  // outside its tip instead of always hugging the centre.
  const moveEnergyStyle = moveWidth >= 12
    ? `left:calc(${moveEnd}% + 4px)`
    : `left:calc(${moveEnd}% - 4px);transform:translateX(-100%)`;
  const stillEnergyStyle = stillWidth >= 12
    ? `left:calc(${stillEnd}% - 4px);transform:translateX(-100%)`
    : `left:calc(${stillEnd}% + 4px)`;
  const stillParts = hasStill
    ? `<span class="fill still" style="width:${stillWidth}%;left:50%"></span>` +
      `<span class="tick still" style="left:calc(50% + ${stillTick}%)"></span>` +
      `<span class="lbl energy still" style="${stillEnergyStyle}">${gate.still_energy ?? "-"}</span>`
    : `<span class="fill blank"></span>`;
  // Threshold numbers sit in the lane above the rail, anchored on the outer
  // side of their tick so the move and still labels never meet at the centre
  // when both thresholds are low. Keeping them out of the rail also stops them
  // colliding with the fill and the tick.
  const stillThreshold = hasStill
    ? `<span class="lbl thr still" style="left:calc(50% + ${stillTick}% + 3px)">${gate.still_threshold ?? "-"}</span>`
    : "";
  return (
    `<div class="gate live${gate.gate > maxGate ? " beyond" : ""}">` +
    `<span class="gate-info"><span class="gate-num">${gate.gate}</span><span class="gate-dist">${gateMetres(gate.gate)}</span></span>` +
    `<div class="track">` +
    `<span class="lbl thr move" style="left:calc(50% - ${moveTick}% - 3px);transform:translateX(-100%)">${gate.move_threshold ?? "-"}</span>` +
    stillThreshold +
    `<div class="rail">` +
    `<span class="fill move" style="width:${moveWidth}%;right:50%"></span>` +
    stillParts +
    `<span class="centre"></span>` +
    `<span class="tick move" style="left:calc(50% - ${moveTick}%)"></span>` +
    `<span class="lbl energy move" style="${moveEnergyStyle}">${gate.move_energy ?? "-"}</span>` +
    `</div></div></div>`
  );
}

function renderGates(status) {
  // The help block lives outside the section that is re-rendered every poll,
  // so opening it does not get undone a second later.
  const live = ["empty_ready", "empty", "walk_ready", "walk", "still_ready", "still"].includes(status.phase);
  show("gatesHelp", live);
  if (!live) { byId("gates").innerHTML = ""; return; }
  // The legend is part of the re-rendered block, but it holds no interactive
  // elements so a rebuild every second is invisible to the user.
  byId("gates").innerHTML = `<h2>Current thresholds and live energies</h2><div class="gates-live">` +
    `<div class="legend"><span class="gate-info"></span><div class="legend-track">` +
    `<span class="legend-half move"><span class="swatch move"></span>move</span>` +
    `<span class="legend-half still"><span class="swatch still"></span>still</span>` +
    `</div></div>` +
    status.gates.map((gate) => gateRow(gate, status.max_gate)).join("") + "</div>";
}

function thresholdRow(name, data) {
  if (!data) { return ""; }
  const word = statusWords[data.status] || data.status;
  return `<div class="muted">${name}: now ${data.current} - propose <strong>${data.proposed}</strong> (empty ${data.empty_max}, occupied ${data.occupied_max}) - ${word}</div>`;
}

function renderResults(status) {
  if (!["results", "writing"].includes(status.phase) || !status.results.length) {
    byId("results").innerHTML = ""; return;
  }
  const gates = status.results.map((row) =>
    `<div class="gate"><strong>Gate ${row.gate}</strong>${row.beyond ? ' <span class="muted">beyond room, ignored by module</span>' : ""}${thresholdRow("move", row.move)}${thresholdRow("still", row.still)}</div>`
  ).join("");
  byId("results").innerHTML = "<h2>Proposed thresholds</h2>" + gates +
    `<p class="muted">Timeout: ${status.timeout_seconds} s. Max gate: ${status.max_gate} (room ${status.room_length_metres} m / 0.75 m per gate, clamped to 2-8).</p>
     <p class="muted">Write sends each proposed value to the radar. The radar stores them in its own flash, so reflashing the ESP does not undo them. The Factory Reset button in the device web UI restores the defaults.</p>`;
}

function renderWrites(status) {
  if (!["writing", "done", "error"].includes(status.phase) || !status.writes.length) { byId("writes").innerHTML = ""; return; }
  const rows = status.writes.map((write) => {
    const mark = write.verified === true ? '<span class="verified">yes</span>' : write.verified === false ? '<span class="failed">no</span>' : "...";
    return `<tr><td>${write.label}</td><td>${write.target}</td><td>${mark}</td></tr>`;
  }).join("");
  byId("writes").innerHTML = `<h2>Writes</h2><table><tr><th>Setting</th><th>Value</th><th>Verified</th></tr>${rows}</table>`;
}

function render(status) {
  // Done keeps the setup form so the user can recalibrate without restarting
  // the script; it sits below the done message and starts a fresh session.
  const connectVisible = ["setup", "error", "done"].includes(status.phase);
  show("setup", connectVisible);
  // Only the first poll may auto-connect, and only if no session is running,
  // so reloading the page mid-calibration does not restart it.
  if (autoConnect) { autoConnect = false; if (connectVisible) { connect(); } }
  byId("connectButton").textContent = status.phase === "done" ? "Start again" : "Connect";
  byId("setupError").textContent = status.phase === "error" ? status.error || "" : "";
  byId("instructions").innerHTML = `<p>${status.phase === "done" && status.message ? status.message : instructions[status.phase] || ""}</p>`;
  const hasDuration = status.phase_duration_seconds !== null;
  show("progress", hasDuration);
  if (hasDuration) {
    byId("progressBar").style.width = `${Math.min(100, 100 * status.phase_elapsed_seconds / status.phase_duration_seconds)}%`;
    byId("progressText").textContent = `${Math.ceil(status.phase_remaining_seconds)} s left`;
  }
  renderGates(status); renderResults(status); renderWrites(status);
  byId("logView").textContent = (status.log || []).join("\\n");
  show("startButton", ["empty_ready", "walk_ready", "still_ready"].includes(status.phase));
  show("writeButton", status.phase === "results");
  show("cancelButton", !["setup", "connecting", "writing", "done", "error"].includes(status.phase));
  if (status.phase !== "writing") { byId("writeButton").disabled = false; }
}

async function poll() {
  const response = await fetch("/status", { cache: "no-store" });
  render(await response.json());
}

async function connect() {
  const hostname = byId("hostname").value, roomLength = byId("roomLength").value;
  byId("connectButton").disabled = true;
  // Put the form values in the URL so a reload or bookmark reconnects to the
  // same device without retyping; replaceState avoids a reload and a second
  // connect racing the one below.
  history.replaceState(null, "", `?address=${encodeURIComponent(hostname)}&length=${encodeURIComponent(roomLength)}`);
  await post("/connect", { hostname, room_length_metres: parseFloat(roomLength) });
  byId("connectButton").disabled = false;
}
const params = new URLSearchParams(location.search);
if (params.has("address")) { byId("hostname").value = params.get("address"); }
if (params.has("length")) { byId("roomLength").value = params.get("length"); }
let autoConnect = params.has("address");
byId("connectButton").onclick = connect;
byId("startButton").onclick = () => post("/start");
byId("cancelButton").onclick = () => post("/cancel");
byId("writeButton").onclick = () => { byId("writeButton").disabled = true; return post("/write"); };
byId("copyLogButton").onclick = async () => {
  await navigator.clipboard.writeText(byId("logView").textContent);
  byId("copyLogButton").textContent = "Copied";
  setTimeout(() => { byId("copyLogButton").textContent = "Copy log"; }, 1200);
};

poll();
setInterval(poll, 1000);
</script>
</body>
</html>
"""

class RequestHandler(BaseHTTPRequestHandler):
    """Serve the page and the small JSON API the page drives."""

    app: Calibration

    def do_GET(self) -> None:
        path = urlsplit(self.path).path
        if path == "/":
            self._send_bytes(PAGE.encode("utf-8"), "text/html; charset=utf-8")
        elif path == "/status":
            self._send_json(self.app.build_status())
        else:
            self.send_error(404)

    def do_POST(self) -> None:
        path = urlsplit(self.path).path
        length = int(self.headers.get("Content-Length", "0"))
        payload = json.loads(self.rfile.read(length)) if length else {}
        if path == "/connect":
            self.app.connect(str(payload["hostname"]), float(payload["room_length_metres"]))
        elif path == "/start":
            self.app.start_phase()
        elif path == "/cancel":
            self.app.cancel()
        elif path == "/write":
            self.app.start_writing()
        else:
            self.send_error(404)
            return
        self._send_json({"ok": True})

    def _send_json(self, payload: dict[str, Any]) -> None:
        self._send_bytes(json.dumps(payload).encode("utf-8"), "application/json")

    def _send_bytes(self, body: bytes, content_type: str) -> None:
        self.send_response(200)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, format: str, *args: Any) -> None:
        # The page polls /status once per second, so logging every request would
        # bury the startup hint and the device errors that matter.
        return


def local_ip_address() -> str:
    # A UDP connect picks the route without sending a packet, which is the
    # portable way to learn the address a phone on the LAN can reach.
    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
        sock.connect(("8.8.8.8", 80))
        return sock.getsockname()[0]


def main() -> None:
    app = Calibration()
    RequestHandler.app = app
    server = ThreadingHTTPServer(("0.0.0.0", PORT), RequestHandler)
    print(f"Open http://{local_ip_address()}:{PORT} on your phone.")
    print("The phone and the sensor must share the LAN, and the firewall must allow 8765.")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nStopping.")
    finally:
        app.shutdown()
        server.server_close()


if __name__ == "__main__":
    main()
