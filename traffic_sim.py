#!/usr/bin/env python3
"""
traffic_sim.py — Real-time 4‑way intersection traffic micro-simulator (Python 3.10)

README (quick start)
--------------------
Run a quick 5s dry run that prints ~25 events and a heartbeat:
    python traffic_sim.py --dry-run

Run indefinitely at 200ms ticks, stdout sink:
    python traffic_sim.py

File sink (JSONL):
    python traffic_sim.py --sink file:/tmp/vehicle_events.jsonl

Event Hubs sink (requires azure-eventhub):
    export EVENTHUBS_CONNECTION_STRING='Endpoint=sb://...'
    python traffic_sim.py --sink eventhubs --eventhubs-conn "<env:EVENTHUBS_CONNECTION_STRING>"

Kafka sink (optional, requires confluent-kafka):
    python traffic_sim.py --sink kafka://broker:9092/vehicle_events

Signal sources
    --signal-source fixed                                  # default 2-phase
    --signal-source file:/path/to/signal.json              # reloads if mtime changes
    --signal-source http://host:port/state                 # GET per tick (200ms timeout)

CLI flags (defaults shown; see --help):
    --tick-ms 200
    --duration-s 0
    --spawn-rate "N=0.8,E=0.6,S=0.8,W=0.6"   # vehicles/min per approach (Poisson-ish)
    --turn-probs "straight=0.85,left=0.10,right=0.05"
    --max-vehicles 500
    --random-seed 42
    --signal-source fixed|file:/...|http://...
    --sink stdout|file:/...|eventhubs|kafka://broker:9092/topic
    --eventhubs-conn "<env:EVENTHUBS_CONNECTION_STRING>"
    --topic vehicle_events
    --intersection-size-m 60
    --validate      # quick invariant check
    --dry-run       # run 5 seconds and emit sample events

Sample vehicle event (compact JSON per line):
    {"type":"vehicle_event","ts":"2025-10-15T12:34:56.789Z","veh_id":"v-000123","approach":"N","lane":1,"intent":"straight","x":0.0,"y":12.3,"speed_mps":6.2,"acc_mps2":1.0,"dist_to_stop_m":5.7,"state":"moving","signal":"red","tick":12345}
"""
from __future__ import annotations
import argparse
import asyncio
import dataclasses
from dataclasses import dataclass, field
import json
import logging
import math
import os
import random
import signal as _signal
import sys
import time
from typing import Dict, List, Optional, Tuple, Iterable

# -----------------------
# Constants & utilities
# -----------------------
APPROACHES = ("N", "E", "S", "W")
TURNS = ("straight", "left", "right")

def utc_iso_now() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime()) + f".{int((time.time()%1)*1000):03d}Z"

def clamp(v: float, lo: float, hi: float) -> float:
    return lo if v < lo else hi if v > hi else v

def parse_keyfloats(s: str, keys: Iterable[str]) -> Dict[str, float]:
    parts = [p.strip() for p in s.split(",") if p.strip()]
    m: Dict[str, float] = {}
    for p in parts:
        if "=" not in p:
            continue
        k, v = p.split("=", 1)
        k = k.strip()
        try:
            m[k] = float(v.strip())
        except ValueError:
            raise argparse.ArgumentTypeError(f"Bad float for {k}: {v}")
    for k in keys:
        m.setdefault(k, 0.0)
    return m

# -----------------------
# Data model
# -----------------------
@dataclass
class Vehicle:
    vid: str
    approach: str  # N,E,S,W (origin approach)
    lane: int
    intent: str    # straight,left,right
    x: float
    y: float
    v: float = 0.0         # speed m/s
    a: float = 0.0         # accel m/s^2 (set per tick)
    state: str = "moving"  # moving|queued|clearing|exited
    # For turning
    turning: bool = False
    turn_theta: float = 0.0   # radians along arc [0..pi/2]
    turn_dir: int = 0         # +1 left, -1 right, 0 straight
    turn_center: Tuple[float,float] | None = None
    # bookkeeping
    entered_tick: int = 0
    last_signal: str = "green"

    def pos(self) -> Tuple[float,float]:
        return (self.x, self.y)

class Lane:
    def __init__(self, approach: str, lane_id: int, spawn: Tuple[float,float], dirvec: Tuple[float,float]):
        self.approach = approach
        self.lane_id = lane_id
        self.spawn = spawn
        self.dirvec = dirvec  # unit vector toward center
        self.vehicles: List[Vehicle] = []

    def sort_along_axis(self):
        # order by distance from center decreasing (leaders first are closest to stop line?)
        if self.approach == "N":
            # moving toward -y to 0; leader has smallest y (closest to 0)
            self.vehicles.sort(key=lambda v: v.y)
        elif self.approach == "S":
            self.vehicles.sort(key=lambda v: -v.y)
        elif self.approach == "E":
            self.vehicles.sort(key=lambda v: v.x)
        elif self.approach == "W":
            self.vehicles.sort(key=lambda v: -v.x)

    def front_vehicle(self) -> Optional[Vehicle]:
        if not self.vehicles:
            return None
        self.sort_along_axis()
        return self.vehicles[0]

class SignalSource:
    async def get_state(self) -> Dict[str,str]:
        raise NotImplementedError

class FixedSignalSource(SignalSource):
    def __init__(self, tick_s: float, min_green: float = 8.0, max_green: float = 25.0):
        self.tick_s = tick_s
        self.min_green = min_green
        self.max_green = max_green
        self.phase = 0  # 0: NS green, EW red; 1: EW green, NS red
        self.phase_elapsed = 0.0

    async def get_state(self) -> Dict[str,str]:
        # 2‑phase controller with simple dwell between min and max green using demand heuristic
        # For simplicity, flip at max_green; could add queue-based extension later.
        if self.phase_elapsed >= self.max_green:
            self.phase = 1 - self.phase
            self.phase_elapsed = 0.0
        self.phase_elapsed += self.tick_s
        if self.phase == 0:
            return {"N":"green","S":"green","E":"red","W":"red","phase_id":1,"ts":utc_iso_now()}
        else:
            return {"N":"red","S":"red","E":"green","W":"green","phase_id":2,"ts":utc_iso_now()}

class FileSignalSource(SignalSource):
    def __init__(self, path: str):
        self.path = path
        self._mtime = 0.0
        self._last = {"N":"green","S":"green","E":"red","W":"red","phase_id":1,"ts":utc_iso_now()}
    async def get_state(self) -> Dict[str,str]:
        try:
            st = os.stat(self.path)
            if st.st_mtime > self._mtime:
                with open(self.path, "r", encoding="utf-8") as f:
                    self._last = json.load(f)
                self._mtime = st.st_mtime
        except Exception:
            # keep last state
            pass
        return self._last

class HttpSignalSource(SignalSource):
    def __init__(self, url: str, timeout: float = 0.2):
        self.url = url
        self.timeout = timeout
        self._last = {"N":"green","S":"green","E":"red","W":"red","phase_id":1,"ts":utc_iso_now()}
    async def get_state(self) -> Dict[str,str]:
        # Use stdlib to avoid external deps
        import urllib.request
        try:
            req = urllib.request.Request(self.url, headers={"User-Agent":"traffic-sim/1.0"})
            with urllib.request.urlopen(req, timeout=self.timeout) as resp:
                data = json.loads(resp.read().decode("utf-8"))
                self._last = data
        except Exception:
            pass
        return self._last

class EventSink:
    async def start(self): ...
    async def write(self, events: List[dict]): ...
    async def flush(self): ...
    async def close(self): ...

class StdoutSink(EventSink):
    def __init__(self):
        self._buf: List[str] = []
    async def write(self, events: List[dict]):
        for e in events:
            sys.stdout.write(json.dumps(e, separators=(',',':')) + "\n")
        sys.stdout.flush()
    async def flush(self): 
        sys.stdout.flush()

class FileSink(EventSink):
    def __init__(self, url: str):
        # file:/path/to/file.jsonl
        assert url.startswith("file:")
        self.path = url[len("file:"):]
        self._fh = None
        self._open_for_date = None
    async def start(self):
        self._maybe_rotate()
    def _maybe_rotate(self):
        d = time.strftime("%Y%m%d", time.gmtime())
        if self._fh is None or self._open_for_date != d:
            if self._fh:
                self._fh.flush(); self._fh.close()
            os.makedirs(os.path.dirname(self.path), exist_ok=True)
            self._fh = open(self.path, "a", buffering=1, encoding="utf-8")
            self._open_for_date = d
    async def write(self, events: List[dict]):
        self._maybe_rotate()
        for e in events:
            self._fh.write(json.dumps(e, separators=(',',':')) + "\n")
    async def flush(self):
        if self._fh: self._fh.flush()
    async def close(self):
        if self._fh: self._fh.close()

class EventHubsSink(EventSink):
    def __init__(self, conn_env_or_literal: str, topic: str):
        self.conn_spec = conn_env_or_literal
        self.topic = topic
        self._producer = None
    async def start(self):
        conn = os.environ.get("EVENTHUBS_CONNECTION_STRING") if self.conn_spec.startswith("<env:") else self.conn_spec
        try:
            from azure.eventhub.aio import EventHubProducerClient
            from azure.eventhub import EventData
        except Exception as e:
            raise RuntimeError("azure-eventhub is required for eventhubs sink") from e
        self._EventData = EventData
        self._producer = EventHubProducerClient.from_connection_string(conn, eventhub_name=self.topic)
    async def write(self, events: List[dict]):
        if not self._producer: return
        from azure.eventhub import EventData
        async with self._producer:
            event_batch = await self._producer.create_batch()
            for e in events:
                s = json.dumps(e, separators=(',',':'))
                try:
                    event_batch.add(self._EventData(s))
                except ValueError:
                    await self._producer.send_batch(event_batch)
                    event_batch = await self._producer.create_batch()
                    event_batch.add(self._EventData(s))
            if len(event_batch) > 0:
                await self._producer.send_batch(event_batch)
    async def close(self): ...

class KafkaSink(EventSink):
    def __init__(self, url: str):
        # kafka://broker:9092/topic
        self.url = url
        self._producer = None
        self._topic = None
    async def start(self):
        try:
            from confluent_kafka import Producer
        except Exception as e:
            raise RuntimeError("confluent-kafka is required for kafka sink") from e
        u = self.url[len("kafka://"):]
        broker, topic = u.split("/", 1)
        self._topic = topic
        self._producer = Producer({"bootstrap.servers": broker})
    async def write(self, events: List[dict]):
        if not self._producer: return
        for e in events:
            self._producer.produce(self._topic, json.dumps(e, separators=(',',':')))
        self._producer.poll(0)
    async def flush(self):
        if self._producer: self._producer.flush()

# -----------------------
# Intersection world
# -----------------------
class Intersection:
    def __init__(self, L: float = 60.0):
        self.L = L
        # Spawn points (toward center)
        self.lanes: Dict[str, Lane] = {
            "N": Lane("N", 1, (0.0,  L), (0.0, -1.0)),
            "S": Lane("S", 1, (0.0, -L), (0.0,  1.0)),
            "E": Lane("E", 1, ( L, 0.0), (-1.0, 0.0)),
            "W": Lane("W", 1, (-L, 0.0), ( 1.0, 0.0)),
        }
        self.vehicles: Dict[str, Vehicle] = {}
        self.tick: int = 0
        self.violations: Dict[str,int] = {"red_cross":0, "headway":0}
        # metrics windows
        self.arrivals_hist: Dict[str,List[int]] = {a:[] for a in APPROACHES}
        self.through_hist: Dict[str,List[int]] = {a:[] for a in APPROACHES}
        self.speeds_hist: Dict[str,List[float]] = {a:[] for a in APPROACHES}

    def reset_metrics_if_needed(self, maxlen: int):
        for a in APPROACHES:
            if len(self.arrivals_hist[a])>maxlen: self.arrivals_hist[a]=self.arrivals_hist[a][-maxlen:]
            if len(self.through_hist[a])>maxlen: self.through_hist[a]=self.through_hist[a][-maxlen:]
            if len(self.speeds_hist[a])>maxlen: self.speeds_hist[a]=self.speeds_hist[a][-maxlen:]

    def spawn_vehicle(self, approach: str, intent: str, tick: int, seed: int) -> Optional[Vehicle]:
        lane = self.lanes[approach]
        # Ensure first 20m clear
        def dist_along(v: Vehicle) -> float:
            if approach in ("N","S"):
                return abs(v.y)
            else:
                return abs(v.x)
        for v in lane.vehicles:
            if dist_along(v) >= self.L - 20.0:  # near spawn
                return None  # defer spawn
        vid = f"v-{tick:06d}-{seed%1000:03d}-{len(self.vehicles)%10000:04d}"
        x,y = lane.spawn
        v = Vehicle(vid=vid, approach=approach, lane=1, intent=intent, x=x, y=y, v=0.0, a=0.0, state="moving", entered_tick=tick)
        # prepare turn geom if needed
        if intent in ("left","right"):
            v.turning = False
            v.turn_theta = 0.0
            v.turn_dir = +1 if intent=="left" else -1
            R = 10.0
            # Define quarter-circle centers for each approach
            if approach=="N":   # going toward (0,0) from +y
                cx,cy = (+R * (1 if v.turn_dir==+1 else -1), 0.0)
            elif approach=="S":
                cx,cy = (-R * (1 if v.turn_dir==+1 else -1), 0.0)
            elif approach=="E":
                cx,cy = (0.0, -R * (1 if v.turn_dir==+1 else -1))
            else: # W
                cx,cy = (0.0, +R * (1 if v.turn_dir==+1 else -1))
            v.turn_center = (cx, cy)
        self.vehicles[v.vid] = v
        lane.vehicles.append(v)
        return v

    def remove_if_exited(self, v: Vehicle) -> bool:
        L = self.L
        if abs(v.x) > L or abs(v.y) > L:
            v.state = "exited"
            # remove from lane
            lane = self.lanes[v.approach]
            if v in lane.vehicles:
                lane.vehicles.remove(v)
            if v.vid in self.vehicles:
                del self.vehicles[v.vid]
            return True
        return False

# -----------------------
# Physics & update
# -----------------------
class Simulator:
    def __init__(self, inter: Intersection, tick_s: float, signal: SignalSource, sink: EventSink,
                 v_des: float = 13.4, a: float = 1.5, b: float = 2.5, headway_m: float = 2.0,
                 max_vehicles: int = 500):
        self.I = inter
        self.dt = tick_s
        self.signal_src = signal
        self.sink = sink
        self.v_des = v_des
        self.a0 = a
        self.b0 = b
        self.headway_m = headway_m
        self.max_vehicles = max_vehicles
        self.signals = {a:"green" for a in APPROACHES}

    def dist_to_stop(self, v: Vehicle) -> float:
        # distance along lane axis to stop line at 0
        if v.approach == "N":
            return abs(v.y - 0.0)
        if v.approach == "S":
            return abs(0.0 - v.y)
        if v.approach == "E":
            return abs(v.x - 0.0)
        return abs(0.0 - v.x)

    async def step(self, tick: int):
        # 1) poll signal
        state = await self.signal_src.get_state()
        self.signals = {k:state.get(k,"red") for k in APPROACHES}
        # 2) per-lane headway sorting
        for a in APPROACHES:
            self.I.lanes[a].sort_along_axis()
        # 3) update vehicles
        events: List[dict] = []
        dt = self.dt
        for a in APPROACHES:
            lane = self.I.lanes[a]
            leader: Optional[Vehicle] = None
            for i,v in enumerate(lane.vehicles):
                sig = self.signals[a]
                v.last_signal = sig
                # Determine target accel/speed
                target_v = self.v_des
                # Turning speed cap
                if v.turning or (v.intent in ("left","right") and self.is_in_junction(v)):
                    target_v = min(target_v, 8.0)
                # Car-following: ensure headway to leader
                if i>0:
                    leader = lane.vehicles[i-1]
                    gap = self.distance_between(v, leader) - self.headway_m
                    if gap < self.headway_m:
                        # reduce speed to avoid collision
                        target_v = min(target_v, max(0.0, gap/dt))
                        self.I.violations["headway"] += 1 if gap<0 else 0
                # Signal compliance near stop line
                dstop = self.dist_to_stop(v)
                if sig == "red" and dstop <= 8.0 and not self.is_in_junction(v):
                    # decelerate to stop before line
                    target_v = min(target_v, max(0.0, dstop/dt * 0.5))
                    if v.v <= 0.1:
                        v.state = "queued"
                    else:
                        v.state = "moving"
                elif sig == "green":
                    # first in queue releases; others follow via headway above
                    if i==0 and v.v < target_v and dstop <= 1.0:
                        v.state = "moving"
                # accel toward target
                dv = target_v - v.v
                max_acc = self.a0 if dv>0 else -self.b0
                a_cmd = clamp(dv/dt, -self.b0, self.a0)
                v.a = clamp(a_cmd, -self.b0, self.a0)
                v.v = clamp(v.v + v.a * dt, 0.0, 30.0)
                # position update
                self.advance_position(v, dt)
                # state transitions
                if self.is_in_junction(v):
                    v.state = "clearing"
                # exit check
                self.I.remove_if_exited(v)
                # Collect metrics and event
                self.I.speeds_hist[a].append(v.v)
                ev = {
                    "type":"vehicle_event",
                    "ts": utc_iso_now(),
                    "veh_id": v.vid,
                    "approach": v.approach,
                    "lane": v.lane,
                    "intent": v.intent,
                    "x": round(v.x,3),
                    "y": round(v.y,3),
                    "speed_mps": round(v.v,3),
                    "acc_mps2": round(v.a,3),
                    "dist_to_stop_m": round(self.dist_to_stop(v),3),
                    "state": v.state,
                    "signal": sig,
                    "tick": tick
                }
                events.append(ev)
                # red light violation detection (entered junction on red)
                if self.just_crossed_stopline(v) and sig=="red":
                    self.I.violations["red_cross"] += 1
        # 4) heartbeat
        hb = self.heartbeat(tick)
        events.append(hb)
        # 5) emit
        await self.sink.write(events)

    def is_in_junction(self, v: Vehicle) -> bool:
        return abs(v.x) <= 2.0 and abs(v.y) <= 2.0

    def just_crossed_stopline(self, v: Vehicle) -> bool:
        # simple: if now in junction
        return self.is_in_junction(v)

    def distance_between(self, follower: Vehicle, leader: Vehicle) -> float:
        # along approach axis only (1D)
        if follower.approach in ("N","S"):
            return abs(abs(follower.y) - abs(leader.y))
        else:
            return abs(abs(follower.x) - abs(leader.x))

    def advance_position(self, v: Vehicle, dt: float):
        # straight advance until near center; handle turning with quarter circle radius 10
        if v.intent == "straight":
            v.x += self.I.lanes[v.approach].dirvec[0]*v.v*dt
            v.y += self.I.lanes[v.approach].dirvec[1]*v.v*dt
        else:
            # Start turning when entering near-center box
            if (not v.turning) and (abs(v.x)<=2.0 or abs(v.y)<=2.0):
                v.turning = True
                v.turn_theta = 0.0
            if v.turning and v.turn_center:
                cx,cy = v.turn_center
                R = 10.0
                # theta progress per arc length s = v*dt; dtheta = s/R
                dtheta = (v.v*dt) / R
                v.turn_theta = min(v.turn_theta + dtheta, math.pi/2)
                # Map approach+turn_dir to param origin
                if v.approach=="N":
                    theta0 = -math.pi/2 if v.turn_dir==+1 else -math.pi
                elif v.approach=="S":
                    theta0 = 0.0 if v.turn_dir==+1 else math.pi/2
                elif v.approach=="E":
                    theta0 = math.pi if v.turn_dir==+1 else math.pi/2
                else: # W
                    theta0 = -math.pi/2 if v.turn_dir==+1 else 0.0
                theta = theta0 + v.turn_dir * v.turn_theta
                v.x = cx + R*math.cos(theta)
                v.y = cy + R*math.sin(theta)
                if v.turn_theta >= math.pi/2 - 1e-3:
                    # turn complete: continue along new axis outwards
                    v.turning = False
                    # set direction vector after turn
                    if v.approach=="N":
                        if v.turn_dir==+1: # left -> E
                            v.approach = "E"
                            v.x += 0.01
                        else:              # right -> W
                            v.approach = "W"
                            v.x -= 0.01
                    elif v.approach=="S":
                        if v.turn_dir==+1: # left -> W
                            v.approach = "W"; v.x -= 0.01
                        else:              # right -> E
                            v.approach = "E"; v.x += 0.01
                    elif v.approach=="E":
                        if v.turn_dir==+1: # left -> S
                            v.approach = "S"; v.y -= 0.01
                        else:              # right -> N
                            v.approach = "N"; v.y += 0.01
                    else: # W
                        if v.turn_dir==+1: # left -> N
                            v.approach = "N"; v.y += 0.01
                        else:              # right -> S
                            v.approach = "S"; v.y -= 0.01
            else:
                v.x += self.I.lanes[v.approach].dirvec[0]*v.v*dt
                v.y += self.I.lanes[v.approach].dirvec[1]*v.v*dt

    def heartbeat(self, tick: int) -> dict:
        # 5s windows
        win = int(round(5.0/self.dt))
        self.I.reset_metrics_if_needed(win+5)
        metrics = {}
        for a in APPROACHES:
            q = sum(1 for v in self.I.lanes[a].vehicles if v.state=="queued")
            arr = sum(self.I.arrivals_hist[a][-win:]) if self.I.arrivals_hist[a] else 0
            thr = sum(self.I.through_hist[a][-win:]) if self.I.through_hist[a] else 0
            sp = self.I.speeds_hist[a][-win:]
            avg = (sum(sp)/len(sp)) if sp else 0.0
            metrics[a] = {"queue": q, "arrivals_5s": arr, "throughput_5s": thr, "avg_speed": round(avg,3)}
        return {"type":"sim_heartbeat","ts":utc_iso_now(),"tick":tick,"metrics":metrics}

# -----------------------
# Spawner
# -----------------------
class Spawner:
    def __init__(self, inter: Intersection, spawn_rate_min: Dict[str,float], turn_probs: Dict[str,float], max_vehicles: int):
        self.I = inter
        self.rpm = spawn_rate_min
        self.turn_probs = turn_probs
        self.max_vehicles = max_vehicles
        self.rsec = {k: max(0.0, v/60.0) for k,v in self.rpm.items()}

    def maybe_spawn(self, tick: int, dt: float, rng: random.Random):
        # approximate Poisson(λ*dt) by Bernoulli with p = min(1, λ*dt); at most 1 per approach per tick
        if len(self.I.vehicles) >= self.max_vehicles:
            return
        for a in APPROACHES:
            lam = self.rsec.get(a,0.0)
            p = min(1.0, lam*dt)
            if rng.random() < p:
                intent = self.pick_intent(rng)
                v = self.I.spawn_vehicle(a, intent, tick, int(rng.random()*1e9))
                if v:
                    self.I.arrivals_hist[a].append(1)

    def pick_intent(self, rng: random.Random) -> str:
        r = rng.random()
        c = 0.0
        for k in ("straight","left","right"):
            c += self.turn_probs.get(k,0.0)
            if r <= c:
                return k
        return "straight"

# -----------------------
# Runner
# -----------------------
async def run_sim(args):
    logging.basicConfig(stream=sys.stderr, level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    rng = random.Random(args.random_seed)
    dt = args.tick_ms / 1000.0
    inter = Intersection(L=args.intersection_size_m)
    # signal source
    if args.signal_source == "fixed":
        sigsrc = FixedSignalSource(dt)
    elif args.signal_source.startswith("file:"):
        sigsrc = FileSignalSource(args.signal_source[len("file:"):])
    elif args.signal_source.startswith("http://") or args.signal_source.startswith("https://"):
        sigsrc = HttpSignalSource(args.signal_source)
    else:
        raise SystemExit(f"Unknown signal source: {args.signal_source}")
    # sink
    if args.sink == "stdout":
        sink = StdoutSink()
    elif args.sink.startswith("file:"):
        sink = FileSink(args.sink)
    elif args.sink == "eventhubs":
        sink = EventHubsSink(args.eventhubs_conn, args.topic)
    elif args.sink.startswith("kafka://"):
        sink = KafkaSink(args.sink)
    else:
        raise SystemExit(f"Unknown sink: {args.sink}")
    await sink.start() if hasattr(sink, "start") else None
    sim = Simulator(inter, dt, sigsrc, sink, max_vehicles=args.max_vehicles)
    spawner = Spawner(inter, args.spawn_rate, args.turn_probs, args.max_vehicles)

    stop = False
    def _sig_handler(*_):
        nonlocal stop
        stop = True
    loop = asyncio.get_running_loop()
    for s in (_signal.SIGINT, _signal.SIGTERM):
        try:
            loop.add_signal_handler(s, _sig_handler)
        except NotImplementedError:
            pass

    start_time = time.perf_counter()
    max_ticks = int(args.duration_s/dt) if args.duration_s>0 else None
    tick = 0
    try:
        while True:
            t0 = time.perf_counter()
            tick += 1
            inter.tick = tick
            spawner.maybe_spawn(tick, dt, rng)
            await sim.step(tick)
            # pacing
            elapsed = time.perf_counter() - t0
            sleep_t = max(0.0, dt - elapsed)
            await asyncio.sleep(sleep_t)
            if stop: break
            if max_ticks is not None and tick >= max_ticks: break
            if args.dry_run and (time.perf_counter() - start_time) >= 5.0: break
    finally:
        await sink.flush() if hasattr(sink,"flush") else None
        await sink.close() if hasattr(sink,"close") else None

    if args.validate:
        ok, msg = validate_invariants(inter)
        if ok:
            print("VALIDATION: OK", file=sys.stderr)
        else:
            print(f"VALIDATION: FAIL - {msg}", file=sys.stderr)

# -----------------------
# Validation
# -----------------------
def validate_invariants(inter: Intersection) -> Tuple[bool,str]:
    v = inter.violations
    if v["red_cross"]>0:
        return False, f"red light crossings detected: {v['red_cross']}"
    if v["headway"]>0:
        # headway tracking is conservative; allow minor breaches
        if v["headway"] > 5:
            return False, f"excessive headway breaches: {v['headway']}"
    return True, "ok"

# -----------------------
# CLI
# -----------------------
def build_argparser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Real-time 4‑way intersection traffic simulator emitting JSONL events.")
    p.add_argument("--tick-ms", type=int, default=200)
    p.add_argument("--duration-s", type=int, default=0, help="0 = run forever")
    p.add_argument("--spawn-rate", type=str, default="N=0.8,E=0.6,S=0.8,W=0.6",
                   help="vehicles/min per approach, e.g., 'N=0.8,E=0.6,S=0.8,W=0.6'")
    p.add_argument("--turn-probs", type=str, default="straight=0.85,left=0.10,right=0.05")
    p.add_argument("--max-vehicles", type=int, default=500)
    p.add_argument("--random-seed", type=int, default=42)
    p.add_argument("--signal-source", type=str, default="fixed")
    p.add_argument("--sink", type=str, default="stdout")
    p.add_argument("--eventhubs-conn", type=str, default="<env:EVENTHUBS_CONNECTION_STRING>")
    p.add_argument("--topic", type=str, default="vehicle_events")
    p.add_argument("--intersection-size-m", type=float, default=60.0)
    p.add_argument("--validate", action="store_true")
    p.add_argument("--dry-run", action="store_true")
    return p

def parse_args(argv: List[str]) -> argparse.Namespace:
    p = build_argparser()
    args = p.parse_args(argv)
    # parse spawn rates and turn probs
    args.spawn_rate = parse_keyfloats(args.spawn_rate, APPROACHES)
    args.turn_probs = parse_keyfloats(args.turn_probs, TURNS)
    # normalize turn probs
    s = sum(args.turn_probs.values()) or 1.0
    for k in list(args.turn_probs.keys()):
        args.turn_probs[k] = max(0.0, args.turn_probs[k]/s)
    return args

# -----------------------
# Entry
# -----------------------
def main(argv: Optional[List[str]] = None):
    args = parse_args(argv if argv is not None else sys.argv[1:])
    return asyncio.run(run_sim(args))

if __name__ == "__main__":
    main()
