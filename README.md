# scenario-execution-server

Remote action execution for [scenario-execution](https://github.com/IntelLabs/scenario_execution).  
Actions declared with `remote(host)` run on a server process instead of locally.

## Packages

| Package | Role |
|---|---|
| `scenario_execution_remote` | Client-side OSC2 modifier + py\_trees integration |
| `scenario_execution_server` | Server binary — executes action plugins on behalf of clients |

## Install

```bash
python -m venv .venv && source .venv/bin/activate
pip install -e scenario_execution/scenario_execution   # base library
pip install -e scenario_execution_remote
pip install -e scenario_execution_server
```

## Usage

**Start the server** (on the machine that should execute actions):

```bash
# TCP
scenario_execution_server --port 7613

# Unix domain socket
scenario_execution_server --socket /tmp/se.sock
```

**Declare a remote action** in your `.osc` scenario:

```osc
import osc.types
import osc.helpers
import osc.remote

scenario test_run_process:
    do serial:
        run_process("hostname") with:
            remote("127.0.0.1")         # TCP, default port 7613
            # remote("192.168.1.10:9000")  # TCP, explicit port
            # remote("/tmp/se.sock")        # Unix domain socket
```

**Run the scenario** normally — the client connects to the server during tree setup:

```bash
scenario_execution test.osc -t
```

## How it works

```
scenario_execution (client)          scenario_execution_server
        |                                       |
  setup()  ──── init + setup ────────────────► |  instantiates action plugin
  initialise() ─ execute ──────────────────── ► |  calls action.execute()
  update() ─────── update (poll) ─────────── ► |  returns RUNNING / SUCCESS / FAILURE
  terminate() ──── terminate ──────────────── ► |
```

Transport: ZMQ REQ/REP, serialisation: msgpack.  
One socket per `remote()` modifier instance.  
Connection timeout: 5 s (setup), 30 s (execution).

## OSC2 modifier signature

```osc
modifier remote:
    endpoint: string  # hostname/IP, host:port, or /path/to/unix-socket
```

Any action available as a `scenario_execution.actions` entry-point on the server can be used remotely.
