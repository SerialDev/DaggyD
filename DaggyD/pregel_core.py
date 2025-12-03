"""
Pregel BSP (Bulk Synchronous Parallel) implementation in Lisp-like functional style.

This module provides composable functions for building Pregel graphs:
- Functions take data as first argument, return modified data
- Enables chaining: run(add_node(add_channel(pregel(), ...), ...), ...)
- Or piping: pregel() |> add_channel(...) |> add_node(...) |> run(...)

Example (Lisp-style composition):
    result = run(
        add_node(
            add_channel(
                add_channel(pregel(), "input", "LastValue", initial_value=10),
                "output", "LastValue"
            ),
            "doubler", lambda inputs: {"out": inputs["input"] * 2},
            subscribe_to=["input"], write_to={"out": "output"}
        )
    )

Example (imperative with returned state):
    p = pregel()
    p = add_channel(p, "input", "LastValue", initial_value=10)
    p = add_channel(p, "output", "LastValue")
    p = add_node(p, "doubler", func, subscribe_to=["input"], write_to={"out": "output"})
    result = run(p)
"""

from collections import deque
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor, as_completed
from multiprocessing import Manager, cpu_count
import pickle
import sqlite3
import copy


def stringify_truncated(obj, max_len=100):
    """Truncate string representation for display."""
    s = str(obj)
    return s if len(s) <= max_len else s[:max_len] + "..."


# =============================================================================
# CORE DATA CONSTRUCTORS
# =============================================================================

def pregel(debug=True, parallel=False, max_workers=None):
    """
    Create a new Pregel graph data structure.
    
    Returns an immutable-style dict representing the graph state.
    All operations return new/modified state rather than mutating.
    """
    if debug:
        print("\033[36m[INIT] Pregel BSP Executor initialized\033[0m")
        if parallel:
            print(f"\033[36m[INIT] Parallel mode enabled with {max_workers or cpu_count()} workers\033[0m")
    
    return {
        "type": "Pregel",
        "nodes": {},
        "channels": {},
        "superstep": 0,
        "debug": debug,
        "executed": {},
        "parallel": parallel,
        "max_workers": max_workers if max_workers else cpu_count()
    }


# =============================================================================
# CHANNEL OPERATIONS
# =============================================================================

def add_channel(p, name, channel_type="LastValue", **kwargs):
    """
    Add a channel to the Pregel graph. Returns new graph state.
    
    Args:
        p: Pregel graph state
        name: Channel name
        channel_type: One of "LastValue", "Topic", "BinaryOperator", "Ephemeral", "Accumulator"
        **kwargs: Channel-specific options (initial_value, operator, etc.)
    
    Returns:
        New Pregel graph state with channel added
    """
    from .channels import (
        create_last_value_channel,
        create_topic_channel,
        create_binary_operator_channel,
        create_ephemeral_channel,
        create_accumulator_channel
    )
    
    if name in p["channels"]:
        raise ValueError(f"\033[31mChannel {name} already exists\033[0m")
    
    if channel_type == "LastValue":
        channel = create_last_value_channel(kwargs.get("initial_value"))
    elif channel_type == "Topic":
        channel = create_topic_channel()
    elif channel_type == "BinaryOperator":
        operator = kwargs.get("operator")
        if not operator:
            raise ValueError(f"\033[31mBinaryOperator requires an operator function\033[0m")
        channel = create_binary_operator_channel(operator, kwargs.get("initial_value"))
    elif channel_type == "Ephemeral":
        channel = create_ephemeral_channel()
    elif channel_type == "Accumulator":
        operator = kwargs.get("operator")
        if not operator:
            raise ValueError(f"\033[31mAccumulator requires an operator function\033[0m")
        channel = create_accumulator_channel(operator, kwargs.get("initial_value"))
    else:
        raise ValueError(f"\033[31mUnknown channel type: {channel_type}\033[0m")
    
    if p["debug"]:
        print(f"\033[36m[REGISTER] Channel '{name}' (type={channel_type})\033[0m")
    
    # Return new state with channel added
    new_channels = dict(p["channels"])
    new_channels[name] = channel
    return {**p, "channels": new_channels}


# =============================================================================
# NODE OPERATIONS
# =============================================================================

def add_node(p, name, func, subscribe_to=None, write_to=None):
    """
    Add a compute node to the Pregel graph. Returns new graph state.
    
    Args:
        p: Pregel graph state
        name: Node name
        func: Compute function (inputs) -> outputs
        subscribe_to: List of channel names to read from
        write_to: Dict mapping output keys to channel names
    
    Returns:
        New Pregel graph state with node added
    """
    if name in p["nodes"]:
        raise ValueError(f"\033[31mNode {name} already exists\033[0m")
    
    subscribe_to = subscribe_to if isinstance(subscribe_to, list) else [subscribe_to] if subscribe_to else []
    write_to = write_to if isinstance(write_to, dict) else {write_to: write_to} if write_to else {}
    
    if p["debug"]:
        print(f"\033[36m[REGISTER] Node '{name}': subscribes={stringify_truncated(subscribe_to)}, writes={stringify_truncated(write_to)}\033[0m")
    
    new_nodes = dict(p["nodes"])
    new_nodes[name] = {
        "func": func,
        "subscribe_to": subscribe_to,
        "write_to": write_to,
        "active": True
    }
    return {**p, "nodes": new_nodes}


# =============================================================================
# EXECUTION
# =============================================================================

def _plan(p):
    """
    Determine which nodes to execute in current superstep.
    Returns list of node names.
    """
    ready_nodes = []
    
    for node_name, node_info in p["nodes"].items():
        has_new_message = False
        has_data = False
        
        for channel_name in node_info["subscribe_to"]:
            if channel_name in p["channels"]:
                channel = p["channels"][channel_name]
                if channel["has_updates"]():
                    has_new_message = True
                    break
                if not channel["is_empty"]():
                    has_data = True
        
        if has_new_message:
            if not node_info["active"]:
                node_info["active"] = True
                if p["debug"]:
                    print(f"\033[33m[REACTIVATE] {node_name} received message, reactivating\033[0m")
            ready_nodes.append(node_name)
        elif p["superstep"] == 0 and node_info["active"] and has_data:
            if p["executed"].get(node_name) == "not_started":
                ready_nodes.append(node_name)
        elif not node_info["subscribe_to"] and node_info["active"]:
            if p["executed"].get(node_name) == "not_started":
                ready_nodes.append(node_name)
    
    if p["debug"] and ready_nodes:
        print(f"\033[35m[PLAN] Superstep {p['superstep']}: nodes={ready_nodes}\033[0m")
    
    return ready_nodes


def _execute_node(p, node_name):
    """Execute a single node. Mutates channel state, returns success bool."""
    node_info = p["nodes"][node_name]
    func = node_info["func"]
    subscribe_to = node_info["subscribe_to"]
    write_to = node_info["write_to"]
    
    inputs = {}
    for channel_name in subscribe_to:
        if channel_name in p["channels"]:
            inputs[channel_name] = p["channels"][channel_name]["read"]()
    
    if p["debug"]:
        print(f"\033[35m[EXECUTE] {node_name}: inputs={stringify_truncated(inputs)}\033[0m")
    
    try:
        if inputs:
            result = func(inputs)
        else:
            result = func()
        
        if result is not None:
            if isinstance(result, dict):
                outputs = result
            else:
                default_channel = list(write_to.keys())[0] if write_to else None
                outputs = {default_channel: result} if default_channel else {}
            
            for output_key, output_value in outputs.items():
                channel_name = write_to.get(output_key, output_key)
                if channel_name in p["channels"]:
                    p["channels"][channel_name]["write"](output_value)
                    if p["debug"]:
                        print(f"\033[32m[WRITE] {node_name} -> {channel_name}: {stringify_truncated(output_value)}\033[0m")
        else:
            node_info["active"] = False
            if p["debug"]:
                print(f"\033[33m[HALT] {node_name} voted to halt\033[0m")
        
        p["executed"][node_name] = "succeeded"
        return True
        
    except Exception as e:
        p["executed"][node_name] = "failed"
        p["nodes"][node_name]["active"] = False
        if p["debug"]:
            print(f"\033[31m[FAIL] {node_name}: {e}\033[0m")
        return False


def _execute_node_worker(node_name, func, subscribe_to, write_to, channel_states, channel_types):
    """Worker function for parallel node execution."""
    inputs = {}
    for channel_name in subscribe_to:
        if channel_name in channel_states:
            ch_state = channel_states[channel_name]
            ch_type = channel_types[channel_name]
            if ch_type == "LastValue":
                inputs[channel_name] = ch_state["value"]
            elif ch_type == "Topic":
                inputs[channel_name] = ch_state["values"].copy()
            elif ch_type == "BinaryOperator":
                inputs[channel_name] = ch_state["value"]
            elif ch_type == "Ephemeral":
                inputs[channel_name] = ch_state["value"] if ch_state.get("has_value") else None
            elif ch_type == "Accumulator":
                inputs[channel_name] = ch_state["value"]
    
    try:
        if inputs:
            result = func(inputs)
        else:
            result = func()
        
        outputs = {}
        if result is not None:
            if isinstance(result, dict):
                outputs = result
            else:
                default_channel = list(write_to.keys())[0] if write_to else None
                outputs = {default_channel: result} if default_channel else {}
        
        return {"status": "succeeded", "node_name": node_name, "outputs": outputs, "write_to": write_to}
    
    except Exception as e:
        return {"status": "failed", "node_name": node_name, "error": str(e), "outputs": {}, "write_to": write_to}


def _execute_nodes_parallel(p, ready_nodes):
    """Execute multiple nodes in parallel."""
    channel_states = {}
    channel_types = {}
    for name, channel in p["channels"].items():
        channel_states[name] = channel["get_state"]()
        channel_types[name] = channel["type"]
    
    tasks = []
    for node_name in ready_nodes:
        node_info = p["nodes"][node_name]
        tasks.append({
            "node_name": node_name,
            "func": node_info["func"],
            "subscribe_to": node_info["subscribe_to"],
            "write_to": node_info["write_to"]
        })
    
    results = []
    with ThreadPoolExecutor(max_workers=min(len(tasks), p["max_workers"])) as executor:
        futures = {}
        for task in tasks:
            future = executor.submit(
                _execute_node_worker,
                task["node_name"],
                task["func"],
                task["subscribe_to"],
                task["write_to"],
                channel_states,
                channel_types
            )
            futures[future] = task["node_name"]
        
        for future in as_completed(futures):
            node_name = futures[future]
            try:
                result = future.result()
                results.append(result)
            except Exception as e:
                results.append({
                    "status": "failed",
                    "node_name": node_name,
                    "error": str(e),
                    "outputs": {},
                    "write_to": {}
                })
    
    for result in results:
        node_name = result["node_name"]
        if result["status"] == "succeeded":
            p["executed"][node_name] = "succeeded"
            outputs = result["outputs"]
            write_to = result["write_to"]
            
            if outputs:
                for output_key, output_value in outputs.items():
                    channel_name = write_to.get(output_key, output_key)
                    if channel_name in p["channels"]:
                        p["channels"][channel_name]["write"](output_value)
                        if p["debug"]:
                            print(f"\033[32m[WRITE] {node_name} -> {channel_name}: {stringify_truncated(output_value)}\033[0m")
            else:
                p["nodes"][node_name]["active"] = False
                if p["debug"]:
                    print(f"\033[33m[HALT] {node_name} voted to halt\033[0m")
        else:
            p["executed"][node_name] = "failed"
            p["nodes"][node_name]["active"] = False
            if p["debug"]:
                print(f"\033[31m[FAIL] {node_name}: {result.get('error', 'Unknown error')}\033[0m")
    
    return results


def _consume_messages(p):
    """Mark messages as consumed after planning."""
    for channel_name, channel in p["channels"].items():
        if "consume" in channel:
            channel["consume"]()


def _update(p):
    """BSP barrier: checkpoint all channels."""
    for channel_name, channel in p["channels"].items():
        checkpoint_value = channel["checkpoint"]()
        if p["debug"] and channel["type"] == "Ephemeral" and checkpoint_value is not None:
            print(f"\033[33m[CHECKPOINT] {channel_name}: cleared ephemeral value\033[0m")


def run(p, max_supersteps=1000):
    """
    Run the Pregel computation to completion.
    
    Args:
        p: Pregel graph state
        max_supersteps: Maximum number of supersteps before forced termination
    
    Returns:
        Dict mapping channel names to their final values
    """
    if p["debug"]:
        print(f"\033[36m\n[START] Beginning BSP execution\033[0m")
    
    # Initialize execution state
    for name in p["nodes"]:
        p["executed"][name] = "not_started"
    
    while p["superstep"] < max_supersteps:
        ready_nodes = _plan(p)
        
        if not ready_nodes:
            if p["debug"]:
                print(f"\033[36m[TERMINATE] No active nodes at superstep {p['superstep']}\033[0m")
            break
        
        _consume_messages(p)
        
        if p["parallel"] and len(ready_nodes) > 1:
            _execute_nodes_parallel(p, ready_nodes)
        else:
            for node_name in ready_nodes:
                _execute_node(p, node_name)
        
        _update(p)
        p["superstep"] += 1
    
    final_state = {}
    for name, channel in p["channels"].items():
        final_state[name] = channel["read"]()
    
    if p["debug"]:
        print(f"\033[36m[COMPLETE] Finished after {p['superstep']} supersteps\033[0m")
        print(f"\033[36m[FINAL STATE] {stringify_truncated(final_state)}\033[0m")
    
    return final_state


def reset(p):
    """
    Reset the Pregel graph to initial state. Returns the reset graph.
    
    Args:
        p: Pregel graph state
    
    Returns:
        Reset Pregel graph state
    """
    p["superstep"] = 0
    p["executed"] = {}
    
    for node_info in p["nodes"].values():
        node_info["active"] = True
    
    for channel in p["channels"].values():
        channel["clear"]()
    
    if p["debug"]:
        print("\033[36m[RESET] Pregel state cleared\033[0m")
    
    return p


# =============================================================================
# CHECKPOINTING
# =============================================================================

def get_checkpoint(p):
    """Get a checkpoint of the current state."""
    checkpoint = {
        "superstep": p["superstep"],
        "executed": p["executed"].copy(),
        "channel_states": {},
        "node_active_states": {}
    }
    
    for name, channel in p["channels"].items():
        checkpoint["channel_states"][name] = channel["get_state"]()
    
    for name, node_info in p["nodes"].items():
        checkpoint["node_active_states"][name] = node_info["active"]
    
    return checkpoint


def restore_checkpoint(p, checkpoint):
    """Restore state from a checkpoint. Returns the restored graph."""
    p["superstep"] = checkpoint["superstep"]
    p["executed"] = checkpoint["executed"].copy()
    
    for name, ch_state in checkpoint["channel_states"].items():
        if name in p["channels"]:
            p["channels"][name]["set_state"](ch_state)
    
    for name, active in checkpoint["node_active_states"].items():
        if name in p["nodes"]:
            p["nodes"][name]["active"] = active
    
    if p["debug"]:
        print(f"\033[36m[RESTORE] Restored checkpoint at superstep {p['superstep']}\033[0m")
    
    return p


def save_to_db(p, db_path):
    """Save checkpoint to SQLite database."""
    checkpoint = get_checkpoint(p)
    conn = sqlite3.connect(db_path)
    conn.execute(
        "CREATE TABLE IF NOT EXISTS pregel_checkpoint (id INTEGER PRIMARY KEY, data BLOB)"
    )
    serialized = pickle.dumps(checkpoint)
    conn.execute(
        "REPLACE INTO pregel_checkpoint (id, data) VALUES (?, ?)", (1, serialized)
    )
    conn.commit()
    conn.close()
    if p["debug"]:
        print(f"\033[36m[SAVE] Saved checkpoint to {db_path}\033[0m")
    return p


def load_from_db(p, db_path):
    """Load checkpoint from SQLite database. Returns (graph, success)."""
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    cur.execute("SELECT data FROM pregel_checkpoint WHERE id=?", (1,))
    row = cur.fetchone()
    conn.close()
    if row:
        checkpoint = pickle.loads(row[0])
        return restore_checkpoint(p, checkpoint), True
    return p, False


# =============================================================================
# GRAPHVIZ EXPORT
# =============================================================================

def to_graphviz(p, show_channel_values=True, title=None, rankdir="LR"):
    """Export Pregel graph structure to Graphviz DOT format."""
    lines = []
    lines.append('digraph PregelGraph {')
    lines.append(f'    rankdir={rankdir};')
    lines.append('    node [fontname="Arial"];')
    lines.append('    edge [fontname="Arial"];')
    
    if title:
        lines.append(f'    labelloc="t";')
        lines.append(f'    label="{title}";')
    lines.append('')
    
    # Channels
    lines.append('    // Channels')
    for name, channel in p["channels"].items():
        ch_type = channel["type"]
        value = channel["read"]()
        
        if show_channel_values:
            value_str = stringify_truncated(value, 30)
            label = f"{name}\\n[{ch_type}]\\n{value_str}"
        else:
            label = f"{name}\\n[{ch_type}]"
        
        colors = {
            "LastValue": "lightblue",
            "Topic": "lightyellow",
            "BinaryOperator": "lightgreen",
            "Ephemeral": "lightgray",
            "Accumulator": "lightcoral"
        }
        fillcolor = colors.get(ch_type, "white")
        
        lines.append(f'    "ch_{name}" [shape=ellipse, style=filled, fillcolor="{fillcolor}", label="{label}"];')
    lines.append('')
    
    # Nodes
    lines.append('    // Nodes')
    for name, node_info in p["nodes"].items():
        active = node_info["active"]
        status = p["executed"].get(name, "not_started")
        
        if status == "succeeded":
            fillcolor = "lightgreen"
        elif status == "failed":
            fillcolor = "lightcoral"
        elif active:
            fillcolor = "lightyellow"
        else:
            fillcolor = "lightgray"
        
        status_str = f"({status})" if status != "not_started" else "(active)" if active else "(halted)"
        label = f"{name}\\n{status_str}"
        
        lines.append(f'    "node_{name}" [shape=box, style="filled,rounded", fillcolor="{fillcolor}", label="{label}"];')
    lines.append('')
    
    # Edges
    lines.append('    // Subscriptions (channel -> node)')
    for node_name, node_info in p["nodes"].items():
        for channel_name in node_info["subscribe_to"]:
            if channel_name in p["channels"]:
                lines.append(f'    "ch_{channel_name}" -> "node_{node_name}" [style=dashed, color=blue];')
    lines.append('')
    
    lines.append('    // Writes (node -> channel)')
    for node_name, node_info in p["nodes"].items():
        for output_key, channel_name in node_info["write_to"].items():
            if channel_name in p["channels"]:
                if output_key != channel_name:
                    lines.append(f'    "node_{node_name}" -> "ch_{channel_name}" [label="{output_key}", color=green];')
                else:
                    lines.append(f'    "node_{node_name}" -> "ch_{channel_name}" [color=green];')
    
    lines.append('}')
    return '\n'.join(lines)


def to_graphviz_dataflow(p, title=None, rankdir="LR"):
    """Export simplified dataflow graph showing only node-to-node connections."""
    lines = []
    lines.append('digraph PregelDataflow {')
    lines.append(f'    rankdir={rankdir};')
    lines.append('    node [fontname="Arial", shape=box, style="filled,rounded"];')
    lines.append('    edge [fontname="Arial"];')
    
    if title:
        lines.append(f'    labelloc="t";')
        lines.append(f'    label="{title}";')
    lines.append('')
    
    # Build channel -> writers mapping
    channel_writers = {}
    for node_name, node_info in p["nodes"].items():
        for channel_name in node_info["write_to"].values():
            if channel_name not in channel_writers:
                channel_writers[channel_name] = []
            channel_writers[channel_name].append(node_name)
    
    # Nodes
    lines.append('    // Nodes')
    for name, node_info in p["nodes"].items():
        active = node_info["active"]
        status = p["executed"].get(name, "not_started")
        
        if status == "succeeded":
            fillcolor = "lightgreen"
        elif status == "failed":
            fillcolor = "lightcoral"
        elif active:
            fillcolor = "lightyellow"
        else:
            fillcolor = "lightgray"
        
        lines.append(f'    "{name}" [fillcolor="{fillcolor}"];')
    lines.append('')
    
    # Dataflow edges
    lines.append('    // Dataflow edges')
    edges_added = set()
    for node_name, node_info in p["nodes"].items():
        for channel_name in node_info["subscribe_to"]:
            if channel_name in channel_writers:
                for writer_node in channel_writers[channel_name]:
                    edge_key = (writer_node, node_name, channel_name)
                    if edge_key not in edges_added:
                        lines.append(f'    "{writer_node}" -> "{node_name}" [label="{channel_name}"];')
                        edges_added.add(edge_key)
    
    lines.append('}')
    return '\n'.join(lines)


def save_graphviz(p, filepath, **kwargs):
    """Save Pregel graph to a DOT file."""
    dot = to_graphviz(p, **kwargs)
    with open(filepath, 'w') as f:
        f.write(dot)
    if p["debug"]:
        print(f"\033[32m[EXPORT] Saved Pregel graph to {filepath}\033[0m")
    return p


def save_graphviz_dataflow(p, filepath, **kwargs):
    """Save dataflow graph to a DOT file."""
    dot = to_graphviz_dataflow(p, **kwargs)
    with open(filepath, 'w') as f:
        f.write(dot)
    if p["debug"]:
        print(f"\033[32m[EXPORT] Saved dataflow graph to {filepath}\033[0m")
    return p


def render_graphviz(p, filepath, format="png", **kwargs):
    """Render Pregel graph to image. Requires Graphviz installed."""
    import subprocess
    import shutil
    
    if not shutil.which("dot"):
        if p["debug"]:
            print("\033[33m[WARNING] Graphviz 'dot' not found. Install Graphviz to render.\033[0m")
        return p
    
    dot_content = to_graphviz(p, **kwargs)
    output_path = f"{filepath}.{format}"
    
    try:
        result = subprocess.run(
            ["dot", f"-T{format}", "-o", output_path],
            input=dot_content, text=True, capture_output=True, timeout=30
        )
        if result.returncode == 0:
            if p["debug"]:
                print(f"\033[32m[RENDER] Saved image to {output_path}\033[0m")
        elif p["debug"]:
            print(f"\033[31m[ERROR] Graphviz: {result.stderr}\033[0m")
    except Exception as e:
        if p["debug"]:
            print(f"\033[31m[ERROR] Render failed: {e}\033[0m")
    
    return p


def render_graphviz_dataflow(p, filepath, format="png", **kwargs):
    """Render dataflow graph to image."""
    import subprocess
    import shutil
    
    if not shutil.which("dot"):
        if p["debug"]:
            print("\033[33m[WARNING] Graphviz 'dot' not found.\033[0m")
        return p
    
    dot_content = to_graphviz_dataflow(p, **kwargs)
    output_path = f"{filepath}.{format}"
    
    try:
        result = subprocess.run(
            ["dot", f"-T{format}", "-o", output_path],
            input=dot_content, text=True, capture_output=True, timeout=30
        )
        if result.returncode == 0 and p["debug"]:
            print(f"\033[32m[RENDER] Saved dataflow image to {output_path}\033[0m")
    except Exception:
        pass
    
    return p


# =============================================================================
# ANIMATION SUPPORT
# =============================================================================

def run_step(p):
    """
    Execute a single superstep. Returns (p, step_info) where step_info contains:
    - superstep: The superstep number
    - executed_nodes: List of nodes that executed
    - channel_writes: Dict of {channel_name: new_value} for channels that changed
    - halted_nodes: List of nodes that voted to halt
    - active_nodes: List of nodes still active
    
    Returns (p, None) if no nodes were ready to execute.
    """
    # Initialize execution state if needed
    for name in p["nodes"]:
        if name not in p["executed"]:
            p["executed"][name] = "not_started"
    
    ready_nodes = _plan(p)
    
    if not ready_nodes:
        return p, None
    
    # Capture state before execution
    channel_values_before = {name: ch["read"]() for name, ch in p["channels"].items()}
    
    _consume_messages(p)
    
    # Track which nodes execute and halt
    executed_nodes = []
    halted_nodes = []
    
    if p["parallel"] and len(ready_nodes) > 1:
        _execute_nodes_parallel(p, ready_nodes)
        executed_nodes = list(ready_nodes)
    else:
        for node_name in ready_nodes:
            _execute_node(p, node_name)
            executed_nodes.append(node_name)
    
    # Check which nodes halted
    for node_name in executed_nodes:
        if not p["nodes"][node_name]["active"]:
            halted_nodes.append(node_name)
    
    _update(p)
    
    # Capture state after execution
    channel_values_after = {name: ch["read"]() for name, ch in p["channels"].items()}
    
    # Find which channels changed
    channel_writes = {}
    for name in channel_values_after:
        if channel_values_before.get(name) != channel_values_after[name]:
            channel_writes[name] = channel_values_after[name]
    
    step_info = {
        "superstep": p["superstep"],
        "executed_nodes": executed_nodes,
        "channel_writes": channel_writes,
        "halted_nodes": halted_nodes,
        "active_nodes": [n for n, info in p["nodes"].items() if info["active"]]
    }
    
    p["superstep"] += 1
    
    return p, step_info


def _format_value_for_display(value, max_len=15):
    """Format a channel value for clean display in visualizations."""
    if value is None:
        return "∅"
    elif isinstance(value, dict):
        # Extract meaningful values from dicts (e.g., {'rank': 0.33, 'iteration': 1})
        if 'rank' in value:
            return f"{value['rank']:.3f}"
        elif 'value' in value:
            return f"{value['value']}"
        elif len(value) == 1:
            return _format_value_for_display(list(value.values())[0], max_len)
        else:
            # Show key count for complex dicts
            return f"{{...{len(value)}}}"
    elif isinstance(value, (list, tuple)):
        if len(value) == 0:
            return "[]"
        elif len(value) <= 3:
            items = [_format_value_for_display(v, 5) for v in value]
            return f"[{','.join(items)}]"
        else:
            return f"[...{len(value)}]"
    elif isinstance(value, float):
        if abs(value) < 0.001 and value != 0:
            return f"{value:.2e}"
        elif abs(value) >= 1000:
            return f"{value:.1f}"
        else:
            return f"{value:.3f}"
    elif isinstance(value, int):
        return str(value)
    elif isinstance(value, str):
        if len(value) > max_len:
            return value[:max_len-2] + ".."
        return value
    else:
        s = str(value)
        if len(s) > max_len:
            return s[:max_len-2] + ".."
        return s


def to_graphviz_animated(p, step_info=None, title=None, rankdir="TB"):
    """
    Export Pregel graph to Graphviz DOT with animation highlights.
    
    Args:
        p: Pregel graph state
        step_info: Output from run_step() - highlights recent changes
        title: Graph title
        rankdir: Graph direction (TB=top-bottom recommended for clarity)
    
    Returns:
        DOT string with highlighted nodes/channels
    """
    executed = step_info.get("executed_nodes", []) if step_info else []
    changed_channels = set(step_info.get("channel_writes", {}).keys()) if step_info else set()
    halted = step_info.get("halted_nodes", []) if step_info else []
    superstep = step_info.get("superstep", p["superstep"]) if step_info else p["superstep"]
    
    lines = []
    lines.append('digraph PregelGraph {')
    lines.append(f'    rankdir={rankdir};')
    lines.append('    splines=spline;')
    lines.append('    nodesep=0.6;')
    lines.append('    ranksep=0.8;')
    lines.append('    pad=0.3;')
    lines.append('    bgcolor="#0f0f1a";')
    lines.append('    ')
    lines.append('    node [fontname="SF Pro Display, Helvetica Neue, Arial", fontsize=10];')
    lines.append('    edge [fontname="SF Pro Display, Helvetica Neue, Arial", fontsize=8];')
    
    # Title
    display_title = f"{title} · Step {superstep}" if title else f"Step {superstep}"
    lines.append(f'    labelloc="t";')
    lines.append(f'    label=<<font point-size="16" color="#ffffff"><b>{display_title}</b></font>>;')
    lines.append('')
    
    # Group channels and nodes for cleaner layout
    lines.append('    // Compute Nodes (main actors)')
    for name, node_info in p["nodes"].items():
        active = node_info["active"]
        
        # Collect primary state value for display (skip inbox/message channels)
        primary_value = None
        for ch_name in node_info["subscribe_to"]:
            if ch_name in p["channels"]:
                # Skip inbox/message channels - they're transient
                if 'inbox' in ch_name.lower() or 'msg' in ch_name.lower():
                    continue
                val = p["channels"][ch_name]["read"]()
                if val is not None and val != 0 and val != [] and val != {}:
                    primary_value = _format_value_for_display(val)
                    break
        
        values_str = primary_value if primary_value else ""
        
        # Node styling based on state - simplified to 3 clear states:
        # Green = just executed this step
        # Blue = waiting/idle (will execute later)
        # Gray = halted/done (won't execute again)
        if name in executed:
            # Just ran this step - highlight green
            fillcolor = "#16a34a"
            bordercolor = "#22c55e"
            fontcolor = "white"
            penwidth = "3"
        elif name in halted or not active:
            # Done - won't run again
            fillcolor = "#374151"
            bordercolor = "#4b5563"
            fontcolor = "#9ca3af"
            penwidth = "1.5"
        else:
            # Idle - waiting to execute
            fillcolor = "#2563eb"
            bordercolor = "#3b82f6"
            fontcolor = "white"
            penwidth = "2"
        
        # Clean node name for display
        display_name = name.replace('vertex_', '').replace('node_', '')
        
        if values_str:
            # Larger node with clear white text for both name and value
            label = f'<<table border="0" cellborder="0" cellspacing="2"><tr><td><font point-size="16" color="{fontcolor}"><b>{display_name}</b></font></td></tr><tr><td><font point-size="13" color="#ffffff">{values_str}</font></td></tr></table>>'
            node_size = "1.1"
        else:
            label = f'<<font point-size="16" color="{fontcolor}"><b>{display_name}</b></font>>'
            node_size = "0.9"
        
        lines.append(f'    "{name}" [shape=circle, width={node_size}, height={node_size}, fixedsize=true, style="filled", fillcolor="{fillcolor}", color="{bordercolor}", penwidth={penwidth}, label={label}];')
    lines.append('')
    
    # Build edge map from nodes to understand the graph structure
    # We want to show node->node edges based on write_to -> subscribe_to relationships
    lines.append('    // Data Flow Edges')
    
    # Create a map: channel -> list of nodes that subscribe to it
    channel_subscribers = {}
    for node_name, node_info in p["nodes"].items():
        for ch_name in node_info["subscribe_to"]:
            if ch_name not in channel_subscribers:
                channel_subscribers[ch_name] = []
            channel_subscribers[ch_name].append(node_name)
    
    # Now create edges: if node A writes to channel X, and node B subscribes to X, draw A->B
    edges_drawn = set()
    for src_node, node_info in p["nodes"].items():
        for output_key, channel_name in node_info["write_to"].items():
            # Find who subscribes to this channel
            subscribers = channel_subscribers.get(channel_name, [])
            for dst_node in subscribers:
                if dst_node != src_node:  # Skip self-loops for clarity
                    edge_key = (src_node, dst_node, channel_name)
                    if edge_key not in edges_drawn:
                        edges_drawn.add(edge_key)
                        
                        # Determine if this edge was active
                        is_active = src_node in executed and channel_name in changed_channels
                        
                        if is_active:
                            color = "#22c55e"
                            penwidth = "2.5"
                            style = "bold"
                        else:
                            color = "#4b5563"
                            penwidth = "1"
                            style = "solid"
                        
                        # No edge labels - the graph structure is self-explanatory
                        lines.append(f'    "{src_node}" -> "{dst_node}" [color="{color}", penwidth={penwidth}, style={style}];')
    
    # Skip self-loops - they add visual clutter without adding information
    
    lines.append('')
    
    # Legend with all 3 states
    lines.append('    // Legend')
    lines.append('    subgraph cluster_legend {')
    lines.append('        label="";')
    lines.append('        style=invis;')
    lines.append('        margin=5;')
    lines.append('        ')
    lines.append('        leg1 [shape=circle, width=0.3, style=filled, fillcolor="#16a34a", label="", xlabel=<<font point-size="9" color="#888888">ran</font>>];')
    lines.append('        leg2 [shape=circle, width=0.3, style=filled, fillcolor="#2563eb", label="", xlabel=<<font point-size="9" color="#888888">idle</font>>];')
    lines.append('        leg3 [shape=circle, width=0.3, style=filled, fillcolor="#374151", label="", xlabel=<<font point-size="9" color="#888888">done</font>>];')
    lines.append('        leg1 -> leg2 -> leg3 [style=invis];')
    lines.append('    }')
    
    lines.append('}')
    return '\n'.join(lines)


def _generate_pregel_summary_frame(metrics, title=None, min_width=None, min_height=None):
    """Generate a DOT graph showing Pregel execution summary statistics.
    
    Args:
        metrics: Execution metrics dict
        title: Optional title
        min_width: Target width in pixels (to match animation frame size)
        min_height: Target height in pixels (to match animation frame size)
    """
    # Scale font sizes based on frame dimensions, but keep table compact
    base_width = 600
    if min_width:
        scale = min(min_width / base_width, 1.8)  # Cap at 1.8x to stay compact
        scale = max(scale, 0.8)  # Don't go too small
    else:
        scale = 1.0
    
    title_size = int(14 * scale)
    header_size = int(11 * scale)
    text_size = int(9 * scale)
    small_size = int(8 * scale)
    cell_padding = int(5 * scale)
    
    lines = []
    lines.append('digraph Summary {')
    lines.append('    rankdir=TB;')
    lines.append('    bgcolor="#0f0f1a";')
    lines.append('    dpi=150;')
    
    # Constrain size to fit within animation frame
    if min_width and min_height:
        w_inches = min_width / 150
        h_inches = min_height / 150
        lines.append(f'    size="{w_inches:.2f},{h_inches:.2f}!";')
    
    lines.append('    margin=0.2;')
    lines.append('    pad=0.1;')
    lines.append('')
    lines.append('    node [fontname="Helvetica", shape=none];')
    lines.append('')
    
    # Title
    display_title = title or "Pregel Graph"
    lines.append(f'    labelloc="t";')
    lines.append(f'    label=<<font point-size="{title_size}" color="#22c55e"><b>{display_title} - COMPLETE</b></font>>;')
    lines.append('')
    
    # Build summary table with scaled font sizes
    lines.append('    summary [label=<')
    lines.append(f'        <table border="0" cellborder="1" cellspacing="0" cellpadding="{cell_padding}" bgcolor="#1c1917">')
    
    # Header
    lines.append(f'            <tr><td colspan="2" bgcolor="#292524"><font point-size="{header_size}" color="#3b82f6"><b>EXECUTION SUMMARY</b></font></td></tr>')
    
    # Pregel section
    lines.append(f'            <tr><td align="left"><font point-size="{text_size}" color="#a8a29e">Nodes</font></td><td align="right"><font point-size="{text_size}" color="#ffffff">{metrics["nodes"]}</font></td></tr>')
    lines.append(f'            <tr><td align="left"><font point-size="{text_size}" color="#a8a29e">Channels</font></td><td align="right"><font point-size="{text_size}" color="#ffffff">{metrics["channels"]}</font></td></tr>')
    lines.append(f'            <tr><td align="left"><font point-size="{text_size}" color="#a8a29e">Supersteps</font></td><td align="right"><font point-size="{text_size}" color="#ffffff">{metrics["supersteps"]}</font></td></tr>')
    lines.append(f'            <tr><td align="left"><font point-size="{text_size}" color="#a8a29e">Node Executions</font></td><td align="right"><font point-size="{text_size}" color="#ffffff">{metrics["node_executions"]}</font></td></tr>')
    lines.append(f'            <tr><td align="left"><font point-size="{text_size}" color="#a8a29e">Messages Sent</font></td><td align="right"><font point-size="{text_size}" color="#ffffff">{metrics["messages_sent"]}</font></td></tr>')
    
    # Highlight max parallel if > 1
    max_parallel = metrics.get("max_parallel", 1)
    if max_parallel > 1:
        lines.append(f'            <tr><td align="left"><font point-size="{text_size}" color="#a8a29e">Max Parallel</font></td><td align="right"><font point-size="{text_size}" color="#22c55e"><b>{max_parallel}</b></font></td></tr>')
    else:
        lines.append(f'            <tr><td align="left"><font point-size="{text_size}" color="#a8a29e">Max Parallel</font></td><td align="right"><font point-size="{text_size}" color="#ffffff">{max_parallel}</font></td></tr>')
    
    # Show parallel execution history if available
    parallel_history = metrics.get("parallel_history", [])
    if parallel_history and any(count > 1 for count in parallel_history):
        # Show parallelization per superstep
        parallel_str = " ".join([f"<font color='#22c55e'>{c}</font>" if c > 1 else str(c) for c in parallel_history])
        lines.append(f'            <tr><td align="left"><font point-size="{text_size}" color="#a8a29e">Per-Step</font></td><td align="right"><font point-size="{small_size}">{parallel_str}</font></td></tr>')
    
    # Frames
    lines.append(f'            <tr><td align="left"><font point-size="{text_size}" color="#a8a29e">Frames</font></td><td align="right"><font point-size="{text_size}" color="#ffffff">{metrics["total_frames"]}</font></td></tr>')
    
    # Parallelism indicator
    if max_parallel > 1:
        lines.append(f'            <tr><td colspan="2" bgcolor="#16a34a"><font point-size="{text_size}" color="#ffffff"><b>PARALLEL EXECUTION</b></font></td></tr>')
    
    lines.append('        </table>')
    lines.append('    >];')
    lines.append('}')
    
    return '\n'.join(lines)


def _print_pregel_summary(metrics, title=None):
    """Print Pregel execution summary to console."""
    print()
    print("\033[34m" + "=" * 50 + "\033[0m")
    print(f"\033[34m  PREGEL SUMMARY: {title or 'Animation'}\033[0m")
    print("\033[34m" + "=" * 50 + "\033[0m")
    print()
    print(f"    Nodes:            {metrics['nodes']}")
    print(f"    Channels:         {metrics['channels']}")
    print(f"    Supersteps:       {metrics['supersteps']}")
    print(f"    Node executions:  {metrics['node_executions']}")
    print(f"    Messages sent:    {metrics['messages_sent']}")
    
    # Parallelization stats
    max_parallel = metrics.get('max_parallel', 1)
    if max_parallel > 1:
        print(f"    Max parallel:     {max_parallel} \033[32m(parallel!)\033[0m")
    else:
        print(f"    Max parallel:     {max_parallel}")
    
    # Show per-superstep parallelization history
    parallel_history = metrics.get("parallel_history", [])
    execution_history = metrics.get("execution_history", [])
    
    if parallel_history:
        # Show which supersteps had parallel execution
        parallel_steps = [(i, count) for i, count in enumerate(parallel_history) if count > 1]
        if parallel_steps:
            print(f"    Parallel steps:   {len(parallel_steps)}/{len(parallel_history)}")
            print(f"    Per-step nodes:   {parallel_history}")
    
    # Show detailed execution history if available
    if execution_history:
        print()
        print("\033[36m  EXECUTION HISTORY:\033[0m")
        for step_info in execution_history:
            step = step_info.get("superstep", "?")
            nodes = step_info.get("executed_nodes", [])
            if len(nodes) > 1:
                print(f"    Step {step}: \033[32m{nodes}\033[0m  (parallel: {len(nodes)})")
            elif nodes:
                print(f"    Step {step}: {nodes}")
    
    print()
    print(f"    Total frames:     {metrics['total_frames']}")
    print()
    print("\033[34m" + "=" * 50 + "\033[0m")
    print()


def render_animation(p, output_path, max_supersteps=20, frame_delay=100, title=None, format="gif"):
    """
    Run computation and render animated GIF showing each superstep.
    
    Args:
        p: Pregel graph state (will be modified during execution)
        output_path: Base path for output (without extension)
        max_supersteps: Maximum supersteps to run
        frame_delay: Delay between frames in centiseconds (100 = 1 second)
        title: Title for the animation
        format: Output format ("gif" or "mp4")
    
    Returns:
        Tuple of (p, metrics) where metrics contains execution statistics
    """
    import subprocess
    import shutil
    import tempfile
    import os
    
    # Initialize metrics with parallelization tracking
    metrics = {
        "nodes": len(p.get("nodes", {})),
        "channels": len(p.get("channels", {})),
        "supersteps": 0,
        "node_executions": 0,
        "messages_sent": 0,
        "max_parallel": 0,
        "total_frames": 0,
        "parallel_history": [],      # Nodes executed per superstep
        "execution_history": [],     # Detailed per-step info
    }
    
    if not shutil.which("dot"):
        if p["debug"]:
            print("\033[33m[WARNING] Graphviz 'dot' not found. Cannot render animation.\033[0m")
        return p, metrics
    
    if not shutil.which("magick"):
        if p["debug"]:
            print("\033[33m[WARNING] ImageMagick 'magick' not found. Cannot create GIF.\033[0m")
        return p, metrics
    
    # Create temp directory for frames
    with tempfile.TemporaryDirectory() as tmpdir:
        frames = []
        frame_num = 0
        frame_dimensions = None  # Track animation frame size for summary
        
        # Initial frame (before any execution)
        initial_info = {
            "superstep": 0,
            "executed_nodes": [],
            "channel_writes": {},
            "halted_nodes": [],
            "active_nodes": [n for n, info in p["nodes"].items() if info["active"]]
        }
        dot_content = to_graphviz_animated(p, initial_info, title=title)
        frame_path = os.path.join(tmpdir, f"frame_{frame_num:04d}.png")
        
        result = subprocess.run(
            ["dot", "-Tpng", "-o", frame_path],
            input=dot_content, text=True, capture_output=True, timeout=30
        )
        if result.returncode == 0:
            frames.append(frame_path)
            frame_num += 1
            # Get frame dimensions for consistent sizing
            if frame_dimensions is None:
                try:
                    identify_result = subprocess.run(
                        ["identify", "-format", "%w %h", frame_path],
                        capture_output=True, text=True, timeout=10
                    )
                    if identify_result.returncode == 0:
                        w, h = identify_result.stdout.strip().split()
                        frame_dimensions = (int(w), int(h))
                except Exception:
                    pass
        
        # Run step by step
        for _ in range(max_supersteps):
            p, step_info = run_step(p)
            if step_info is None:
                break
            
            # Track metrics
            metrics["supersteps"] += 1
            executed = step_info.get("executed_nodes", [])
            num_executed = len(executed)
            metrics["node_executions"] += num_executed
            metrics["max_parallel"] = max(metrics["max_parallel"], num_executed)
            metrics["parallel_history"].append(num_executed)
            metrics["execution_history"].append({
                "superstep": step_info.get("superstep", metrics["supersteps"]),
                "executed_nodes": list(executed),
                "parallel_count": num_executed
            })
            
            # Estimate messages sent
            for node_name in executed:
                node_info = p["nodes"].get(node_name, {})
                metrics["messages_sent"] += len(node_info.get("write_to", {}))
            
            dot_content = to_graphviz_animated(p, step_info, title=title)
            frame_path = os.path.join(tmpdir, f"frame_{frame_num:04d}.png")
            
            result = subprocess.run(
                ["dot", "-Tpng", "-o", frame_path],
                input=dot_content, text=True, capture_output=True, timeout=30
            )
            if result.returncode == 0:
                frames.append(frame_path)
                frame_num += 1
            
            if p["debug"]:
                parallel_indicator = " \033[32m(parallel)\033[0m" if num_executed > 1 else ""
                print(f"\033[35m[ANIM] Frame {frame_num}: superstep={step_info['superstep']}, executed={executed}{parallel_indicator}\033[0m")
        
        # Add final "done" frame showing all nodes as completed (gray)
        final_info = {
            "superstep": p["superstep"],
            "executed_nodes": [],  # No nodes executing
            "channel_writes": {},
            "halted_nodes": list(p["nodes"].keys()),  # All nodes halted
            "active_nodes": []
        }
        dot_content = to_graphviz_animated(p, final_info, title=title)
        frame_path = os.path.join(tmpdir, f"frame_{frame_num:04d}.png")
        
        result = subprocess.run(
            ["dot", "-Tpng", "-o", frame_path],
            input=dot_content, text=True, capture_output=True, timeout=30
        )
        if result.returncode == 0:
            frames.append(frame_path)
            frame_num += 1
        
        # Add summary frame - constrained to fit within animation frame dimensions
        metrics["total_frames"] = len(frames) + 1  # +1 for summary
        
        # Generate summary with size constraints to match animation frames
        min_w = frame_dimensions[0] if frame_dimensions else None
        min_h = frame_dimensions[1] if frame_dimensions else None
        dot_content = _generate_pregel_summary_frame(metrics, title, min_width=min_w, min_height=min_h)
        frame_path = os.path.join(tmpdir, f"frame_{frame_num:04d}.png")
        
        result = subprocess.run(
            ["dot", "-Tpng", "-o", frame_path],
            input=dot_content, text=True, capture_output=True, timeout=30
        )
        if result.returncode == 0:
            frames.append(frame_path)
        
        if len(frames) < 2:
            if p["debug"]:
                print("\033[33m[WARNING] Not enough frames for animation.\033[0m")
            return p, metrics
        
        # Create GIF using ImageMagick with consistent frame sizes
        output_file = f"{output_path}.{format}"
        
        try:
            # Build command with per-frame delays and size normalization
            cmd = ["magick"]
            
            # Normal frames
            for f in frames[:-2]:
                cmd.extend(["-delay", str(frame_delay), f])
            
            # Done frame with 2x delay
            if len(frames) >= 2:
                cmd.extend(["-delay", str(frame_delay * 2), frames[-2]])
            
            # Summary frame with 3 second delay
            cmd.extend(["-delay", "300", frames[-1]])
            
            # Use -coalesce and -extent to ensure consistent frame sizes
            # Uses max of animation frames and summary frame dimensions
            if frame_dimensions:
                cmd.extend([
                    "-coalesce",
                    "-gravity", "center",
                    "-background", "#0f0f1a",
                    "-extent", f"{frame_dimensions[0]}x{frame_dimensions[1]}",
                ])
            
            cmd.extend([
                "-loop", "0",  # Infinite loop
                "-dispose", "previous",
                output_file
            ])
            
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=60)
            
            if result.returncode == 0:
                if p["debug"]:
                    print(f"\033[32m[ANIM] Created animation: {output_file} ({len(frames)} frames)\033[0m")
            else:
                if p["debug"]:
                    print(f"\033[31m[ERROR] ImageMagick failed: {result.stderr}\033[0m")
        except Exception as e:
            if p["debug"]:
                print(f"\033[31m[ERROR] Animation creation failed: {e}\033[0m")
        
        # Print summary to console
        _print_pregel_summary(metrics, title)
    
    return p, metrics


def collect_animation_frames(p, max_supersteps=20, title=None):
    """
    Run computation and collect DOT strings for each frame.
    
    Args:
        p: Pregel graph state
        max_supersteps: Maximum supersteps
        title: Animation title
    
    Returns:
        (p, frames) where frames is a list of DOT strings
    """
    frames = []
    
    # Initial frame
    initial_info = {
        "superstep": 0,
        "executed_nodes": [],
        "channel_writes": {},
        "halted_nodes": [],
        "active_nodes": [n for n, info in p["nodes"].items() if info["active"]]
    }
    frames.append(to_graphviz_animated(p, initial_info, title=title))
    
    # Run step by step
    for _ in range(max_supersteps):
        p, step_info = run_step(p)
        if step_info is None:
            break
        frames.append(to_graphviz_animated(p, step_info, title=title))
    
    return p, frames


# =============================================================================
# STATE ACCESSORS (for composability)
# =============================================================================

def get_nodes(p):
    """Get nodes dict from graph."""
    return p["nodes"]

def get_channels(p):
    """Get channels dict from graph."""
    return p["channels"]

def get_superstep(p):
    """Get current superstep number."""
    return p["superstep"]

def get_executed(p):
    """Get execution status dict."""
    return p["executed"]


# =============================================================================
# OOP WRAPPER CLASS FOR BACKWARD COMPATIBILITY
# =============================================================================

class Pregel:
    """
    Object-oriented wrapper for backward compatibility.
    
    Internally uses the functional API but provides method-style access:
        pregel = Pregel(debug=True)
        pregel.add_channel("input", "LastValue", initial_value=10)
        pregel.add_node("doubler", func, subscribe_to=["input"], write_to={"out": "output"})
        pregel.run()
    """
    
    def __init__(self, debug=True, parallel=False, max_workers=None):
        self._p = pregel(debug=debug, parallel=parallel, max_workers=max_workers)
    
    def add_channel(self, name, channel_type="LastValue", **kwargs):
        self._p = add_channel(self._p, name, channel_type, **kwargs)
        return self
    
    def add_node(self, name, func, subscribe_to=None, write_to=None):
        self._p = add_node(self._p, name, func, subscribe_to, write_to)
        return self
    
    def add_to_registry(self, name, subscribe_to=None, write_to=None):
        def decorator(func):
            self.add_node(name, func, subscribe_to, write_to)
            return func
        return decorator
    
    def run(self, max_supersteps=1000):
        return run(self._p, max_supersteps)
    
    def reset(self):
        self._p = reset(self._p)
        return self
    
    def get_checkpoint(self):
        return get_checkpoint(self._p)
    
    def restore_checkpoint(self, checkpoint):
        self._p = restore_checkpoint(self._p, checkpoint)
        return self
    
    def save_to_db(self, db_path):
        self._p = save_to_db(self._p, db_path)
        return self
    
    def load_from_db(self, db_path):
        self._p, success = load_from_db(self._p, db_path)
        return success
    
    def to_graphviz(self, **kwargs):
        return to_graphviz(self._p, **kwargs)
    
    def to_graphviz_dataflow(self, **kwargs):
        return to_graphviz_dataflow(self._p, **kwargs)
    
    def save_graphviz(self, filepath, **kwargs):
        self._p = save_graphviz(self._p, filepath, **kwargs)
        return self
    
    def save_graphviz_dataflow(self, filepath, **kwargs):
        self._p = save_graphviz_dataflow(self._p, filepath, **kwargs)
        return self
    
    def render_graphviz(self, filepath, format="png", **kwargs):
        self._p = render_graphviz(self._p, filepath, format, **kwargs)
        return self
    
    def render_graphviz_dataflow(self, filepath, format="png", **kwargs):
        self._p = render_graphviz_dataflow(self._p, filepath, format, **kwargs)
        return self
    
    @property
    def nodes(self):
        return get_nodes(self._p)
    
    @property
    def channels(self):
        return get_channels(self._p)
    
    @property
    def superstep(self):
        return get_superstep(self._p)
    
    @property
    def executed(self):
        return get_executed(self._p)
    
    @property
    def debug(self):
        return self._p["debug"]
    
    @property
    def parallel(self):
        return self._p["parallel"]
    
    @property
    def max_workers(self):
        return self._p["max_workers"]


class PregelParallel(Pregel):
    """Parallel Pregel executor - convenience subclass."""
    
    def __init__(self, debug=True, max_workers=None, use_processes=False):
        super().__init__(debug=debug, parallel=True, max_workers=max_workers)
        self._use_processes = use_processes
        self.channel_configs = {}
    
    def add_channel(self, name, channel_type="LastValue", **kwargs):
        self.channel_configs[name] = {"type": channel_type, "kwargs": kwargs}
        return super().add_channel(name, channel_type, **kwargs)
    
    def execute_nodes(self, ready_nodes):
        return _execute_nodes_parallel(self._p, ready_nodes)
    
    def plan(self):
        return _plan(self._p)
    
    def consume_messages(self):
        return _consume_messages(self._p)
    
    def update(self):
        return _update(self._p)
    
    def shutdown(self):
        pass
    
    @property
    def use_processes(self):
        return self._use_processes


# =============================================================================
# HELPER FUNCTIONS
# =============================================================================

def create_node_builder(name):
    """Create a fluent builder for nodes."""
    builder = {
        "name": name,
        "subscribe_to": [],
        "write_to": {},
        "func": None
    }
    
    def subscribe_to_fn(*channels):
        builder["subscribe_to"].extend(channels)
        return builder
    
    def write_to_fn(**channel_mappings):
        builder["write_to"].update(channel_mappings)
        return builder
    
    def do_fn(func):
        builder["func"] = func
        return builder
    
    def build_fn():
        if not builder["func"]:
            raise ValueError(f"\033[31mNode {name} has no compute function\033[0m")
        return builder
    
    builder["subscribe_to_fn"] = subscribe_to_fn
    builder["write_to_fn"] = write_to_fn
    builder["do_fn"] = do_fn
    builder["build_fn"] = build_fn
    
    return builder


def add_nodes_to_pregel(p, *node_builders):
    """Add multiple nodes from builders to a Pregel graph."""
    for builder in node_builders:
        node = builder["build_fn"]()
        if isinstance(p, Pregel):
            p.add_node(node["name"], node["func"], node["subscribe_to"], node["write_to"])
        else:
            p = add_node(p, node["name"], node["func"], node["subscribe_to"], node["write_to"])
    return p


def create_pregel_graph(config, debug=True, parallel=False, max_workers=None):
    """Create a Pregel graph from a configuration dict."""
    p = Pregel(debug=debug, parallel=parallel, max_workers=max_workers)
    
    for channel_config in config.get("channels", []):
        name = channel_config["name"]
        channel_type = channel_config.get("type", "LastValue")
        kwargs = {k: v for k, v in channel_config.items() if k not in ["name", "type"]}
        p.add_channel(name, channel_type, **kwargs)
    
    for node_config in config.get("nodes", []):
        name = node_config["name"]
        func = node_config["func"]
        subscribe_to = node_config.get("subscribe_to", [])
        write_to = node_config.get("write_to", {})
        p.add_node(name, func, subscribe_to, write_to)
    
    return p


def run_pregel_from_config(config, max_supersteps=1000, debug=True, parallel=False):
    """Create and run a Pregel graph from a configuration dict."""
    p = create_pregel_graph(config, debug=debug, parallel=parallel)
    return p.run(max_supersteps=max_supersteps)


# =============================================================================
# FUNCTIONAL ALIASES (for Lisp-style composition)
# =============================================================================

# These allow: run(add_node(add_channel(pregel(), ...), ...))
# Already defined above as standalone functions
