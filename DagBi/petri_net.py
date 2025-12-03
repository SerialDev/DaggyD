"""
Petri Net implementation in Lisp-like functional style.

This module provides composable functions for building Petri nets:
- Functions take data as first argument, return modified data
- Enables chaining/composition like Lisp

Example (Lisp-style composition):
    result = run(
        add_transition(
            add_place(
                add_place(petri_net(), "p1", initial_tokens=1),
                "p2"
            ),
            "t1", consume_from=[("p1", 1)], produce_to=[("p2", 1)]
        )
    )

Example (imperative with returned state):
    net = petri_net()
    net = add_place(net, "buffer", capacity=5, initial_tokens=0)
    net = add_place(net, "empty", capacity=5, initial_tokens=5)
    net = add_transition(net, "produce", consume_from=[("empty", 1)], produce_to=[("buffer", 1)])
    net = add_transition(net, "consume", consume_from=[("buffer", 1)], produce_to=[("empty", 1)])
    result = run(net, max_steps=100)

Petri Net Concepts:
- Places: Hold tokens (implemented as Place channels)
- Transitions: Fire when enabled, consuming and producing tokens
- Input Arcs: Connect places to transitions (with weights)
- Output Arcs: Connect transitions to places (with weights)
- Inhibitor Arcs: Prevent firing when place has tokens
"""

from collections import deque
from typing import Dict, List, Tuple, Optional, Callable, Any
import json


# =============================================================================
# CORE DATA CONSTRUCTOR
# =============================================================================

def petri_net(debug=True):
    """
    Create a new Petri net data structure.
    
    Returns a dict representing the Petri net state.
    All operations return new/modified state rather than mutating.
    """
    if debug:
        print("\033[36m[INIT] Petri Net initialized\033[0m")
    
    return {
        "type": "PetriNet",
        "places": {},
        "transitions": {},
        "step": 0,
        "debug": debug,
        "firing_history": []
    }


# =============================================================================
# PLACE OPERATIONS
# =============================================================================

def add_place(net, name, capacity=float('inf'), initial_tokens=0):
    """
    Add a place to the Petri net. Returns new net state.
    
    Args:
        net: Petri net state
        name: Place name
        capacity: Maximum tokens (default: unlimited)
        initial_tokens: Starting token count
    
    Returns:
        New Petri net state with place added
    """
    from .channels import create_place_channel
    
    if name in net["places"]:
        raise ValueError(f"Place '{name}' already exists")
    
    place = create_place_channel(capacity=capacity, initial_tokens=initial_tokens)
    
    cap_str = str(capacity) if capacity != float('inf') else "inf"
    if net["debug"]:
        print(f"\033[36m[PLACE] Added '{name}' (capacity={cap_str}, tokens={initial_tokens})\033[0m")
    
    new_places = dict(net["places"])
    new_places[name] = place
    return {**net, "places": new_places}


def get_marking(net):
    """Get current marking (token distribution) as a dict."""
    return {name: place["read"]() for name, place in net["places"].items()}


def set_marking(net, marking):
    """Set the marking (token distribution). Returns modified net."""
    for name, tokens in marking.items():
        if name in net["places"]:
            net["places"][name]["restore"](tokens)
    return net


# =============================================================================
# TRANSITION OPERATIONS
# =============================================================================

def add_transition(net, name, consume_from=None, produce_to=None, 
                   inhibitor_from=None, action=None, priority=0):
    """
    Add a transition to the Petri net. Returns new net state.
    
    Args:
        net: Petri net state
        name: Transition name
        consume_from: List of (place_name, weight) tuples for input arcs
        produce_to: List of (place_name, weight) tuples for output arcs
        inhibitor_from: List of place names that block firing when non-empty
        action: Optional function to execute when transition fires
        priority: Higher priority transitions fire first
    
    Returns:
        New Petri net state with transition added
    """
    if name in net["transitions"]:
        raise ValueError(f"Transition '{name}' already exists")
    
    consume_from = consume_from or []
    produce_to = produce_to or []
    inhibitor_from = inhibitor_from or []
    
    # Validate places exist
    for place, weight in consume_from:
        if place not in net["places"]:
            raise ValueError(f"Place '{place}' not found for transition '{name}'")
    
    for place, weight in produce_to:
        if place not in net["places"]:
            raise ValueError(f"Place '{place}' not found for transition '{name}'")
    
    for place in inhibitor_from:
        if place not in net["places"]:
            raise ValueError(f"Place '{place}' not found for inhibitor arc in '{name}'")
    
    transition = {
        "consume_from": consume_from,
        "produce_to": produce_to,
        "inhibitor_from": inhibitor_from,
        "action": action,
        "priority": priority,
        "fire_count": 0
    }
    
    if net["debug"]:
        consume_str = ", ".join(f"{p}:{w}" for p, w in consume_from) or "empty"
        produce_str = ", ".join(f"{p}:{w}" for p, w in produce_to) or "empty"
        inhibit_str = ", ".join(inhibitor_from) if inhibitor_from else "none"
        print(f"\033[36m[TRANSITION] Added '{name}' (consume=[{consume_str}], produce=[{produce_str}], inhibit=[{inhibit_str}])\033[0m")
    
    new_transitions = dict(net["transitions"])
    new_transitions[name] = transition
    return {**net, "transitions": new_transitions}


def is_enabled(net, transition_name):
    """Check if a transition is enabled (can fire)."""
    if transition_name not in net["transitions"]:
        return False
    
    trans = net["transitions"][transition_name]
    
    # Check input arcs
    for place_name, weight in trans["consume_from"]:
        if not net["places"][place_name]["can_consume"](weight):
            return False
    
    # Check output arcs (capacity)
    for place_name, weight in trans["produce_to"]:
        if not net["places"][place_name]["can_produce"](weight):
            return False
    
    # Check inhibitor arcs
    for place_name in trans["inhibitor_from"]:
        if net["places"][place_name]["read"]() > 0:
            return False
    
    return True


def get_enabled_transitions(net):
    """Get list of all enabled transitions, sorted by priority."""
    enabled = [name for name in net["transitions"] if is_enabled(net, name)]
    enabled.sort(key=lambda n: (-net["transitions"][n]["priority"], n))
    return enabled


def fire_transition(net, transition_name):
    """Fire a single transition. Returns (modified_net, action_result)."""
    if not is_enabled(net, transition_name):
        raise RuntimeError(f"Transition '{transition_name}' is not enabled")
    
    trans = net["transitions"][transition_name]
    
    # Consume tokens
    for place_name, weight in trans["consume_from"]:
        net["places"][place_name]["consume"](weight)
    
    # Execute action
    result = None
    if trans["action"]:
        inputs = {place: net["places"][place]["read"]() 
                 for place, _ in trans["consume_from"]}
        result = trans["action"](inputs) if inputs else trans["action"]()
    
    # Produce tokens
    for place_name, weight in trans["produce_to"]:
        net["places"][place_name]["write"](weight)
    
    # Commit all changes
    for place in net["places"].values():
        place["checkpoint"]()
    
    trans["fire_count"] += 1
    
    if net["debug"]:
        print(f"\033[32m[FIRE] {transition_name} -> marking={get_marking(net)}\033[0m")
    
    return net, result


# =============================================================================
# EXECUTION
# =============================================================================

def step_one(net):
    """
    Execute one step: fire all transitions that were enabled at the START of this step.
    
    This implements BSP-style parallel semantics:
    - All transitions enabled at step start fire "simultaneously"
    - Transitions that become enabled DURING the step wait for the next step
    - This ensures proper visualization of parallel execution
    
    Returns (net, list_of_fired).
    """
    # Capture enabled transitions at the START of this step only
    initially_enabled = get_enabled_transitions(net)
    if not initially_enabled:
        return net, []
    
    fired = []
    marking_before = get_marking(net)
    
    # Only fire transitions that were enabled at the start (BSP semantics)
    for trans_name in initially_enabled:
        if is_enabled(net, trans_name):
            net, _ = fire_transition(net, trans_name)
            fired.append(trans_name)
    
    history_entry = {
        "step": net["step"],
        "marking_before": marking_before,
        "fired": fired,
        "marking_after": get_marking(net)
    }
    net["firing_history"].append(history_entry)
    net["step"] += 1
    
    return net, fired


def run(net, max_steps=1000):
    """
    Run the Petri net until no transitions enabled or max_steps reached.
    
    Args:
        net: Petri net state
        max_steps: Maximum steps before stopping
    
    Returns:
        Final marking dict
    """
    if net["debug"]:
        print(f"\033[36m\n[START] Beginning Petri net execution\033[0m")
        print(f"\033[35m[MARKING] Initial: {get_marking(net)}\033[0m")
    
    for _ in range(max_steps):
        net, fired = step_one(net)
        if not fired:
            if net["debug"]:
                print(f"\033[36m[TERMINATE] No enabled transitions at step {net['step']}\033[0m")
            break
    
    final = get_marking(net)
    if net["debug"]:
        print(f"\033[36m[COMPLETE] Finished after {net['step']} steps\033[0m")
        print(f"\033[35m[MARKING] Final: {final}\033[0m")
    
    return final


def reset(net):
    """Reset the Petri net to initial state. Returns reset net."""
    for place in net["places"].values():
        place["clear"]()
    
    for trans in net["transitions"].values():
        trans["fire_count"] = 0
    
    net["step"] = 0
    net["firing_history"] = []
    
    if net["debug"]:
        print("\033[36m[RESET] Petri net reset to initial state\033[0m")
    
    return net


# =============================================================================
# REACHABILITY ANALYSIS
# =============================================================================

def compute_reachability_graph(net, max_markings=10000):
    """Compute the reachability graph via BFS."""
    original_marking = get_marking(net)
    
    marking_to_idx = {}
    markings = []
    edges = []
    deadlocks = []
    
    def marking_tuple(m):
        return tuple(sorted(m.items()))
    
    initial = get_marking(net)
    initial_tuple = marking_tuple(initial)
    marking_to_idx[initial_tuple] = 0
    markings.append(initial)
    
    queue = deque([0])
    is_complete = True
    
    while queue:
        if len(markings) >= max_markings:
            is_complete = False
            break
        
        current_idx = queue.popleft()
        current_marking = markings[current_idx]
        
        set_marking(net, current_marking)
        enabled = get_enabled_transitions(net)
        
        if not enabled:
            deadlocks.append(current_idx)
            continue
        
        for trans_name in enabled:
            set_marking(net, current_marking)
            if is_enabled(net, trans_name):
                fire_transition(net, trans_name)
                new_marking = get_marking(net)
                new_tuple = marking_tuple(new_marking)
                
                if new_tuple not in marking_to_idx:
                    new_idx = len(markings)
                    marking_to_idx[new_tuple] = new_idx
                    markings.append(new_marking)
                    queue.append(new_idx)
                else:
                    new_idx = marking_to_idx[new_tuple]
                
                edges.append((current_idx, new_idx, trans_name))
    
    # Check boundedness
    is_bounded = True
    max_tokens = {}
    for marking in markings:
        for place, tokens in marking.items():
            max_tokens[place] = max(max_tokens.get(place, 0), tokens)
            if tokens > 1000:
                is_bounded = False
    
    set_marking(net, original_marking)
    
    return {
        "markings": markings,
        "edges": edges,
        "initial_idx": 0,
        "deadlock_markings": deadlocks,
        "is_bounded": is_bounded,
        "is_complete": is_complete,
        "max_tokens": max_tokens,
        "num_markings": len(markings),
        "num_edges": len(edges)
    }


def analyze_reachability(net, max_markings=10000):
    """Perform reachability analysis and return formatted report."""
    graph = compute_reachability_graph(net, max_markings)
    
    lines = []
    lines.append("=" * 60)
    lines.append("PETRI NET REACHABILITY ANALYSIS")
    lines.append("=" * 60)
    lines.append("")
    lines.append("SUMMARY")
    lines.append("-" * 40)
    lines.append(f"  Places:          {len(net['places'])}")
    lines.append(f"  Transitions:     {len(net['transitions'])}")
    lines.append(f"  Reachable states: {graph['num_markings']}")
    lines.append(f"  Edges:           {graph['num_edges']}")
    lines.append(f"  Analysis complete: {'Yes' if graph['is_complete'] else 'No (truncated)'}")
    lines.append("")
    lines.append("PROPERTIES")
    lines.append("-" * 40)
    lines.append(f"  Bounded:         {'Yes' if graph['is_bounded'] else 'No'}")
    lines.append(f"  Deadlock-free:   {'Yes' if not graph['deadlock_markings'] else 'No'}")
    if graph['deadlock_markings']:
        lines.append(f"  Deadlock states: {len(graph['deadlock_markings'])}")
    lines.append("")
    lines.append("PLACE BOUNDS")
    lines.append("-" * 40)
    for place, max_tok in sorted(graph['max_tokens'].items()):
        capacity = net["places"][place]["get_capacity"]()
        cap_str = str(int(capacity)) if capacity != float('inf') else "inf"
        lines.append(f"  {place}: max={max_tok}, capacity={cap_str}")
    lines.append("")
    lines.append("INITIAL MARKING")
    lines.append("-" * 40)
    initial = graph['markings'][graph['initial_idx']]
    for place, tokens in sorted(initial.items()):
        lines.append(f"  {place}: {tokens}")
    lines.append("")
    if graph['deadlock_markings']:
        lines.append("DEADLOCK MARKINGS")
        lines.append("-" * 40)
        for idx in graph['deadlock_markings'][:5]:
            marking = graph['markings'][idx]
            marking_str = ", ".join(f"{p}:{t}" for p, t in sorted(marking.items()))
            lines.append(f"  State {idx}: {{{marking_str}}}")
        if len(graph['deadlock_markings']) > 5:
            lines.append(f"  ... and {len(graph['deadlock_markings']) - 5} more")
        lines.append("")
    lines.append("TRANSITION INFO")
    lines.append("-" * 40)
    for name, trans in sorted(net["transitions"].items()):
        consume = ", ".join(f"{p}:{w}" for p, w in trans["consume_from"]) or "empty"
        produce = ", ".join(f"{p}:{w}" for p, w in trans["produce_to"]) or "empty"
        lines.append(f"  {name}:")
        lines.append(f"    Input arcs:  {{{consume}}}")
        lines.append(f"    Output arcs: {{{produce}}}")
        if trans["inhibitor_from"]:
            lines.append(f"    Inhibitors:  {{{', '.join(trans['inhibitor_from'])}}}")
    lines.append("")
    lines.append("=" * 60)
    
    return "\n".join(lines)


# =============================================================================
# GRAPHVIZ EXPORT
# =============================================================================

def to_graphviz(net, show_tokens=True, show_arc_weights=True, title=None, rankdir="LR"):
    """Export Petri net structure to Graphviz DOT format."""
    lines = []
    lines.append('digraph PetriNet {')
    lines.append(f'    rankdir={rankdir};')
    lines.append('    node [fontname="Arial"];')
    lines.append('    edge [fontname="Arial"];')
    
    if title:
        lines.append(f'    labelloc="t";')
        lines.append(f'    label="{title}";')
    lines.append('')
    
    # Places
    lines.append('    // Places')
    for name, place in net["places"].items():
        tokens = place["read"]()
        capacity = place["get_capacity"]()
        
        if show_tokens:
            if capacity != float('inf'):
                label = f"{name}\\n{tokens}/{int(capacity)}"
            else:
                label = f"{name}\\n{tokens}"
        else:
            label = name
        
        if tokens == 0:
            fillcolor = "white"
        elif capacity != float('inf') and tokens >= capacity:
            fillcolor = "lightcoral"
        else:
            fillcolor = "lightblue"
        
        lines.append(f'    "{name}" [shape=circle, style=filled, fillcolor="{fillcolor}", label="{label}"];')
    lines.append('')
    
    # Transitions
    lines.append('    // Transitions')
    for name, trans in net["transitions"].items():
        enabled = is_enabled(net, name)
        fillcolor = "lightgreen" if enabled else "lightgray"
        lines.append(f'    "{name}" [shape=box, style=filled, fillcolor="{fillcolor}", label="{name}"];')
    lines.append('')
    
    # Arcs
    lines.append('    // Arcs')
    for trans_name, trans in net["transitions"].items():
        for place_name, weight in trans["consume_from"]:
            if show_arc_weights and weight != 1:
                lines.append(f'    "{place_name}" -> "{trans_name}" [label="{weight}"];')
            else:
                lines.append(f'    "{place_name}" -> "{trans_name}";')
        
        for place_name, weight in trans["produce_to"]:
            if show_arc_weights and weight != 1:
                lines.append(f'    "{trans_name}" -> "{place_name}" [label="{weight}"];')
            else:
                lines.append(f'    "{trans_name}" -> "{place_name}";')
        
        for place_name in trans["inhibitor_from"]:
            lines.append(f'    "{place_name}" -> "{trans_name}" [style=dashed, arrowhead=odot, color=red];')
    
    lines.append('}')
    return '\n'.join(lines)


def to_graphviz_reachability(net, max_markings=100, title=None):
    """Export reachability graph to Graphviz DOT format."""
    graph = compute_reachability_graph(net, max_markings)
    
    lines = []
    lines.append('digraph ReachabilityGraph {')
    lines.append('    rankdir=TB;')
    lines.append('    node [fontname="Arial", shape=box, style=rounded];')
    lines.append('    edge [fontname="Arial"];')
    
    if title:
        lines.append(f'    labelloc="t";')
        lines.append(f'    label="{title}";')
    lines.append('')
    
    lines.append('    // Markings')
    for idx, marking in enumerate(graph['markings']):
        marking_str = ", ".join(f"{p}:{t}" for p, t in sorted(marking.items()))
        
        if idx == graph['initial_idx']:
            fillcolor = "lightblue"
            label = f"M{idx} (initial)\\n{{{marking_str}}}"
        elif idx in graph['deadlock_markings']:
            fillcolor = "lightcoral"
            label = f"M{idx} (deadlock)\\n{{{marking_str}}}"
        else:
            fillcolor = "white"
            label = f"M{idx}\\n{{{marking_str}}}"
        
        lines.append(f'    "M{idx}" [label="{label}", style=filled, fillcolor="{fillcolor}"];')
    lines.append('')
    
    lines.append('    // Transitions')
    for from_idx, to_idx, trans_name in graph['edges']:
        lines.append(f'    "M{from_idx}" -> "M{to_idx}" [label="{trans_name}"];')
    
    lines.append('}')
    return '\n'.join(lines)


def save_graphviz(net, filepath, **kwargs):
    """Save Petri net structure to a DOT file."""
    dot = to_graphviz(net, **kwargs)
    with open(filepath, 'w') as f:
        f.write(dot)
    if net["debug"]:
        print(f"\033[32m[EXPORT] Saved Petri net to {filepath}\033[0m")
    return net


def save_graphviz_reachability(net, filepath, **kwargs):
    """Save reachability graph to a DOT file."""
    dot = to_graphviz_reachability(net, **kwargs)
    with open(filepath, 'w') as f:
        f.write(dot)
    if net["debug"]:
        print(f"\033[32m[EXPORT] Saved reachability graph to {filepath}\033[0m")
    return net


def render_graphviz(net, filepath, format="png", **kwargs):
    """Render Petri net to image. Requires Graphviz installed."""
    import subprocess
    import shutil
    
    if not shutil.which("dot"):
        if net["debug"]:
            print("\033[33m[WARNING] Graphviz 'dot' not found. Install Graphviz to render.\033[0m")
        return net
    
    dot_content = to_graphviz(net, **kwargs)
    output_path = f"{filepath}.{format}"
    
    try:
        result = subprocess.run(
            ["dot", f"-T{format}", "-o", output_path],
            input=dot_content, text=True, capture_output=True, timeout=30
        )
        if result.returncode == 0:
            if net["debug"]:
                print(f"\033[32m[RENDER] Saved image to {output_path}\033[0m")
        elif net["debug"]:
            print(f"\033[31m[ERROR] Graphviz: {result.stderr}\033[0m")
    except Exception as e:
        if net["debug"]:
            print(f"\033[31m[ERROR] Render failed: {e}\033[0m")
    
    return net


def render_graphviz_reachability(net, filepath, format="png", **kwargs):
    """Render reachability graph to image."""
    import subprocess
    import shutil
    
    if not shutil.which("dot"):
        if net["debug"]:
            print("\033[33m[WARNING] Graphviz 'dot' not found.\033[0m")
        return net
    
    dot_content = to_graphviz_reachability(net, **kwargs)
    output_path = f"{filepath}.{format}"
    
    try:
        result = subprocess.run(
            ["dot", f"-T{format}", "-o", output_path],
            input=dot_content, text=True, capture_output=True, timeout=30
        )
        if result.returncode == 0 and net["debug"]:
            print(f"\033[32m[RENDER] Saved reachability image to {output_path}\033[0m")
    except Exception:
        pass
    
    return net


# =============================================================================
# SERIALIZATION
# =============================================================================

def to_dict(net):
    """Serialize Petri net to dictionary."""
    return {
        "places": {
            name: {
                "tokens": place["read"](),
                "capacity": place["get_capacity"](),
                "initial_tokens": place["get_state"]()["initial_tokens"]
            }
            for name, place in net["places"].items()
        },
        "transitions": {
            name: {
                "consume_from": trans["consume_from"],
                "produce_to": trans["produce_to"],
                "inhibitor_from": trans["inhibitor_from"],
                "priority": trans["priority"],
                "fire_count": trans["fire_count"]
            }
            for name, trans in net["transitions"].items()
        },
        "step": net["step"],
        "marking": get_marking(net)
    }


def to_json(net, indent=2):
    """Serialize to JSON string."""
    return json.dumps(to_dict(net), indent=indent)


# =============================================================================
# STATE ACCESSORS
# =============================================================================

def get_places(net):
    """Get places dict."""
    return net["places"]

def get_transitions(net):
    """Get transitions dict."""
    return net["transitions"]

def get_step(net):
    """Get current step number."""
    return net["step"]


# =============================================================================
# ANIMATION SUPPORT
# =============================================================================

def fire_one_transition(net):
    """
    Fire a single enabled transition. Returns (net, step_info) where step_info contains:
    - step: The step number
    - fired_transition: Name of transition that fired (or None)
    - marking_before: Marking before firing
    - marking_after: Marking after firing
    - tokens_consumed: Dict of {place: count} tokens consumed
    - tokens_produced: Dict of {place: count} tokens produced
    
    Returns (net, None) if no transitions are enabled.
    """
    enabled = get_enabled_transitions(net)
    if not enabled:
        return net, None
    
    # Pick the highest priority enabled transition
    trans_name = enabled[0]
    trans = net["transitions"][trans_name]
    
    marking_before = get_marking(net)
    
    # Track token changes
    tokens_consumed = {}
    for place_name, weight in trans["consume_from"]:
        tokens_consumed[place_name] = weight
    
    tokens_produced = {}
    for place_name, weight in trans["produce_to"]:
        tokens_produced[place_name] = weight
    
    # Fire the transition
    net, _ = fire_transition(net, trans_name)
    
    marking_after = get_marking(net)
    
    step_info = {
        "step": net["step"],
        "fired_transition": trans_name,
        "marking_before": marking_before,
        "marking_after": marking_after,
        "tokens_consumed": tokens_consumed,
        "tokens_produced": tokens_produced,
        "enabled_transitions": get_enabled_transitions(net)
    }
    
    net["step"] += 1
    
    return net, step_info


def to_graphviz_animated(net, step_info=None, title=None, show_arc_weights=True):
    """
    Export Petri net to Graphviz DOT with animation highlights.
    
    Args:
        net: Petri net state
        step_info: Output from fire_one_transition() - highlights recent changes
        title: Graph title
        show_arc_weights: Show arc weights
    
    Returns:
        DOT string with highlighted places/transitions
    """
    fired = step_info.get("fired_transition") if step_info else None
    tokens_consumed = step_info.get("tokens_consumed", {}) if step_info else {}
    tokens_produced = step_info.get("tokens_produced", {}) if step_info else {}
    step = step_info.get("step", net["step"]) if step_info else net["step"]
    is_done = step_info.get("is_done", False) if step_info else False
    is_deadlock = step_info.get("is_deadlock", False) if step_info else False
    
    lines = []
    lines.append('digraph PetriNet {')
    lines.append('    rankdir=LR;')
    lines.append('    splines=spline;')
    lines.append('    nodesep=0.7;')
    lines.append('    ranksep=1.0;')
    lines.append('    pad=0.4;')
    lines.append('    bgcolor="#0f0f1a";')
    lines.append('    ')
    lines.append('    node [fontname="SF Pro Display, Helvetica Neue, Arial", fontsize=11];')
    lines.append('    edge [fontname="SF Pro Display, Helvetica Neue, Arial", fontsize=9];')
    
    # Title - show DEADLOCK in red if deadlocked, DONE in green otherwise
    if is_done and is_deadlock:
        display_title = f"{title} · DEADLOCK!" if title else "DEADLOCK!"
        title_color = "#ef4444"  # Red for deadlock
    elif is_done:
        display_title = f"{title} · DONE" if title else "DONE"
        title_color = "#22c55e"  # Green for done
    else:
        display_title = f"{title} · Step {step}" if title else f"Step {step}"
        title_color = "#ffffff"
    lines.append(f'    labelloc="t";')
    lines.append(f'    label=<<font point-size="16" color="{title_color}"><b>{display_title}</b></font>>;')
    lines.append('')
    
    # Places - clean circular design with token count
    lines.append('    // Places')
    for name, place in net["places"].items():
        tokens = place["read"]()
        capacity = place["get_capacity"]()
        
        # Token display: dots for small, number for large
        if tokens == 0:
            token_display = ""
        elif tokens <= 4:
            token_display = "●" * tokens
        else:
            token_display = str(tokens)
        
        # Color based on state
        if name in tokens_consumed and name in tokens_produced:
            fillcolor = "#7c3aed"  # Purple - both
            bordercolor = "#8b5cf6"
            fontcolor = "white"
            penwidth = "3"
        elif name in tokens_consumed:
            fillcolor = "#ea580c"  # Orange - consumed
            bordercolor = "#f97316"
            fontcolor = "white"
            penwidth = "3"
        elif name in tokens_produced:
            fillcolor = "#16a34a"  # Green - produced
            bordercolor = "#22c55e"
            fontcolor = "white"
            penwidth = "3"
        elif tokens == 0:
            fillcolor = "#1f2937"  # Dark - empty
            bordercolor = "#374151"
            fontcolor = "#6b7280"
            penwidth = "1.5"
        else:
            fillcolor = "#2563eb"  # Blue - has tokens
            bordercolor = "#3b82f6"
            fontcolor = "white"
            penwidth = "2"
        
        # Clean label - only show token row if there are tokens
        if token_display:
            label = f'<<table border="0" cellborder="0" cellspacing="0"><tr><td><font point-size="11" color="{fontcolor}"><b>{name}</b></font></td></tr><tr><td><font point-size="13" color="{fontcolor}">{token_display}</font></td></tr></table>>'
        else:
            label = f'<<font point-size="11" color="{fontcolor}"><b>{name}</b></font>>'
        
        lines.append(f'    "{name}" [shape=circle, width=0.8, height=0.8, fixedsize=true, style="filled", fillcolor="{fillcolor}", color="{bordercolor}", penwidth={penwidth}, label={label}];')
    lines.append('')
    
    # Transitions - thin bars
    lines.append('    // Transitions')
    for name, trans in net["transitions"].items():
        enabled = is_enabled(net, name)
        
        if name == fired:
            fillcolor = "#dc2626"  # Red - firing
            bordercolor = "#ef4444"
            fontcolor = "white"
            penwidth = "3"
        elif enabled:
            fillcolor = "#16a34a"  # Green - enabled
            bordercolor = "#22c55e"
            fontcolor = "white"
            penwidth = "2"
        else:
            fillcolor = "#374151"  # Gray - disabled
            bordercolor = "#4b5563"
            fontcolor = "#9ca3af"
            penwidth = "1.5"
        
        label = f'<<font point-size="10" color="{fontcolor}"><b>{name}</b></font>>'
        lines.append(f'    "{name}" [shape=rect, width=0.15, height=0.7, style="filled", fillcolor="{fillcolor}", color="{bordercolor}", penwidth={penwidth}, label={label}];')
    lines.append('')
    
    # Arcs
    lines.append('    // Arcs')
    for trans_name, trans in net["transitions"].items():
        # Input arcs
        for place_name, weight in trans["consume_from"]:
            if trans_name == fired and place_name in tokens_consumed:
                color = "#f97316"
                penwidth = "2.5"
            else:
                color = "#4b5563"
                penwidth = "1.2"
            
            weight_str = f', label=<<font point-size="9" color="#9ca3af">{weight}</font>>' if show_arc_weights and weight != 1 else ""
            lines.append(f'    "{place_name}" -> "{trans_name}" [color="{color}", penwidth={penwidth}{weight_str}];')
        
        # Output arcs
        for place_name, weight in trans["produce_to"]:
            if trans_name == fired and place_name in tokens_produced:
                color = "#22c55e"
                penwidth = "2.5"
            else:
                color = "#4b5563"
                penwidth = "1.2"
            
            weight_str = f', label=<<font point-size="9" color="#9ca3af">{weight}</font>>' if show_arc_weights and weight != 1 else ""
            lines.append(f'    "{trans_name}" -> "{place_name}" [color="{color}", penwidth={penwidth}{weight_str}];')
        
        # Inhibitor arcs
        for place_name in trans["inhibitor_from"]:
            lines.append(f'    "{place_name}" -> "{trans_name}" [style=dashed, arrowhead=odot, color="#ef4444", penwidth=1.5];')
    
    lines.append('}')
    return '\n'.join(lines)


def _generate_reachability_frame(net, title=None, current_marking=None):
    """
    Generate a DOT graph showing the reachability graph with deadlock highlighting.
    Used in animations when a deadlock state is reached.
    
    Args:
        net: Petri net state
        title: Optional title
        current_marking: Current marking to highlight (optional)
    """
    graph = compute_reachability_graph(net, max_markings=100)
    
    # Find the current marking index if provided
    current_idx = None
    if current_marking:
        marking_tuple = tuple(sorted(current_marking.items()))
        for idx, m in enumerate(graph['markings']):
            if tuple(sorted(m.items())) == marking_tuple:
                current_idx = idx
                break
    
    lines = []
    lines.append('digraph ReachabilityGraph {')
    lines.append('    rankdir=TB;')
    lines.append('    splines=spline;')
    lines.append('    nodesep=0.5;')
    lines.append('    ranksep=0.7;')
    lines.append('    pad=0.3;')
    lines.append('    bgcolor="#0f0f1a";')
    lines.append('    ')
    lines.append('    node [fontname="SF Pro Display, Helvetica Neue, Arial", fontsize=9];')
    lines.append('    edge [fontname="SF Pro Display, Helvetica Neue, Arial", fontsize=8];')
    
    # Title with DEADLOCK indicator
    display_title = f"{title} - REACHABILITY GRAPH" if title else "REACHABILITY GRAPH"
    has_deadlock = len(graph['deadlock_markings']) > 0
    if has_deadlock:
        display_title += " (DEADLOCK DETECTED)"
        title_color = "#ef4444"  # Red for deadlock
    else:
        title_color = "#22c55e"  # Green
    
    lines.append(f'    labelloc="t";')
    lines.append(f'    label=<<font point-size="14" color="{title_color}"><b>{display_title}</b></font>>;')
    lines.append('')
    
    # Markings as nodes
    lines.append('    // Markings')
    for idx, marking in enumerate(graph['markings']):
        # Create compact marking string (only show non-zero places)
        non_zero = [(p, t) for p, t in sorted(marking.items()) if t > 0]
        if non_zero:
            marking_str = ", ".join(f"{p}:{t}" for p, t in non_zero)
        else:
            marking_str = "(empty)"
        
        # Determine node styling
        is_initial = idx == graph['initial_idx']
        is_deadlock = idx in graph['deadlock_markings']
        is_current = idx == current_idx
        
        if is_deadlock:
            fillcolor = "#dc2626"  # Red for deadlock
            bordercolor = "#ef4444"
            fontcolor = "white"
            penwidth = "3"
            suffix = " (DEADLOCK)"
        elif is_initial:
            fillcolor = "#2563eb"  # Blue for initial
            bordercolor = "#3b82f6"
            fontcolor = "white"
            penwidth = "2"
            suffix = " (initial)"
        elif is_current:
            fillcolor = "#16a34a"  # Green for current
            bordercolor = "#22c55e"
            fontcolor = "white"
            penwidth = "3"
            suffix = " (current)"
        else:
            fillcolor = "#1f2937"  # Dark gray
            bordercolor = "#374151"
            fontcolor = "#d1d5db"
            penwidth = "1.5"
            suffix = ""
        
        label = f'<<table border="0" cellborder="0" cellspacing="0"><tr><td><font point-size="10" color="{fontcolor}"><b>M{idx}{suffix}</b></font></td></tr><tr><td><font point-size="8" color="{fontcolor}">{marking_str}</font></td></tr></table>>'
        
        lines.append(f'    "M{idx}" [shape=box, style="filled,rounded", fillcolor="{fillcolor}", color="{bordercolor}", penwidth={penwidth}, label={label}];')
    lines.append('')
    
    # Edges (transitions)
    lines.append('    // Transitions')
    for from_idx, to_idx, trans_name in graph['edges']:
        # Highlight edges leading to deadlock in red
        if to_idx in graph['deadlock_markings']:
            color = "#ef4444"
            penwidth = "2"
        else:
            color = "#6b7280"
            penwidth = "1.2"
        
        lines.append(f'    "M{from_idx}" -> "M{to_idx}" [label=<<font point-size="8" color="#9ca3af">{trans_name}</font>>, color="{color}", penwidth={penwidth}];')
    
    # Add legend
    lines.append('')
    lines.append('    // Legend')
    lines.append('    subgraph cluster_legend {')
    lines.append('        label=<<font point-size="9" color="#9ca3af">Legend</font>>;')
    lines.append('        style=rounded;')
    lines.append('        color="#374151";')
    lines.append('        bgcolor="#1f2937";')
    lines.append('        ')
    lines.append('        leg_initial [shape=box, style="filled,rounded", fillcolor="#2563eb", color="#3b82f6", label=<<font point-size="8" color="white">Initial</font>>];')
    lines.append('        leg_deadlock [shape=box, style="filled,rounded", fillcolor="#dc2626", color="#ef4444", label=<<font point-size="8" color="white">Deadlock</font>>];')
    lines.append('        leg_normal [shape=box, style="filled,rounded", fillcolor="#1f2937", color="#374151", label=<<font point-size="8" color="#d1d5db">Normal</font>>];')
    lines.append('        ')
    lines.append('        leg_initial -> leg_deadlock -> leg_normal [style=invis];')
    lines.append('    }')
    
    lines.append('}')
    return '\n'.join(lines)


def _generate_petri_summary_frame(metrics, title=None, min_width=None, min_height=None):
    """Generate a DOT graph showing Petri net execution summary statistics.
    
    Args:
        metrics: Execution metrics dict
        title: Optional title
        min_width: Target width in pixels (to match animation frame size)
        min_height: Target height in pixels (to match animation frame size)
    """
    # Scale font sizes based on frame dimensions, but keep table compact
    # Use smaller base to ensure it fits
    base_width = 600
    if min_width:
        scale = min(min_width / base_width, 1.8)  # Cap at 1.8x to stay compact
        scale = max(scale, 0.8)  # Don't go too small
    else:
        scale = 1.0
    
    title_size = int(14 * scale)
    header_size = int(11 * scale)
    text_size = int(9 * scale)
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
    display_title = title or "Petri Net"
    lines.append(f'    labelloc="t";')
    lines.append(f'    label=<<font point-size="{title_size}" color="#22c55e"><b>{display_title} - COMPLETE</b></font>>;')
    lines.append('')
    
    # Build summary table with scaled font sizes
    lines.append('    summary [label=<')
    lines.append(f'        <table border="0" cellborder="1" cellspacing="0" cellpadding="{cell_padding}" bgcolor="#1c1917">')
    
    # Header
    lines.append(f'            <tr><td colspan="2" bgcolor="#292524"><font point-size="{header_size}" color="#f97316"><b>EXECUTION SUMMARY</b></font></td></tr>')
    
    # Petri Net section
    lines.append(f'            <tr><td align="left"><font point-size="{text_size}" color="#a8a29e">Places</font></td><td align="right"><font point-size="{text_size}" color="#ffffff">{metrics["places"]}</font></td></tr>')
    lines.append(f'            <tr><td align="left"><font point-size="{text_size}" color="#a8a29e">Transitions</font></td><td align="right"><font point-size="{text_size}" color="#ffffff">{metrics["transitions"]}</font></td></tr>')
    lines.append(f'            <tr><td align="left"><font point-size="{text_size}" color="#a8a29e">Steps</font></td><td align="right"><font point-size="{text_size}" color="#ffffff">{metrics["steps"]}</font></td></tr>')
    lines.append(f'            <tr><td align="left"><font point-size="{text_size}" color="#a8a29e">Total Firings</font></td><td align="right"><font point-size="{text_size}" color="#ffffff">{metrics["total_firings"]}</font></td></tr>')
    
    # Highlight max parallel if > 1
    max_parallel = metrics.get("max_parallel", 1)
    if max_parallel > 1:
        lines.append(f'            <tr><td align="left"><font point-size="{text_size}" color="#a8a29e">Max Parallel</font></td><td align="right"><font point-size="{text_size}" color="#22c55e"><b>{max_parallel}</b></font></td></tr>')
    else:
        lines.append(f'            <tr><td align="left"><font point-size="{text_size}" color="#a8a29e">Max Parallel</font></td><td align="right"><font point-size="{text_size}" color="#ffffff">{max_parallel}</font></td></tr>')
    
    lines.append(f'            <tr><td align="left"><font point-size="{text_size}" color="#a8a29e">Tokens Moved</font></td><td align="right"><font point-size="{text_size}" color="#ffffff">{metrics.get("tokens_moved", 0)}</font></td></tr>')
    lines.append(f'            <tr><td align="left"><font point-size="{text_size}" color="#a8a29e">Frames</font></td><td align="right"><font point-size="{text_size}" color="#ffffff">{metrics["total_frames"]}</font></td></tr>')
    
    # Parallelism indicator
    if max_parallel > 1:
        lines.append(f'            <tr><td colspan="2" bgcolor="#16a34a"><font point-size="{text_size}" color="#ffffff"><b>PARALLEL EXECUTION</b></font></td></tr>')
    
    lines.append('        </table>')
    lines.append('    >];')
    lines.append('}')
    
    return '\n'.join(lines)


def _print_petri_summary(metrics, title=None):
    """Print Petri net execution summary to console."""
    print()
    print("\033[36m" + "=" * 50 + "\033[0m")
    print(f"\033[36m  PETRI NET SUMMARY: {title or 'Animation'}\033[0m")
    print("\033[36m" + "=" * 50 + "\033[0m")
    print()
    print(f"    Places:           {metrics['places']}")
    print(f"    Transitions:      {metrics['transitions']}")
    print(f"    Steps executed:   {metrics['steps']}")
    print(f"    Total firings:    {metrics['total_firings']}")
    parallel = metrics.get('max_parallel', 1)
    if parallel > 1:
        print(f"    Max parallel:     {parallel} \033[32m(parallel!)\033[0m")
    else:
        print(f"    Max parallel:     {parallel}")
    print(f"    Tokens moved:     {metrics.get('tokens_moved', 0)}")
    print(f"    Total frames:     {metrics['total_frames']}")
    print()
    
    if metrics.get("firing_history"):
        print("    \033[33mFiring History:\033[0m")
        for step in metrics["firing_history"][:10]:  # Show first 10
            fired = step.get("fired", [])
            if isinstance(fired, list) and len(fired) > 1:
                print(f"      Step {step['step']}: \033[32m{fired}\033[0m (parallel: {len(fired)})")
            else:
                print(f"      Step {step['step']}: {fired}")
        if len(metrics.get("firing_history", [])) > 10:
            print(f"      ... and {len(metrics['firing_history']) - 10} more steps")
    print()
    print("\033[36m" + "=" * 50 + "\033[0m")
    print()


def render_animation(net, output_path, max_steps=20, frame_delay=100, title=None, format="gif"):
    """
    Run Petri net and render animated GIF showing each step.
    
    Args:
        net: Petri net state (will be modified during execution)
        output_path: Base path for output (without extension)
        max_steps: Maximum steps to run
        frame_delay: Delay between frames in centiseconds (100 = 1 second)
        title: Title for the animation
        format: Output format ("gif" or "mp4")
    
    Returns:
        Tuple of (net, metrics) where metrics contains execution statistics
    """
    import subprocess
    import shutil
    import tempfile
    import os
    
    # Initialize metrics
    metrics = {
        "places": len(net.get("places", {})),
        "transitions": len(net.get("transitions", {})),
        "steps": 0,
        "total_firings": 0,
        "max_parallel": 0,
        "tokens_moved": 0,
        "firing_history": [],
        "total_frames": 0,
    }
    
    if not shutil.which("dot"):
        if net["debug"]:
            print("\033[33m[WARNING] Graphviz 'dot' not found. Cannot render animation.\033[0m")
        return net, metrics
    
    if not shutil.which("magick"):
        if net["debug"]:
            print("\033[33m[WARNING] ImageMagick 'magick' not found. Cannot create GIF.\033[0m")
        return net, metrics
    
    # Save initial marking for reachability analysis later
    initial_marking = get_marking(net)
    
    # Create temp directory for frames
    with tempfile.TemporaryDirectory() as tmpdir:
        frames = []
        frame_num = 0
        frame_dimensions = None  # Track animation frame size for summary
        is_deadlock = False  # Track if we ended in a deadlock
        
        # Initial frame (before any firing)
        initial_info = {
            "step": 0,
            "fired_transition": None,
            "marking_before": get_marking(net),
            "marking_after": get_marking(net),
            "tokens_consumed": {},
            "tokens_produced": {},
            "enabled_transitions": get_enabled_transitions(net)
        }
        dot_content = to_graphviz_animated(net, initial_info, title=title)
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
        
        # Run step by step using step_one for parallel execution
        for _ in range(max_steps):
            marking_before = get_marking(net)
            net, fired_list = step_one(net)
            
            if not fired_list:
                # Check if this is a deadlock (no enabled transitions)
                is_deadlock = len(get_enabled_transitions(net)) == 0
                break
            
            # Track metrics
            metrics["steps"] += 1
            metrics["total_firings"] += len(fired_list)
            metrics["max_parallel"] = max(metrics["max_parallel"], len(fired_list))
            metrics["firing_history"].append({"step": metrics["steps"], "fired": fired_list})
            
            # Calculate tokens moved
            marking_after = get_marking(net)
            tokens_consumed = {p: marking_before[p] - marking_after.get(p, 0) 
                              for p in marking_before if marking_before[p] > marking_after.get(p, 0)}
            tokens_produced = {p: marking_after[p] - marking_before.get(p, 0) 
                              for p in marking_after if marking_after.get(p, 0) > marking_before.get(p, 0)}
            metrics["tokens_moved"] += sum(tokens_consumed.values()) + sum(tokens_produced.values())
            
            step_info = {
                "step": net.get("step", 0),
                "fired_transition": fired_list[0] if len(fired_list) == 1 else None,
                "fired_transitions": fired_list,
                "tokens_consumed": tokens_consumed,
                "tokens_produced": tokens_produced,
            }
            
            dot_content = to_graphviz_animated(net, step_info, title=title)
            frame_path = os.path.join(tmpdir, f"frame_{frame_num:04d}.png")
            
            result = subprocess.run(
                ["dot", "-Tpng", "-o", frame_path],
                input=dot_content, text=True, capture_output=True, timeout=30
            )
            if result.returncode == 0:
                frames.append(frame_path)
                frame_num += 1
            
            if net["debug"]:
                print(f"\033[35m[ANIM] Frame {frame_num}: step={metrics['steps']}, fired={fired_list}\033[0m")
        
        # Add final "done" frame showing no transitions firing
        final_info = {
            "step": net.get("step", 0),
            "fired_transition": None,  # Nothing firing
            "tokens_consumed": {},
            "tokens_produced": {},
            "is_done": True,  # Signal that animation is complete
            "is_deadlock": is_deadlock  # Flag if this is a deadlock state
        }
        dot_content = to_graphviz_animated(net, final_info, title=title)
        frame_path = os.path.join(tmpdir, f"frame_{frame_num:04d}.png")
        
        result = subprocess.run(
            ["dot", "-Tpng", "-o", frame_path],
            input=dot_content, text=True, capture_output=True, timeout=30
        )
        if result.returncode == 0:
            frames.append(frame_path)
            frame_num += 1
        
        # If deadlock detected, add reachability graph frame
        reachability_frame_idx = None
        if is_deadlock:
            # Reset to initial state to compute full reachability graph
            current_marking = get_marking(net)
            set_marking(net, initial_marking)
            
            dot_content = _generate_reachability_frame(net, title=title, current_marking=current_marking)
            frame_path = os.path.join(tmpdir, f"frame_{frame_num:04d}.png")
            
            result = subprocess.run(
                ["dot", "-Tpng", "-o", frame_path],
                input=dot_content, text=True, capture_output=True, timeout=30
            )
            if result.returncode == 0:
                frames.append(frame_path)
                reachability_frame_idx = frame_num
                frame_num += 1
                if net["debug"]:
                    print(f"\033[35m[ANIM] Added reachability graph frame (deadlock detected)\033[0m")
            
            # Restore the deadlock marking
            set_marking(net, current_marking)
            
            # Update metrics
            metrics["is_deadlock"] = True
        
        # Add summary frame - constrained to fit within animation frame dimensions
        metrics["total_frames"] = len(frames) + 1  # +1 for summary
        min_w = frame_dimensions[0] if frame_dimensions else None
        min_h = frame_dimensions[1] if frame_dimensions else None
        dot_content = _generate_petri_summary_frame(metrics, title, min_width=min_w, min_height=min_h)
        frame_path = os.path.join(tmpdir, f"frame_{frame_num:04d}.png")
        
        result = subprocess.run(
            ["dot", "-Tpng", "-o", frame_path],
            input=dot_content, text=True, capture_output=True, timeout=30
        )
        if result.returncode == 0:
            frames.append(frame_path)
        
        if len(frames) < 2:
            if net["debug"]:
                print("\033[33m[WARNING] Not enough frames for animation.\033[0m")
            return net, metrics
        
        # Create GIF using ImageMagick with consistent frame sizes
        output_file = f"{output_path}.{format}"
        
        try:
            # Build command with per-frame delays
            cmd = ["magick"]
            
            # Determine how many special frames we have at the end
            # Order: [normal frames...] [done frame] [reachability frame if deadlock] [summary frame]
            num_special_frames = 2 if not is_deadlock else 3  # done + summary OR done + reachability + summary
            
            # Normal frames (excluding special ending frames)
            for f in frames[:-num_special_frames]:
                cmd.extend(["-delay", str(frame_delay), f])
            
            if is_deadlock and reachability_frame_idx is not None:
                # Done frame with 2x delay
                cmd.extend(["-delay", str(frame_delay * 2), frames[-3]])
                # Reachability graph frame with 4 second delay (important info!)
                cmd.extend(["-delay", "400", frames[-2]])
            else:
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
                if net["debug"]:
                    print(f"\033[32m[ANIM] Created animation: {output_file} ({len(frames)} frames)\033[0m")
            else:
                if net["debug"]:
                    print(f"\033[31m[ERROR] ImageMagick failed: {result.stderr}\033[0m")
        except Exception as e:
            if net["debug"]:
                print(f"\033[31m[ERROR] Animation creation failed: {e}\033[0m")
        
        # Print summary to console
        _print_petri_summary(metrics, title)
    
    return net, metrics


def collect_animation_frames(net, max_steps=20, title=None):
    """
    Run Petri net and collect DOT strings for each frame.
    
    Args:
        net: Petri net state
        max_steps: Maximum steps
        title: Animation title
    
    Returns:
        (net, frames) where frames is a list of DOT strings
    """
    frames = []
    
    # Initial frame
    initial_info = {
        "step": 0,
        "fired_transition": None,
        "marking_before": get_marking(net),
        "marking_after": get_marking(net),
        "tokens_consumed": {},
        "tokens_produced": {},
        "enabled_transitions": get_enabled_transitions(net)
    }
    frames.append(to_graphviz_animated(net, initial_info, title=title))
    
    # Run step by step
    for _ in range(max_steps):
        net, step_info = fire_one_transition(net)
        if step_info is None:
            break
        frames.append(to_graphviz_animated(net, step_info, title=title))
    
    return net, frames


# =============================================================================
# OOP WRAPPER CLASS FOR BACKWARD COMPATIBILITY
# =============================================================================

class PetriNet:
    """
    Object-oriented wrapper for backward compatibility.
    
    Internally uses the functional API but provides method-style access:
        net = PetriNet(debug=True)
        net.add_place("p1", initial_tokens=1)
        net.add_transition("t1", consume_from=[("p1", 1)])
        net.run()
    """
    
    def __init__(self, debug=True):
        self._net = petri_net(debug=debug)
    
    # Place operations
    def add_place(self, name, capacity=float('inf'), initial_tokens=0):
        self._net = add_place(self._net, name, capacity, initial_tokens)
        return self
    
    def get_marking(self):
        return get_marking(self._net)
    
    def set_marking(self, marking):
        self._net = set_marking(self._net, marking)
        return self
    
    # Transition operations
    def add_transition(self, name, consume_from=None, produce_to=None,
                       inhibitor_from=None, action=None, priority=0):
        self._net = add_transition(self._net, name, consume_from, produce_to,
                                   inhibitor_from, action, priority)
        return self
    
    def is_enabled(self, transition_name):
        return is_enabled(self._net, transition_name)
    
    def get_enabled_transitions(self):
        return get_enabled_transitions(self._net)
    
    def fire_transition(self, transition_name):
        self._net, result = fire_transition(self._net, transition_name)
        return result
    
    # Execution
    def step_one(self):
        self._net, fired = step_one(self._net)
        return fired
    
    def run(self, max_steps=1000):
        return run(self._net, max_steps)
    
    def reset(self):
        self._net = reset(self._net)
        return self
    
    # Analysis
    def compute_reachability_graph(self, max_markings=10000):
        return compute_reachability_graph(self._net, max_markings)
    
    def analyze_reachability(self, max_markings=10000):
        return analyze_reachability(self._net, max_markings)
    
    # Graphviz
    def to_graphviz(self, **kwargs):
        return to_graphviz(self._net, **kwargs)
    
    def to_graphviz_reachability(self, **kwargs):
        return to_graphviz_reachability(self._net, **kwargs)
    
    def save_graphviz(self, filepath, **kwargs):
        self._net = save_graphviz(self._net, filepath, **kwargs)
        return self
    
    def save_graphviz_reachability(self, filepath, **kwargs):
        self._net = save_graphviz_reachability(self._net, filepath, **kwargs)
        return self
    
    def render_graphviz(self, filepath, format="png", **kwargs):
        self._net = render_graphviz(self._net, filepath, format, **kwargs)
        return self
    
    def render_graphviz_reachability(self, filepath, format="png", **kwargs):
        self._net = render_graphviz_reachability(self._net, filepath, format, **kwargs)
        return self
    
    # Serialization
    def to_dict(self):
        return to_dict(self._net)
    
    def to_json(self, indent=2):
        return to_json(self._net, indent)
    
    # State accessors as properties
    @property
    def places(self):
        return get_places(self._net)
    
    @property
    def transitions(self):
        return get_transitions(self._net)
    
    @property
    def step(self):
        return get_step(self._net)
