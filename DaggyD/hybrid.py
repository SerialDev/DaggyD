"""
Hybrid Visualization: Petri Net + Pregel in One View

This module provides functions to visualize both Petri nets and Pregel graphs
together, making it clear which components belong to which model.

Visual distinction:
- Petri Net: Ellipses (places) and bars (transitions) with ORANGE theme
- Pregel: Circles (nodes) with BLUE theme
- Clear cluster labels and borders
"""

import subprocess
import shutil
import tempfile
import os

from .petri_net import get_marking, get_enabled_transitions, is_enabled, fire_one_transition, fire_transition, step_one
from .pregel_core import run_step, _format_value_for_display


def to_graphviz_hybrid(
    petri_net=None,
    petri_step_info=None,
    pregel_graphs=None,
    pregel_step_infos=None,
    title=None,
    active_pregel=None,
    trigger_transition=None  # Which Petri transition triggered the active Pregel
):
    """
    Generate DOT visualization showing both Petri net and Pregel graphs.
    Shows connection between triggering transition and Pregel cluster.
    """
    pregel_graphs = pregel_graphs or {}
    pregel_step_infos = pregel_step_infos or {}
    
    lines = []
    lines.append('digraph HybridGraph {')
    lines.append('    rankdir=TB;')
    lines.append('    splines=polyline;')
    lines.append('    nodesep=0.5;')
    lines.append('    ranksep=0.7;')
    lines.append('    pad=0.3;')
    lines.append('    bgcolor="#0d1117";')
    lines.append('    compound=true;')
    lines.append('    dpi=150;')  # Higher resolution
    lines.append('    size="10,8!";')  # Larger canvas
    lines.append('    ratio="fill";')
    lines.append('    ')
    lines.append('    node [fontname="Helvetica", fontsize=10];')
    lines.append('    edge [fontname="Helvetica", fontsize=8];')
    
    # Title
    display_title = title or "Hybrid View"
    lines.append(f'    labelloc="t";')
    lines.append(f'    label=<<font point-size="14" color="#ffffff"><b>{display_title}</b></font>>;')
    lines.append('')
    
    # === PETRI NET CLUSTER ===
    if petri_net:
        # Support both single fired transition and list of fired transitions
        fired_single = petri_step_info.get("fired_transition") if petri_step_info else None
        fired_list = petri_step_info.get("fired_transitions", []) if petri_step_info else []
        if fired_single and not fired_list:
            fired_list = [fired_single]
        fired_set = set(fired_list)
        
        tokens_consumed = petri_step_info.get("tokens_consumed", {}) if petri_step_info else {}
        tokens_produced = petri_step_info.get("tokens_produced", {}) if petri_step_info else {}
        
        # Find all enabled transitions (for showing parallelism)
        all_enabled = get_enabled_transitions(petri_net)
        
        lines.append('    subgraph cluster_petri {')
        lines.append('        label=<<font point-size="12" color="#fb923c"><b>PETRI NET</b></font>>;')
        lines.append('        style="rounded,filled";')
        lines.append('        fillcolor="#1c1917";')
        lines.append('        color="#fb923c";')
        lines.append('        penwidth=2;')
        lines.append('        margin=15;')
        lines.append('')
        
        # Organize places by lane for better layout
        lanes = {'A': [], 'B': [], 'C': [], 'resources': [], 'main': []}
        for name in petri_net["places"].keys():
            if name.startswith('A_'):
                lanes['A'].append(name)
            elif name.startswith('B_'):
                lanes['B'].append(name)
            elif name.startswith('C_'):
                lanes['C'].append(name)
            elif name in ('gpu', 'mem', 'lock'):
                lanes['resources'].append(name)
            else:
                lanes['main'].append(name)
        
        # Add rank constraints for lanes
        if lanes['A']:
            lines.append(f'        {{ rank=same; {" ".join(f"p_{n}" for n in lanes["A"])} }}')
        if lanes['B']:
            lines.append(f'        {{ rank=same; {" ".join(f"p_{n}" for n in lanes["B"])} }}')
        if lanes['C']:
            lines.append(f'        {{ rank=same; {" ".join(f"p_{n}" for n in lanes["C"])} }}')
        lines.append('')
        
        # Places
        for name, place in petri_net["places"].items():
            tokens = place["read"]()
            
            # Abbreviate names
            abbrevs = {
                'start': 'START', 'done': 'DONE', 'merged': 'JOIN',
                'gpu': 'GPU', 'mem': 'MEM', 'lock': 'LOCK',
            }
            if name in abbrevs:
                short_name = abbrevs[name]
            elif '_' in name:
                # A_wait -> A.w, A_val -> A.v, etc
                parts = name.split('_')
                short_name = f"{parts[0]}.{parts[1][:2]}"
            else:
                short_name = name[:5]
            
            # Token display
            if tokens == 0:
                token_str = ""
            elif tokens <= 3:
                token_str = "●" * tokens
            else:
                token_str = f"[{tokens}]"
            
            # Color: lane-based coloring
            if name in tokens_consumed:
                fill, border = "#ea580c", "#fb923c"
            elif name in tokens_produced:
                fill, border = "#16a34a", "#22c55e"
            elif tokens > 0:
                # Different colors for different lanes
                if name.startswith('A_'):
                    fill, border = "#7c3aed", "#8b5cf6"  # Purple for lane A
                elif name.startswith('B_'):
                    fill, border = "#0891b2", "#06b6d4"  # Cyan for lane B
                elif name.startswith('C_'):
                    fill, border = "#c026d3", "#d946ef"  # Pink for lane C
                elif name in ('gpu', 'mem', 'lock'):
                    fill, border = "#ca8a04", "#eab308"  # Yellow for resources
                else:
                    fill, border = "#c2410c", "#ea580c"  # Orange for main
            else:
                fill, border = "#292524", "#57534e"
            
            fontcolor = "white" if tokens > 0 or name in tokens_consumed or name in tokens_produced else "#a8a29e"
            
            if token_str:
                label = f'<<font point-size="9" color="{fontcolor}"><b>{short_name}</b><br/>{token_str}</font>>'
            else:
                label = f'<<font point-size="9" color="{fontcolor}"><b>{short_name}</b></font>>'
            
            lines.append(f'        "p_{name}" [shape=ellipse, width=0.55, height=0.45, style=filled, fillcolor="{fill}", color="{border}", penwidth=2, label={label}];')
        
        lines.append('')
        
        # Transitions
        for name, trans in petri_net["transitions"].items():
            enabled = name in all_enabled
            # Color based on state - show ALL enabled transitions highlighted
            is_fired = name in fired_set
            if is_fired:
                fill, border = "#16a34a", "#22c55e"  # Green = just fired
                pw = "3"
            elif enabled:
                fill, border = "#fbbf24", "#fcd34d"  # Yellow = ready to fire (parallel!)
                pw = "2"
            else:
                fill, border = "#292524", "#57534e"
                pw = "1"
            
            lines.append(f'        "t_{name}" [shape=rect, width=0.06, height=0.4, style=filled, fillcolor="{fill}", color="{border}", penwidth={pw}, label=""];')
        
        lines.append('')
        
        # Arcs
        for trans_name, trans in petri_net["transitions"].items():
            for place_name, weight in trans["consume_from"]:
                active = (trans_name in fired_set and place_name in tokens_consumed)
                color = "#fb923c" if active else "#57534e"
                pw = "2" if active else "1"
                lines.append(f'        "p_{place_name}" -> "t_{trans_name}" [color="{color}", penwidth={pw}];')
            
            for place_name, weight in trans["produce_to"]:
                active = (trans_name in fired_set and place_name in tokens_produced)
                color = "#22c55e" if active else "#57534e"
                pw = "2" if active else "1"
                lines.append(f'        "t_{trans_name}" -> "p_{place_name}" [color="{color}", penwidth={pw}];')
        
        lines.append('    }')
        lines.append('')
    
    # === PREGEL CLUSTERS ===
    for pregel_name, p in pregel_graphs.items():
        step_info = pregel_step_infos.get(pregel_name, {})
        is_active = (pregel_name == active_pregel)
        
        executed = step_info.get("executed_nodes", []) if step_info else []
        halted = step_info.get("halted_nodes", []) if step_info else []
        
        # Cluster styling
        if is_active:
            cluster_fill = "#0c1929"
            cluster_border = "#3b82f6"
            cluster_pw = "3"
        else:
            cluster_fill = "#1e293b"
            cluster_border = "#475569"
            cluster_pw = "1"
        
        lines.append(f'    subgraph cluster_{pregel_name} {{')
        lines.append(f'        label=<<font point-size="12" color="#60a5fa"><b>PREGEL: {pregel_name}</b></font>>;')
        lines.append('        style="rounded,filled";')
        lines.append(f'        fillcolor="{cluster_fill}";')
        lines.append(f'        color="{cluster_border}";')
        lines.append(f'        penwidth={cluster_pw};')
        lines.append('        margin=15;')
        lines.append('')
        
        # Nodes
        for node_name, node_info in p["nodes"].items():
            active = node_info["active"]
            
            # Get value to display
            value_str = ""
            for ch_name in node_info["subscribe_to"]:
                if ch_name in p["channels"]:
                    if 'inbox' in ch_name.lower():
                        continue
                    val = p["channels"][ch_name]["read"]()
                    if val is not None and val != 0 and val != []:
                        value_str = _format_value_for_display(val)
                        break
            
            # Styling
            if node_name in executed:
                fill, border = "#16a34a", "#22c55e"
            elif node_name in halted or not active:
                fill, border = "#334155", "#475569"
            else:
                fill, border = "#2563eb", "#3b82f6"
            
            fontcolor = "white" if node_name in executed or active else "#94a3b8"
            
            # Short display name
            display_name = node_name.replace('normalize_', 'N').replace('vertex_', '').replace('node_', '')
            
            if value_str:
                label = f'<<font color="{fontcolor}"><b>{display_name}</b><br/><font point-size="9">{value_str}</font></font>>'
                size = "0.65"
            else:
                label = f'<<font color="{fontcolor}"><b>{display_name}</b></font>>'
                size = "0.5"
            
            lines.append(f'        "{pregel_name}_{node_name}" [shape=circle, width={size}, height={size}, fixedsize=true, style=filled, fillcolor="{fill}", color="{border}", penwidth=2, label={label}];')
        
        lines.append('')
        
        # Edges
        channel_subscribers = {}
        for node_name, node_info in p["nodes"].items():
            for ch_name in node_info["subscribe_to"]:
                channel_subscribers.setdefault(ch_name, []).append(node_name)
        
        edges_drawn = set()
        for src_node, node_info in p["nodes"].items():
            for output_key, channel_name in node_info["write_to"].items():
                for dst_node in channel_subscribers.get(channel_name, []):
                    if dst_node != src_node:
                        edge_key = (src_node, dst_node)
                        if edge_key not in edges_drawn:
                            edges_drawn.add(edge_key)
                            color = "#3b82f6" if is_active else "#475569"
                            lines.append(f'        "{pregel_name}_{src_node}" -> "{pregel_name}_{dst_node}" [color="{color}", penwidth=1];')
        
        lines.append('    }')
        lines.append('')
        
        # Draw connection from Petri transition to Pregel cluster
        if is_active and trigger_transition and petri_net:
            # Find first node in pregel to connect to
            first_node = list(p["nodes"].keys())[0] if p["nodes"] else None
            if first_node:
                lines.append('    // Connection: Petri -> Pregel')
                lines.append(f'    "t_{trigger_transition}" -> "{pregel_name}_{first_node}" [')
                lines.append('        style=bold,')
                lines.append('        color="#e879f9",')  # Bright magenta
                lines.append('        penwidth=4,')
                lines.append('        arrowsize=1.5,')
                lines.append(f'        lhead=cluster_{pregel_name},')
                lines.append('        label=<<font point-size="14" color="#e879f9"><b>TRIGGERS</b></font>>];')
                lines.append('')
    
    lines.append('}')
    return '\n'.join(lines)


def _generate_summary_frame(metrics, title=None, min_width=None, min_height=None):
    """Generate a DOT graph showing execution summary statistics.
    
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
    section_size = int(10 * scale)
    text_size = int(9 * scale)
    cell_padding = int(5 * scale)
    
    lines = []
    lines.append('digraph Summary {')
    lines.append('    rankdir=TB;')
    lines.append('    bgcolor="#0d1117";')
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
    display_title = title or "Execution Complete"
    lines.append(f'    labelloc="t";')
    lines.append(f'    label=<<font point-size="{title_size}" color="#22c55e"><b>{display_title} - COMPLETE</b></font>>;')
    lines.append('')
    
    # Build summary table with scaled font sizes
    lines.append('    summary [label=<')
    lines.append(f'        <table border="0" cellborder="1" cellspacing="0" cellpadding="{cell_padding}" bgcolor="#1c1917">')
    
    # Header
    lines.append(f'            <tr><td colspan="2" bgcolor="#292524"><font point-size="{header_size}" color="#f97316"><b>EXECUTION SUMMARY</b></font></td></tr>')
    
    # Petri Net section
    lines.append(f'            <tr><td colspan="2" bgcolor="#1c1917"><font point-size="{section_size}" color="#fb923c"><b>PETRI NET</b></font></td></tr>')
    lines.append(f'            <tr><td align="left"><font point-size="{text_size}" color="#a8a29e">Places</font></td><td align="right"><font point-size="{text_size}" color="#ffffff">{metrics["petri_places"]}</font></td></tr>')
    lines.append(f'            <tr><td align="left"><font point-size="{text_size}" color="#a8a29e">Transitions</font></td><td align="right"><font point-size="{text_size}" color="#ffffff">{metrics["petri_transitions"]}</font></td></tr>')
    lines.append(f'            <tr><td align="left"><font point-size="{text_size}" color="#a8a29e">Steps</font></td><td align="right"><font point-size="{text_size}" color="#ffffff">{metrics["petri_steps"]}</font></td></tr>')
    lines.append(f'            <tr><td align="left"><font point-size="{text_size}" color="#a8a29e">Total Firings</font></td><td align="right"><font point-size="{text_size}" color="#ffffff">{metrics["petri_total_firings"]}</font></td></tr>')
    
    # Highlight max parallel
    max_parallel = metrics.get("petri_max_parallel", 1)
    if max_parallel > 1:
        lines.append(f'            <tr><td align="left"><font point-size="{text_size}" color="#a8a29e">Max Parallel</font></td><td align="right"><font point-size="{text_size}" color="#22c55e"><b>{max_parallel}</b></font></td></tr>')
    else:
        lines.append(f'            <tr><td align="left"><font point-size="{text_size}" color="#a8a29e">Max Parallel</font></td><td align="right"><font point-size="{text_size}" color="#ffffff">{max_parallel}</font></td></tr>')
    
    lines.append(f'            <tr><td align="left"><font point-size="{text_size}" color="#a8a29e">Tokens Moved</font></td><td align="right"><font point-size="{text_size}" color="#ffffff">{metrics["petri_tokens_moved"]}</font></td></tr>')
    
    # Pregel section (if any)
    if metrics.get("pregel_graphs_count", 0) > 0:
        lines.append(f'            <tr><td colspan="2" bgcolor="#1c1917"><font point-size="{section_size}" color="#3b82f6"><b>PREGEL</b></font></td></tr>')
        lines.append(f'            <tr><td align="left"><font point-size="{text_size}" color="#a8a29e">Graphs</font></td><td align="right"><font point-size="{text_size}" color="#ffffff">{metrics["pregel_graphs_count"]}</font></td></tr>')
        lines.append(f'            <tr><td align="left"><font point-size="{text_size}" color="#a8a29e">Supersteps</font></td><td align="right"><font point-size="{text_size}" color="#ffffff">{metrics["pregel_total_supersteps"]}</font></td></tr>')
        lines.append(f'            <tr><td align="left"><font point-size="{text_size}" color="#a8a29e">Node Execs</font></td><td align="right"><font point-size="{text_size}" color="#ffffff">{metrics["pregel_node_executions"]}</font></td></tr>')
    
    # Animation section
    lines.append(f'            <tr><td colspan="2" bgcolor="#1c1917"><font point-size="{section_size}" color="#8b5cf6"><b>ANIMATION</b></font></td></tr>')
    lines.append(f'            <tr><td align="left"><font point-size="{text_size}" color="#a8a29e">Total Frames</font></td><td align="right"><font point-size="{text_size}" color="#ffffff">{metrics["total_frames"]}</font></td></tr>')
    
    # Parallelism indicator
    if max_parallel > 1:
        lines.append(f'            <tr><td colspan="2" bgcolor="#16a34a"><font point-size="{text_size}" color="#ffffff"><b>PARALLEL EXECUTION</b></font></td></tr>')
    
    lines.append('        </table>')
    lines.append('    >];')
    lines.append('}')
    
    return '\n'.join(lines)


def _print_execution_summary(metrics, title=None):
    """Print detailed execution summary to console."""
    print()
    print("\033[36m" + "=" * 60 + "\033[0m")
    print(f"\033[36m  EXECUTION SUMMARY: {title or 'Hybrid Animation'}\033[0m")
    print("\033[36m" + "=" * 60 + "\033[0m")
    print()
    
    # Petri Net stats
    print("\033[33m  PETRI NET\033[0m")
    print(f"    Places:              {metrics['petri_places']}")
    print(f"    Transitions:         {metrics['petri_transitions']}")
    print(f"    Steps executed:      {metrics['petri_steps']}")
    print(f"    Total firings:       {metrics['petri_total_firings']}")
    print(f"    Max parallel:        {metrics['petri_max_parallel']} \033[32m{'(parallel!)' if metrics['petri_max_parallel'] > 1 else ''}\033[0m")
    print(f"    Tokens moved:        {metrics['petri_tokens_moved']}")
    print()
    
    # Pregel stats
    if metrics["pregel_graphs_count"] > 0:
        print("\033[34m  PREGEL COMPUTATIONS\033[0m")
        print(f"    Graphs executed:     {metrics['pregel_graphs_count']}")
        print(f"    Total supersteps:    {metrics['pregel_total_supersteps']}")
        print(f"    Total nodes:         {metrics['pregel_total_nodes']}")
        print(f"    Node executions:     {metrics['pregel_node_executions']}")
        print(f"    Messages sent:       {metrics['pregel_messages_sent']}")
        print()
    
    # Animation stats
    print("\033[35m  ANIMATION\033[0m")
    print(f"    Total frames:        {metrics['total_frames']}")
    print(f"    Petri frames:        {metrics['petri_frames']}")
    print(f"    Pregel frames:       {metrics['pregel_frames']}")
    print()
    
    # Firing history
    if metrics.get("firing_history"):
        print("\033[33m  FIRING HISTORY\033[0m")
        for i, step in enumerate(metrics["firing_history"]):
            fired = step.get("fired", [])
            if len(fired) > 1:
                print(f"    Step {i}: \033[32m{fired}\033[0m  (parallel: {len(fired)})")
            elif fired:
                print(f"    Step {i}: {fired}")
        print()
    
    print("\033[36m" + "=" * 60 + "\033[0m")
    print()


def render_hybrid_animation(
    petri_net,
    pregel_factories,
    output_path,
    max_steps=20,
    frame_delay=100,
    title=None,
    format="gif"
):
    """
    Render animation showing Petri net workflow with embedded Pregel computations.
    
    Args:
        petri_net: Petri net state
        pregel_factories: Dict of {transition_name: (pregel_factory_fn, pregel_name)}
        output_path: Output path (without extension)
        max_steps: Max Petri net steps
        frame_delay: Frame delay in centiseconds
        title: Animation title
        format: Output format
    
    Returns:
        Tuple of (final_petri_net, metrics_dict)
    """
    if not shutil.which("dot"):
        print("\033[33m[WARNING] Graphviz 'dot' not found.\033[0m")
        return petri_net, {}
    
    if not shutil.which("magick"):
        print("\033[33m[WARNING] ImageMagick 'magick' not found.\033[0m")
        return petri_net, {}
    
    # Initialize metrics tracking
    metrics = {
        # Petri net metrics
        "petri_places": len(petri_net.get("places", {})),
        "petri_transitions": len(petri_net.get("transitions", {})),
        "petri_steps": 0,
        "petri_total_firings": 0,
        "petri_max_parallel": 0,
        "petri_tokens_moved": 0,
        "firing_history": [],
        
        # Pregel metrics
        "pregel_graphs_count": 0,
        "pregel_total_supersteps": 0,
        "pregel_total_nodes": 0,
        "pregel_node_executions": 0,
        "pregel_messages_sent": 0,
        
        # Animation metrics
        "total_frames": 0,
        "petri_frames": 0,
        "pregel_frames": 0,
    }
    
    with tempfile.TemporaryDirectory() as tmpdir:
        frames = []
        frame_num = 0
        current_pregel = {}  # Only show ONE pregel at a time (the active one)
        frame_dimensions = None  # Track animation frame size for summary
        
        # Initial frame
        dot = to_graphviz_hybrid(
            petri_net=petri_net,
            petri_step_info={"step": 0, "fired_transition": None, "tokens_consumed": {}, "tokens_produced": {}},
            pregel_graphs=current_pregel,
            title=title
        )
        frame_path = os.path.join(tmpdir, f"frame_{frame_num:04d}.png")
        result = subprocess.run(["dot", "-Tpng", "-o", frame_path], input=dot, text=True, capture_output=True, timeout=30)
        if result.returncode == 0:
            frames.append(frame_path)
            frame_num += 1
            metrics["petri_frames"] += 1
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
        
        # Run Petri net step by step - firing ALL enabled transitions in parallel
        for _ in range(max_steps):
            # Get marking before step
            marking_before = get_marking(petri_net)
            
            # Fire ALL enabled transitions at once (true parallelism)
            petri_net, fired_list = step_one(petri_net)
            if not fired_list:
                break
            
            # Track metrics
            metrics["petri_steps"] += 1
            metrics["petri_total_firings"] += len(fired_list)
            metrics["petri_max_parallel"] = max(metrics["petri_max_parallel"], len(fired_list))
            metrics["firing_history"].append({"step": metrics["petri_steps"], "fired": fired_list})
            
            # Calculate tokens consumed/produced for visualization
            marking_after = get_marking(petri_net)
            tokens_consumed = {p: marking_before[p] - marking_after.get(p, 0) 
                              for p in marking_before if marking_before[p] > marking_after.get(p, 0)}
            tokens_produced = {p: marking_after[p] - marking_before.get(p, 0) 
                              for p in marking_after if marking_after.get(p, 0) > marking_before.get(p, 0)}
            
            # Count tokens moved
            metrics["petri_tokens_moved"] += sum(tokens_consumed.values()) + sum(tokens_produced.values())
            
            step_info = {
                "step": petri_net.get("step", 0),
                "fired_transitions": fired_list,  # List of ALL fired transitions
                "tokens_consumed": tokens_consumed,
                "tokens_produced": tokens_produced
            }
            
            # Check if ANY fired transitions trigger Pregel computations
            triggered_pregels = [(t, pregel_factories[t]) for t in fired_list if t in pregel_factories]
            
            if triggered_pregels:
                # Run Pregel for each triggered transition
                for trigger_trans, (factory_fn, pregel_name) in triggered_pregels:
                    p = factory_fn()
                    # Clear previous and show only current pregel
                    current_pregel.clear()
                    current_pregel[pregel_name] = p
                    
                    # Track Pregel graph metrics
                    metrics["pregel_graphs_count"] += 1
                    metrics["pregel_total_nodes"] += len(p.get("nodes", {}))
                    
                    # Run Pregel steps
                    pregel_step_infos = {}
                    for _ in range(10):
                        p, pregel_info = run_step(p)
                        if pregel_info is None:
                            break
                        
                        # Track Pregel execution metrics
                        metrics["pregel_total_supersteps"] += 1
                        executed_nodes = pregel_info.get("executed_nodes", [])
                        metrics["pregel_node_executions"] += len(executed_nodes)
                        # Estimate messages (each executed node sends to its outputs)
                        for node_name in executed_nodes:
                            node_info = p["nodes"].get(node_name, {})
                            metrics["pregel_messages_sent"] += len(node_info.get("write_to", {}))
                        
                        pregel_step_infos[pregel_name] = pregel_info
                        current_pregel[pregel_name] = p
                        
                        dot = to_graphviz_hybrid(
                            petri_net=petri_net,
                            petri_step_info=step_info,
                            pregel_graphs=current_pregel,
                            pregel_step_infos=pregel_step_infos,
                            title=title,
                            active_pregel=pregel_name,
                            trigger_transition=trigger_trans
                        )
                        frame_path = os.path.join(tmpdir, f"frame_{frame_num:04d}.png")
                        result = subprocess.run(["dot", "-Tpng", "-o", frame_path], input=dot, text=True, capture_output=True, timeout=30)
                        if result.returncode == 0:
                            frames.append(frame_path)
                            frame_num += 1
                            metrics["pregel_frames"] += 1
            else:
                # Just Petri net step (no Pregel triggered)
                dot = to_graphviz_hybrid(
                    petri_net=petri_net,
                    petri_step_info=step_info,
                    pregel_graphs=current_pregel,
                    title=title
                )
                frame_path = os.path.join(tmpdir, f"frame_{frame_num:04d}.png")
                result = subprocess.run(["dot", "-Tpng", "-o", frame_path], input=dot, text=True, capture_output=True, timeout=30)
                if result.returncode == 0:
                    frames.append(frame_path)
                    frame_num += 1
                    metrics["petri_frames"] += 1
                else:
                    print(f"\033[33m[DOT ERROR] {result.stderr[:200]}\033[0m")
        
        # Final "done" frame showing the completed state
        dot = to_graphviz_hybrid(
            petri_net=petri_net,
            petri_step_info={"step": petri_net.get("step", 0), "fired_transition": None, "tokens_consumed": {}, "tokens_produced": {}, "is_done": True},
            pregel_graphs=current_pregel,
            title=f"{title} - DONE" if title else "DONE"
        )
        frame_path = os.path.join(tmpdir, f"frame_{frame_num:04d}.png")
        result = subprocess.run(["dot", "-Tpng", "-o", frame_path], input=dot, text=True, capture_output=True, timeout=30)
        if result.returncode == 0:
            frames.append(frame_path)
            frame_num += 1
        
        # Find maximum frame dimensions across all frames for consistent sizing
        max_width, max_height = 0, 0
        for frame_path in frames:
            try:
                identify_result = subprocess.run(
                    ["identify", "-format", "%w %h", frame_path],
                    capture_output=True, text=True, timeout=10
                )
                if identify_result.returncode == 0:
                    w, h = identify_result.stdout.strip().split()
                    max_width = max(max_width, int(w))
                    max_height = max(max_height, int(h))
            except Exception:
                pass
        
        if max_width == 0 or max_height == 0:
            max_width = frame_dimensions[0] if frame_dimensions else 1500
            max_height = frame_dimensions[1] if frame_dimensions else 1200
        
        # Summary frame - constrained to fit within max animation frame dimensions
        metrics["total_frames"] = len(frames) + 1  # +1 for summary frame
        dot = _generate_summary_frame(metrics, title, min_width=max_width, min_height=max_height)
        frame_path = os.path.join(tmpdir, f"frame_{frame_num:04d}.png")
        result = subprocess.run(["dot", "-Tpng", "-o", frame_path], input=dot, text=True, capture_output=True, timeout=30)
        if result.returncode == 0:
            frames.append(frame_path)
        
        if len(frames) < 2:
            print("\033[33m[WARNING] Not enough frames.\033[0m")
            return petri_net, metrics
        
        # Create GIF with consistent frame sizes
        # Use different delays: normal frames get frame_delay, summary frame gets longer delay
        output_file = f"{output_path}.{format}"
        try:
            # Build command with per-frame delays
            cmd = ["magick"]
            
            # Add all frames except the last two (done + summary) with normal delay
            for f in frames[:-2]:
                cmd.extend(["-delay", str(frame_delay), f])
            
            # Add the "done" frame with 2x delay 
            if len(frames) >= 2:
                cmd.extend(["-delay", str(frame_delay * 2), frames[-2]])
            
            # Add the summary frame with a much longer delay (300 centiseconds = 3 seconds)
            cmd.extend(["-delay", "300", frames[-1]])
            
            # Add final options - use loop 0 (infinite) so summary stays visible
            # Resize all frames to the largest dimensions (including summary) for consistency
            if max_width > 0 and max_height > 0:
                cmd.extend([
                    "-coalesce",
                    "-gravity", "center",
                    "-background", "#0d1117",
                    "-extent", f"{max_width}x{max_height}",
                ])
            
            cmd.extend([
                "-loop", "0",  # Infinite loop so summary frame stays visible
                "-dispose", "previous",
                output_file
            ])
            
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=60)
            if result.returncode == 0:
                print(f"\033[32m[HYBRID] Created: {output_file} ({len(frames)} frames)\033[0m")
            else:
                print(f"\033[31m[ERROR] {result.stderr}\033[0m")
        except Exception as e:
            print(f"\033[31m[ERROR] {e}\033[0m")
        
        # Print detailed summary to console
        _print_execution_summary(metrics, title)
        
        return petri_net, metrics
