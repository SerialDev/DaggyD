"""
Animated Examples - GIF Generation

This module generates animated GIFs showing step-by-step execution
for both Pregel and Petri Net models.

Each example runs 5 different input scenarios to demonstrate
how the system behaves with different initial conditions.

Usage:
    python samples/animated_examples.py

Outputs are saved to: example_outputs/animations/
"""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from DaggyD.pregel_core import (
    pregel, add_channel, add_node, reset,
    render_animation as pregel_render_animation
)
from DaggyD.petri_net import (
    petri_net, add_place, add_transition, reset as petri_reset,
    render_animation as petri_render_animation
)

# Output directory for animations
OUTPUT_DIR = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "example_outputs", "animations"
)


def ensure_output_dir():
    """Create output directory if it doesn't exist."""
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    print(f"\033[36m[OUTPUT DIR] {OUTPUT_DIR}\033[0m\n")


# =============================================================================
# PREGEL ANIMATIONS
# =============================================================================

def create_counter_graph(initial_value=0, max_count=5):
    """
    Simple counter that increments until max.
    Great for showing basic Pregel execution.
    """
    p = pregel(debug=False)
    p = add_channel(p, "counter", "LastValue", initial_value=initial_value)
    p = add_channel(p, "log", "Topic")
    
    def increment(inputs, max_val=max_count):
        val = inputs.get("counter", 0)
        if val < max_val:
            return {"next": val + 1, "msg": f"Count: {val + 1}"}
        return None
    
    p = add_node(p, "incrementer", increment,
        subscribe_to=["counter"],
        write_to={"next": "counter", "msg": "log"})
    
    return p


def create_ping_pong_graph(initial_a=1, initial_b=0):
    """
    Two nodes passing a token back and forth.
    Demonstrates message passing between nodes.
    """
    p = pregel(debug=False)
    p = add_channel(p, "ball_at_a", "LastValue", initial_value=initial_a)
    p = add_channel(p, "ball_at_b", "LastValue", initial_value=initial_b)
    p = add_channel(p, "rally_count", "BinaryOperator", 
                   operator=lambda a, b: a + b, initial_value=0)
    
    def player_a(inputs):
        ball = inputs.get("ball_at_a", 0)
        if ball > 0:
            return {"send_b": 1, "clear_a": 0, "rally": 1}
        return None
    
    def player_b(inputs):
        ball = inputs.get("ball_at_b", 0)
        if ball > 0:
            return {"send_a": 1, "clear_b": 0, "rally": 1}
        return None
    
    p = add_node(p, "player_A", player_a,
        subscribe_to=["ball_at_a"],
        write_to={"send_b": "ball_at_b", "clear_a": "ball_at_a", "rally": "rally_count"})
    
    p = add_node(p, "player_B", player_b,
        subscribe_to=["ball_at_b"],
        write_to={"send_a": "ball_at_a", "clear_b": "ball_at_b", "rally": "rally_count"})
    
    return p


def create_sum_propagation_graph(values):
    """
    Multiple nodes each with a value, computing global sum.
    Demonstrates aggregation patterns.
    """
    p = pregel(debug=False)
    
    # Global sum accumulator
    p = add_channel(p, "global_sum", "BinaryOperator",
                   operator=lambda a, b: a + b, initial_value=0)
    
    # Each node contributes once
    contributed = {}
    
    for i, val in enumerate(values):
        node_name = f"node_{i}"
        channel_name = f"value_{i}"
        contributed[node_name] = False
        
        p = add_channel(p, channel_name, "LastValue", initial_value=val)
        
        def make_contribute(name, ch_name):
            def contribute(inputs):
                if contributed[name]:
                    return None
                val = inputs.get(ch_name, 0)
                contributed[name] = True
                return {"sum": val}
            return contribute
        
        p = add_node(p, node_name, make_contribute(node_name, channel_name),
            subscribe_to=[channel_name],
            write_to={"sum": "global_sum"})
    
    return p


def create_pagerank_graph(graph, damping=0.85, max_iterations=10):
    """
    PageRank algorithm - the classic Pregel example.
    
    Args:
        graph: Dict of {vertex: [out_neighbors]}
        damping: Damping factor (default 0.85)
        max_iterations: Max iterations before halting
    """
    num_vertices = len(graph)
    initial_rank = 1.0 / num_vertices
    convergence_threshold = 0.001
    
    p = pregel(debug=False)
    
    # Create channels for each vertex
    for vertex in graph:
        p = add_channel(p, f"rank_{vertex}", "LastValue",
            initial_value={"rank": initial_rank, "iteration": 0})
        p = add_channel(p, f"inbox_{vertex}", "Accumulator",
            operator=lambda a, b: a + b, initial_value=0.0)
    
    # Create vertex compute functions
    for vertex, out_neighbors in graph.items():
        out_degree = len(out_neighbors)
        
        def make_compute(vid, neighbors, degree):
            def compute(inputs):
                state = inputs.get(f"rank_{vid}", {})
                current_rank = state.get("rank", initial_rank)
                iteration = state.get("iteration", 0)
                inbox = inputs.get(f"inbox_{vid}") or 0.0
                
                if iteration >= max_iterations:
                    return None
                
                if iteration == 0:
                    new_rank = current_rank
                else:
                    new_rank = (1 - damping) / num_vertices + damping * inbox
                
                contribution = new_rank / degree
                
                outputs = {"rank": {"rank": new_rank, "iteration": iteration + 1}}
                for neighbor in neighbors:
                    outputs[f"to_{neighbor}"] = contribution
                
                return outputs
            return compute
        
        write_to = {"rank": f"rank_{vertex}"}
        for neighbor in out_neighbors:
            write_to[f"to_{neighbor}"] = f"inbox_{neighbor}"
        
        p = add_node(p, f"vertex_{vertex}",
            make_compute(vertex, out_neighbors, out_degree),
            subscribe_to=[f"rank_{vertex}", f"inbox_{vertex}"],
            write_to=write_to)
    
    return p


def create_max_value_graph(initial_values):
    """
    Maximum value propagation - another classic Pregel algorithm.
    Each vertex propagates the maximum value it has seen.
    
    Args:
        initial_values: Dict of {vertex: initial_value}
    """
    p = pregel(debug=False)
    
    # Graph structure: fully connected for simplicity
    vertices = list(initial_values.keys())
    
    # Global max channel
    p = add_channel(p, "global_max", "BinaryOperator", operator=max, initial_value=0)
    
    # Create channels and state for each vertex
    for v in vertices:
        p = add_channel(p, f"value_{v}", "LastValue", initial_value=initial_values[v])
        p = add_channel(p, f"inbox_{v}", "Topic")
    
    # Track state
    vertex_max = {v: initial_values[v] for v in vertices}
    sent_initial = {v: False for v in vertices}
    
    for v in vertices:
        other_vertices = [u for u in vertices if u != v]
        
        def make_compute(vid, others):
            def compute(inputs):
                current = inputs.get(f"value_{vid}", 0)
                messages = inputs.get(f"inbox_{vid}", [])
                
                if messages:
                    max_msg = max(messages)
                    if max_msg > vertex_max[vid]:
                        vertex_max[vid] = max_msg
                        outputs = {"gmax": max_msg}
                        for other in others:
                            outputs[f"to_{other}"] = max_msg
                        return outputs
                    return None
                elif not sent_initial[vid]:
                    sent_initial[vid] = True
                    outputs = {"gmax": current}
                    for other in others:
                        outputs[f"to_{other}"] = current
                    return outputs
                return None
            return compute
        
        write_to = {"gmax": "global_max"}
        for other in other_vertices:
            write_to[f"to_{other}"] = f"inbox_{other}"
        
        p = add_node(p, f"node_{v}", make_compute(v, other_vertices),
            subscribe_to=[f"value_{v}", f"inbox_{v}"],
            write_to=write_to)
    
    return p


def create_shortest_path_graph(graph, source):
    """
    Single-source shortest path (SSSP) - Bellman-Ford style.
    
    Args:
        graph: Dict of {vertex: [(neighbor, weight), ...]}
        source: Source vertex
    """
    p = pregel(debug=False)
    
    INF = 999999
    vertices = list(graph.keys())
    
    # Distance channels
    for v in vertices:
        initial_dist = 0 if v == source else INF
        p = add_channel(p, f"dist_{v}", "LastValue", initial_value=initial_dist)
        p = add_channel(p, f"inbox_{v}", "BinaryOperator",
            operator=min, initial_value=INF)
    
    # Track which vertices have sent updates
    has_updated = {v: False for v in vertices}
    
    for v in vertices:
        neighbors = graph[v]
        
        def make_compute(vid, edges):
            def compute(inputs):
                current_dist = inputs.get(f"dist_{vid}", INF)
                incoming = inputs.get(f"inbox_{vid}", INF)
                
                # Check if we got a better distance
                new_dist = min(current_dist, incoming)
                
                if new_dist < current_dist or (vid == source and not has_updated[vid]):
                    has_updated[vid] = True
                    outputs = {"dist": new_dist}
                    for neighbor, weight in edges:
                        outputs[f"to_{neighbor}"] = new_dist + weight
                    return outputs
                return None
            return compute
        
        write_to = {"dist": f"dist_{v}"}
        for neighbor, _ in neighbors:
            write_to[f"to_{neighbor}"] = f"inbox_{neighbor}"
        
        p = add_node(p, f"node_{v}", make_compute(v, neighbors),
            subscribe_to=[f"dist_{v}", f"inbox_{v}"],
            write_to=write_to)
    
    return p


def create_graph_diameter_graph(graph):
    """
    Graph Diameter (Longest Shortest Path) - Find the maximum distance between any two vertices.
    
    This runs a distributed all-pairs shortest path by having each vertex compute
    distances to all other vertices, then finding the maximum.
    
    Args:
        graph: Dict of {vertex: [(neighbor, weight), ...]}
    """
    p = pregel(debug=False)
    
    INF = 999999
    vertices = list(graph.keys())
    n = len(vertices)
    
    # Each vertex maintains distances to ALL other vertices
    # Channel for each vertex's distance vector
    for v in vertices:
        # Initial distances: 0 to self, INF to others
        initial_dists = {u: (0 if u == v else INF) for u in vertices}
        p = add_channel(p, f"dists_{v}", "LastValue", initial_value=initial_dists)
        # Inbox receives distance vectors from neighbors
        p = add_channel(p, f"inbox_{v}", "Topic")
    
    # Global diameter tracking
    p = add_channel(p, "diameter", "BinaryOperator", operator=max, initial_value=0)
    
    iteration = [0]  # Mutable counter
    
    for v in vertices:
        neighbors = graph[v]
        
        def make_compute(vid, edges):
            def compute(inputs):
                my_dists = inputs.get(f"dists_{vid}", {})
                messages = inputs.get(f"inbox_{vid}", [])
                
                # First iteration: just send my distances
                if iteration[0] == 0 and vid == vertices[0]:
                    iteration[0] = 1
                
                updated = False
                new_dists = dict(my_dists)
                
                # Process incoming distance vectors
                for msg in messages:
                    neighbor_id, neighbor_dists, edge_weight = msg
                    for dest, dist in neighbor_dists.items():
                        if dist + edge_weight < new_dists.get(dest, INF):
                            new_dists[dest] = dist + edge_weight
                            updated = True
                
                # Compute local max distance (contribution to diameter)
                local_max = max((d for d in new_dists.values() if d < INF), default=0)
                
                if updated or (not messages and iteration[0] <= 1):
                    outputs = {"dists": new_dists, "diam": local_max}
                    # Send my distances to neighbors
                    for neighbor, weight in edges:
                        outputs[f"to_{neighbor}"] = (vid, new_dists, weight)
                    return outputs
                
                return None
            return compute
        
        write_to = {"dists": f"dists_{v}", "diam": "diameter"}
        for neighbor, _ in neighbors:
            write_to[f"to_{neighbor}"] = f"inbox_{neighbor}"
        
        p = add_node(p, f"node_{v}", make_compute(v, neighbors),
            subscribe_to=[f"dists_{v}", f"inbox_{v}"],
            write_to=write_to)
    
    return p


def create_connected_components_graph(graph):
    """
    Connected Components - Label propagation algorithm.
    
    Each vertex starts with its own ID as component label.
    Vertices propagate the minimum label they've seen.
    When converged, all vertices in the same component have the same label.
    
    Args:
        graph: Dict of {vertex: [neighbors]} (unweighted, undirected)
    """
    p = pregel(debug=False)
    
    vertices = list(graph.keys())
    # Assign numeric IDs to vertices for min comparison
    vertex_ids = {v: i for i, v in enumerate(vertices)}
    
    # Each vertex has a component label (initially its own ID)
    for v in vertices:
        p = add_channel(p, f"label_{v}", "LastValue", initial_value=vertex_ids[v])
        p = add_channel(p, f"inbox_{v}", "BinaryOperator", 
                       operator=min, initial_value=len(vertices))
    
    sent_initial = {v: False for v in vertices}
    
    for v in vertices:
        neighbors = graph[v]
        
        def make_compute(vid, neighs):
            def compute(inputs):
                current_label = inputs.get(f"label_{vid}", vertex_ids[vid])
                incoming = inputs.get(f"inbox_{vid}", len(vertices))
                
                new_label = min(current_label, incoming)
                
                # Send if label improved or first iteration
                if new_label < current_label or not sent_initial[vid]:
                    sent_initial[vid] = True
                    outputs = {"label": new_label}
                    for neighbor in neighs:
                        outputs[f"to_{neighbor}"] = new_label
                    return outputs
                return None
            return compute
        
        write_to = {"label": f"label_{v}"}
        for neighbor in neighbors:
            write_to[f"to_{neighbor}"] = f"inbox_{neighbor}"
        
        p = add_node(p, f"node_{v}", make_compute(v, neighbors),
            subscribe_to=[f"label_{v}", f"inbox_{v}"],
            write_to=write_to)
    
    return p


def create_bfs_graph(graph, source):
    """
    Breadth-First Search - Compute BFS levels from source.
    
    Each vertex computes its distance (in hops) from the source.
    
    Args:
        graph: Dict of {vertex: [neighbors]} (unweighted)
        source: Starting vertex
    """
    p = pregel(debug=False)
    
    INF = 999999
    vertices = list(graph.keys())
    
    # Level channels
    for v in vertices:
        initial_level = 0 if v == source else INF
        p = add_channel(p, f"level_{v}", "LastValue", initial_value=initial_level)
        p = add_channel(p, f"inbox_{v}", "BinaryOperator",
            operator=min, initial_value=INF)
    
    sent = {v: False for v in vertices}
    
    for v in vertices:
        neighbors = graph[v]
        
        def make_compute(vid, neighs):
            def compute(inputs):
                current_level = inputs.get(f"level_{vid}", INF)
                incoming = inputs.get(f"inbox_{vid}", INF)
                
                new_level = min(current_level, incoming)
                
                if new_level < current_level or (vid == source and not sent[vid]):
                    sent[vid] = True
                    outputs = {"level": new_level}
                    # Send level + 1 to neighbors
                    for neighbor in neighs:
                        outputs[f"to_{neighbor}"] = new_level + 1
                    return outputs
                return None
            return compute
        
        write_to = {"level": f"level_{v}"}
        for neighbor in neighbors:
            write_to[f"to_{neighbor}"] = f"inbox_{neighbor}"
        
        p = add_node(p, f"node_{v}", make_compute(v, neighbors),
            subscribe_to=[f"level_{v}", f"inbox_{v}"],
            write_to=write_to)
    
    return p


def create_triangle_count_graph(graph):
    """
    Triangle Counting - Count triangles each vertex participates in.
    
    A triangle exists when vertex A connects to B, B connects to C, and C connects to A.
    
    Args:
        graph: Dict of {vertex: [neighbors]} (undirected)
    """
    p = pregel(debug=False)
    
    vertices = list(graph.keys())
    
    # Each vertex tracks its neighbor set and triangle count
    for v in vertices:
        neighbor_set = set(graph[v])
        p = add_channel(p, f"neighbors_{v}", "LastValue", initial_value=neighbor_set)
        p = add_channel(p, f"triangles_{v}", "LastValue", initial_value=0)
        p = add_channel(p, f"inbox_{v}", "Topic")
    
    # Global triangle count (each triangle counted 3x, once per vertex)
    p = add_channel(p, "total_triangles", "BinaryOperator", 
                   operator=lambda a, b: a + b, initial_value=0)
    
    phase = [0]  # 0: send neighbors, 1: count triangles
    
    for v in vertices:
        neighbors = graph[v]
        
        def make_compute(vid, neighs):
            def compute(inputs):
                my_neighbors = inputs.get(f"neighbors_{vid}", set())
                messages = inputs.get(f"inbox_{vid}", [])
                
                if phase[0] == 0:
                    # Phase 0: Send my neighbor list to all neighbors
                    phase[0] = 1
                    outputs = {}
                    for neighbor in neighs:
                        outputs[f"to_{neighbor}"] = (vid, my_neighbors)
                    return outputs
                else:
                    # Phase 1: Count triangles by checking neighbor intersections
                    triangle_count = 0
                    for sender, sender_neighbors in messages:
                        # Triangle exists if sender's neighbor is also my neighbor
                        common = my_neighbors.intersection(sender_neighbors)
                        # Only count if common neighbor has higher ID (avoid triple counting)
                        for c in common:
                            if c > vid and c > sender:
                                triangle_count += 1
                    
                    if triangle_count > 0:
                        return {"count": triangle_count, "total": triangle_count}
                    return None
            return compute
        
        write_to = {"count": f"triangles_{v}", "total": "total_triangles"}
        for neighbor in neighbors:
            write_to[f"to_{neighbor}"] = f"inbox_{neighbor}"
        
        p = add_node(p, f"node_{v}", make_compute(v, neighbors),
            subscribe_to=[f"neighbors_{v}", f"inbox_{v}"],
            write_to=write_to)
    
    return p


def run_pregel_animations():
    """Generate animated GIFs for Pregel examples."""
    print("\033[36m" + "=" * 60 + "\033[0m")
    print("\033[36m  PREGEL ANIMATIONS\033[0m")
    print("\033[36m" + "=" * 60 + "\033[0m\n")
    
    # Scenario 1: Counter from 0 to 5
    print("\n[1/14] Counter: 0 -> 5")
    p = create_counter_graph(initial_value=0, max_count=5)
    pregel_render_animation(p, 
        os.path.join(OUTPUT_DIR, "pregel_01_counter_0to5"),
        max_supersteps=10, frame_delay=80, title="Counter: 0 to 5")
    
    # Scenario 2: Counter from 3 to 7
    print("\n[2/14] Counter: 3 -> 7")
    p = create_counter_graph(initial_value=3, max_count=7)
    pregel_render_animation(p,
        os.path.join(OUTPUT_DIR, "pregel_02_counter_3to7"),
        max_supersteps=10, frame_delay=80, title="Counter: 3 to 7")
    
    # Scenario 3: Ping-pong (ball starts at A)
    print("\n[3/14] Ping-Pong: Ball at A")
    p = create_ping_pong_graph(initial_a=1, initial_b=0)
    pregel_render_animation(p,
        os.path.join(OUTPUT_DIR, "pregel_03_pingpong_a"),
        max_supersteps=8, frame_delay=60, title="Ping-Pong (Ball at A)")
    
    # Scenario 4: Ping-pong (ball starts at B)
    print("\n[4/14] Ping-Pong: Ball at B")
    p = create_ping_pong_graph(initial_a=0, initial_b=1)
    pregel_render_animation(p,
        os.path.join(OUTPUT_DIR, "pregel_04_pingpong_b"),
        max_supersteps=8, frame_delay=60, title="Ping-Pong (Ball at B)")
    
    # Scenario 5: Sum aggregation with 4 values
    print("\n[5/14] Sum Aggregation: [10, 20, 30, 40]")
    p = create_sum_propagation_graph([10, 20, 30, 40])
    pregel_render_animation(p,
        os.path.join(OUTPUT_DIR, "pregel_05_sum_aggregation"),
        max_supersteps=5, frame_delay=100, title="Sum Aggregation")
    
    # Scenario 6: PageRank - Small triangle graph
    print("\n[6/14] PageRank: Triangle Graph (A->B->C->A)")
    triangle_graph = {
        "A": ["B"],
        "B": ["C"],
        "C": ["A"]
    }
    p = create_pagerank_graph(triangle_graph, max_iterations=8)
    pregel_render_animation(p,
        os.path.join(OUTPUT_DIR, "pregel_06_pagerank_triangle"),
        max_supersteps=12, frame_delay=70, title="PageRank: Triangle")
    
    # Scenario 7: PageRank - Star graph (hub and spokes)
    print("\n[7/14] PageRank: Star Graph (Hub with 4 spokes)")
    star_graph = {
        "Hub": ["S1", "S2", "S3", "S4"],
        "S1": ["Hub"],
        "S2": ["Hub"],
        "S3": ["Hub"],
        "S4": ["Hub"]
    }
    p = create_pagerank_graph(star_graph, max_iterations=8)
    pregel_render_animation(p,
        os.path.join(OUTPUT_DIR, "pregel_07_pagerank_star"),
        max_supersteps=12, frame_delay=70, title="PageRank: Star")
    
    # Scenario 8: Max Value - 4 vertices
    print("\n[8/14] Max Value: Find maximum among [3, 7, 2, 5]")
    p = create_max_value_graph({"A": 3, "B": 7, "C": 2, "D": 5})
    pregel_render_animation(p,
        os.path.join(OUTPUT_DIR, "pregel_08_maxvalue_4nodes"),
        max_supersteps=10, frame_delay=80, title="Max Value: [3,7,2,5]")
    
    # Scenario 9: Max Value - 3 vertices with different max
    print("\n[9/14] Max Value: Find maximum among [10, 5, 15]")
    p = create_max_value_graph({"X": 10, "Y": 5, "Z": 15})
    pregel_render_animation(p,
        os.path.join(OUTPUT_DIR, "pregel_09_maxvalue_3nodes"),
        max_supersteps=10, frame_delay=80, title="Max Value: [10,5,15]")
    
    # Scenario 10: Shortest Path - Simple graph
    print("\n[10/14] Shortest Path: From A in weighted graph")
    sssp_graph = {
        "A": [("B", 1), ("C", 4)],
        "B": [("C", 2), ("D", 5)],
        "C": [("D", 1)],
        "D": []
    }
    p = create_shortest_path_graph(sssp_graph, source="A")
    pregel_render_animation(p,
        os.path.join(OUTPUT_DIR, "pregel_10_shortest_path"),
        max_supersteps=10, frame_delay=90, title="Shortest Path from A")
    
    # Scenario 11: BFS - Breadth-first search levels
    print("\n[11/14] BFS: Levels from vertex A")
    bfs_graph = {
        "A": ["B", "C"],
        "B": ["A", "D", "E"],
        "C": ["A", "F"],
        "D": ["B"],
        "E": ["B", "F"],
        "F": ["C", "E"]
    }
    p = create_bfs_graph(bfs_graph, source="A")
    pregel_render_animation(p,
        os.path.join(OUTPUT_DIR, "pregel_11_bfs_levels"),
        max_supersteps=10, frame_delay=80, title="BFS from A")
    
    # Scenario 12: Connected Components - Two disconnected components
    print("\n[12/14] Connected Components: Two components")
    cc_graph = {
        # Component 1: A-B-C
        "A": ["B"],
        "B": ["A", "C"],
        "C": ["B"],
        # Component 2: X-Y-Z
        "X": ["Y"],
        "Y": ["X", "Z"],
        "Z": ["Y"]
    }
    p = create_connected_components_graph(cc_graph)
    pregel_render_animation(p,
        os.path.join(OUTPUT_DIR, "pregel_12_connected_components"),
        max_supersteps=10, frame_delay=80, title="Connected Components")
    
    # Scenario 13: Shortest Path - Longer path graph (for diameter)
    print("\n[13/14] Shortest Path: Longer chain A->B->C->D->E")
    chain_graph = {
        "A": [("B", 2)],
        "B": [("A", 2), ("C", 3)],
        "C": [("B", 3), ("D", 1)],
        "D": [("C", 1), ("E", 4)],
        "E": [("D", 4)]
    }
    p = create_shortest_path_graph(chain_graph, source="A")
    pregel_render_animation(p,
        os.path.join(OUTPUT_DIR, "pregel_13_shortest_path_chain"),
        max_supersteps=12, frame_delay=80, title="SSSP: Chain Graph")
    
    # Scenario 14: Triangle Counting
    print("\n[14/14] Triangle Count: Graph with triangles")
    triangle_count_graph = {
        # Graph with 2 triangles: A-B-C and B-C-D
        "A": ["B", "C"],
        "B": ["A", "C", "D"],
        "C": ["A", "B", "D"],
        "D": ["B", "C"]
    }
    p = create_triangle_count_graph(triangle_count_graph)
    pregel_render_animation(p,
        os.path.join(OUTPUT_DIR, "pregel_14_triangle_count"),
        max_supersteps=5, frame_delay=100, title="Triangle Counting")


# =============================================================================
# PETRI NET ANIMATIONS
# =============================================================================

def create_producer_consumer_net(buffer_size=3, initial_items=0):
    """
    Producer-consumer with bounded buffer.
    Classic Petri net pattern.
    """
    net = petri_net(debug=False)
    
    net = add_place(net, "buffer", capacity=buffer_size, initial_tokens=initial_items)
    net = add_place(net, "empty_slots", capacity=buffer_size, 
                   initial_tokens=buffer_size - initial_items)
    
    net = add_transition(net, "produce",
        consume_from=[("empty_slots", 1)],
        produce_to=[("buffer", 1)])
    
    net = add_transition(net, "consume",
        consume_from=[("buffer", 1)],
        produce_to=[("empty_slots", 1)])
    
    return net


def create_mutex_net(resource_count=1, p1_active=True, p2_active=True):
    """
    Mutual exclusion with two processes.
    """
    net = petri_net(debug=False)
    
    net = add_place(net, "resource", initial_tokens=resource_count)
    net = add_place(net, "p1_idle", initial_tokens=1 if p1_active else 0)
    net = add_place(net, "p1_working", initial_tokens=0)
    net = add_place(net, "p2_idle", initial_tokens=1 if p2_active else 0)
    net = add_place(net, "p2_working", initial_tokens=0)
    
    net = add_transition(net, "p1_acquire",
        consume_from=[("p1_idle", 1), ("resource", 1)],
        produce_to=[("p1_working", 1)],
        priority=2)  # P1 has higher priority
    
    net = add_transition(net, "p1_release",
        consume_from=[("p1_working", 1)],
        produce_to=[("p1_idle", 1), ("resource", 1)])
    
    net = add_transition(net, "p2_acquire",
        consume_from=[("p2_idle", 1), ("resource", 1)],
        produce_to=[("p2_working", 1)],
        priority=1)
    
    net = add_transition(net, "p2_release",
        consume_from=[("p2_working", 1)],
        produce_to=[("p2_idle", 1), ("resource", 1)])
    
    return net


def create_traffic_light_net(initial_state="red"):
    """
    Simple traffic light state machine.
    """
    net = petri_net(debug=False)
    
    states = {"red": 0, "yellow": 0, "green": 0}
    states[initial_state] = 1
    
    net = add_place(net, "red", initial_tokens=states["red"])
    net = add_place(net, "yellow", initial_tokens=states["yellow"])
    net = add_place(net, "green", initial_tokens=states["green"])
    
    net = add_transition(net, "red_to_green",
        consume_from=[("red", 1)],
        produce_to=[("green", 1)])
    
    net = add_transition(net, "green_to_yellow",
        consume_from=[("green", 1)],
        produce_to=[("yellow", 1)])
    
    net = add_transition(net, "yellow_to_red",
        consume_from=[("yellow", 1)],
        produce_to=[("red", 1)])
    
    return net


def create_fork_join_net(parallel_branches=2):
    """
    Fork-join workflow pattern.
    """
    net = petri_net(debug=False)
    
    net = add_place(net, "start", initial_tokens=1)
    net = add_place(net, "end", initial_tokens=0)
    
    # Create branch places
    branch_places = []
    done_places = []
    for i in range(parallel_branches):
        branch_name = f"branch_{i}"
        done_name = f"done_{i}"
        branch_places.append(branch_name)
        done_places.append(done_name)
        net = add_place(net, branch_name, initial_tokens=0)
        net = add_place(net, done_name, initial_tokens=0)
    
    # Fork transition
    net = add_transition(net, "fork",
        consume_from=[("start", 1)],
        produce_to=[(b, 1) for b in branch_places])
    
    # Process transitions
    for i in range(parallel_branches):
        net = add_transition(net, f"process_{i}",
            consume_from=[(branch_places[i], 1)],
            produce_to=[(done_places[i], 1)])
    
    # Join transition
    net = add_transition(net, "join",
        consume_from=[(d, 1) for d in done_places],
        produce_to=[("end", 1)])
    
    return net


def create_dining_philosophers_deadlock_net():
    """
    Dining Philosophers - DEADLOCK VERSION.
    
    Two philosophers share two forks. Each picks up their left fork first,
    then tries to get their right fork. This creates a circular wait that
    leads to deadlock when both hold their left fork simultaneously.
    
    The animation will show the system reaching the deadlock state where
    both philosophers are stuck waiting for their right fork.
    """
    net = petri_net(debug=False)
    
    # Forks (shared resources)
    net = add_place(net, "fork1", initial_tokens=1)
    net = add_place(net, "fork2", initial_tokens=1)
    
    # Philosopher 1 states
    net = add_place(net, "phil1_thinking", initial_tokens=1)
    net = add_place(net, "phil1_has_left", initial_tokens=0)  # Has fork1 only
    net = add_place(net, "phil1_eating", initial_tokens=0)    # Has both forks
    
    # Philosopher 2 states  
    net = add_place(net, "phil2_thinking", initial_tokens=1)
    net = add_place(net, "phil2_has_left", initial_tokens=0)  # Has fork2 only
    net = add_place(net, "phil2_eating", initial_tokens=0)    # Has both forks
    
    # Philosopher 1 transitions:
    # 1. Pick up left fork (fork1)
    net = add_transition(net, "phil1_take_left",
        consume_from=[("phil1_thinking", 1), ("fork1", 1)],
        produce_to=[("phil1_has_left", 1)])
    
    # 2. Pick up right fork (fork2) - needs to already have left
    net = add_transition(net, "phil1_take_right",
        consume_from=[("phil1_has_left", 1), ("fork2", 1)],
        produce_to=[("phil1_eating", 1)])
    
    # 3. Release both forks
    net = add_transition(net, "phil1_release",
        consume_from=[("phil1_eating", 1)],
        produce_to=[("phil1_thinking", 1), ("fork1", 1), ("fork2", 1)])
    
    # Philosopher 2 transitions (picks up fork2 first, then fork1):
    # 1. Pick up left fork (fork2 for phil2)
    net = add_transition(net, "phil2_take_left",
        consume_from=[("phil2_thinking", 1), ("fork2", 1)],
        produce_to=[("phil2_has_left", 1)])
    
    # 2. Pick up right fork (fork1) - needs to already have left
    net = add_transition(net, "phil2_take_right",
        consume_from=[("phil2_has_left", 1), ("fork1", 1)],
        produce_to=[("phil2_eating", 1)])
    
    # 3. Release both forks
    net = add_transition(net, "phil2_release",
        consume_from=[("phil2_eating", 1)],
        produce_to=[("phil2_thinking", 1), ("fork1", 1), ("fork2", 1)])
    
    return net


def run_petri_animations():
    """Generate animated GIFs for Petri net examples."""
    print("\n\033[36m" + "=" * 60 + "\033[0m")
    print("\033[36m  PETRI NET ANIMATIONS\033[0m")
    print("\033[36m" + "=" * 60 + "\033[0m\n")
    
    # Scenario 1: Producer-consumer (empty buffer)
    print("\n[1/6] Producer-Consumer: Empty buffer")
    net = create_producer_consumer_net(buffer_size=3, initial_items=0)
    petri_render_animation(net,
        os.path.join(OUTPUT_DIR, "petri_01_prodcons_empty"),
        max_steps=8, frame_delay=80, title="Producer-Consumer (Empty)")
    
    # Scenario 2: Producer-consumer (half-full buffer)
    print("\n[2/6] Producer-Consumer: Half-full buffer")
    net = create_producer_consumer_net(buffer_size=4, initial_items=2)
    petri_render_animation(net,
        os.path.join(OUTPUT_DIR, "petri_02_prodcons_half"),
        max_steps=8, frame_delay=80, title="Producer-Consumer (Half-Full)")
    
    # Scenario 3: Mutex (both processes competing)
    print("\n[3/6] Mutex: Both processes active")
    net = create_mutex_net(resource_count=1, p1_active=True, p2_active=True)
    petri_render_animation(net,
        os.path.join(OUTPUT_DIR, "petri_03_mutex_both"),
        max_steps=10, frame_delay=70, title="Mutex (Both Active)")
    
    # Scenario 4: Traffic light cycle
    print("\n[4/6] Traffic Light: Full cycle")
    net = create_traffic_light_net(initial_state="red")
    petri_render_animation(net,
        os.path.join(OUTPUT_DIR, "petri_04_traffic_light"),
        max_steps=6, frame_delay=100, title="Traffic Light Cycle")
    
    # Scenario 5: Fork-join workflow (3 branches)
    print("\n[5/6] Fork-Join: 3 parallel branches")
    net = create_fork_join_net(parallel_branches=3)
    petri_render_animation(net,
        os.path.join(OUTPUT_DIR, "petri_05_fork_join"),
        max_steps=10, frame_delay=80, title="Fork-Join Workflow")
    
    # Scenario 6: Dining Philosophers Deadlock
    print("\n[6/6] Dining Philosophers: DEADLOCK")
    net = create_dining_philosophers_deadlock_net()
    petri_render_animation(net,
        os.path.join(OUTPUT_DIR, "petri_06_philosophers_deadlock"),
        max_steps=10, frame_delay=100, title="Dining Philosophers (DEADLOCK)")


# =============================================================================
# MAIN
# =============================================================================

def main():
    print("\n" + "#" * 60)
    print("#" + "  ANIMATED EXAMPLES".center(58) + "#")
    print("#" * 60 + "\n")
    
    ensure_output_dir()
    
    run_pregel_animations()
    run_petri_animations()
    
    print("\n" + "#" * 60)
    print("#" + "  ALL ANIMATIONS COMPLETE".center(58) + "#")
    print("#" * 60)
    
    # List generated files
    print(f"\n\033[36mGenerated animations in: {OUTPUT_DIR}\033[0m\n")
    if os.path.exists(OUTPUT_DIR):
        files = sorted(os.listdir(OUTPUT_DIR))
        for f in files:
            if f.endswith('.gif'):
                size = os.path.getsize(os.path.join(OUTPUT_DIR, f))
                print(f"  - {f} ({size // 1024} KB)")
    print()


if __name__ == "__main__":
    main()
