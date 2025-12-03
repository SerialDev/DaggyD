"""
STRESS TEST: Complex Hybrid Pipeline with Parallel Lanes

This is a stress test demonstrating:
- Multiple parallel processing lanes (3 workers)
- Resource pools (shared GPU, memory)
- Fork-join patterns
- Multiple Pregel computations per lane
- Synchronization barriers
- Complex token flow

Pipeline Architecture:
                                    ┌─── Lane A ───┐
    [input] → [dispatch] → fork ───┼─── Lane B ───┼─── join → [merge] → [output]
                                    └─── Lane C ───┘
                                    
Each lane runs: validate → transform → aggregate

Resources:
- 2 GPU units (shared across lanes)
- 3 memory slots
- 1 output lock
"""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from DaggyD.pregel_core import pregel, add_channel, add_node
from DaggyD.petri_net import petri_net, add_place, add_transition
from DaggyD.hybrid import render_hybrid_animation

OUTPUT_DIR = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "example_outputs", "animations"
)


# =============================================================================
# PREGEL COMPUTATIONS - Various distributed algorithms
# =============================================================================

def create_validation_graph(lane_id, data):
    """Validate data bounds for a specific lane."""
    p = pregel(debug=False)
    
    for i, val in enumerate(data):
        ch = f"L{lane_id}_v{i}"
        p = add_channel(p, ch, "LastValue", initial_value=val)
        
        def make_fn(v):
            done = [False]
            def fn(inputs):
                if done[0]: return None
                done[0] = True
                return {"out": "OK" if 0 <= v <= 100 else "ERR"}
            return fn
        
        p = add_node(p, f"chk{i}", make_fn(val),
            subscribe_to=[ch], write_to={"out": ch})
    
    return p


def create_transform_graph(lane_id, data):
    """Transform data: normalize and compute derived values."""
    p = pregel(debug=False)
    
    mx = max(data) if data else 1
    
    for i, val in enumerate(data):
        ch = f"L{lane_id}_t{i}"
        p = add_channel(p, ch, "LastValue", initial_value=val)
        
        def make_fn(v, m):
            done = [False]
            def fn(inputs):
                if done[0]: return None
                done[0] = True
                norm = round(v / m, 2) if m else 0
                return {"out": norm}
            return fn
        
        p = add_node(p, f"tx{i}", make_fn(val, mx),
            subscribe_to=[ch], write_to={"out": ch})
    
    return p


def create_reduce_graph(lane_id, data):
    """Reduce: aggregate results from lane."""
    p = pregel(debug=False)
    
    p = add_channel(p, f"L{lane_id}_sum", "BinaryOperator", 
        operator=lambda a,b: round(a+b, 2), initial_value=0)
    p = add_channel(p, f"L{lane_id}_cnt", "BinaryOperator",
        operator=lambda a,b: a+b, initial_value=0)
    
    for i, val in enumerate(data):
        ch = f"L{lane_id}_r{i}"
        p = add_channel(p, ch, "LastValue", initial_value=val)
        
        def make_fn(v):
            done = [False]
            def fn(inputs):
                if done[0]: return None
                done[0] = True
                return {"s": v, "c": 1}
            return fn
        
        p = add_node(p, f"rd{i}", make_fn(val),
            subscribe_to=[ch], 
            write_to={"s": f"L{lane_id}_sum", "c": f"L{lane_id}_cnt"})
    
    return p


def create_merge_graph():
    """Final merge: combine results from all lanes."""
    p = pregel(debug=False)
    
    p = add_channel(p, "total", "BinaryOperator",
        operator=lambda a,b: round(a+b, 2), initial_value=0)
    
    # Simulate merging 3 lane results
    for lane in ["A", "B", "C"]:
        ch = f"lane_{lane}"
        val = {"A": 1.5, "B": 2.0, "C": 1.8}[lane]
        p = add_channel(p, ch, "LastValue", initial_value=val)
        
        def make_fn(v):
            done = [False]
            def fn(inputs):
                if done[0]: return None
                done[0] = True
                return {"out": v}
            return fn
        
        p = add_node(p, f"merge_{lane}", make_fn(val),
            subscribe_to=[ch], write_to={"out": "total"})
    
    return p


# =============================================================================
# PETRI NET - Complex workflow with parallel lanes and resources
# =============================================================================

def create_complex_pipeline():
    """
    Complex Petri net with:
    - 3 parallel processing lanes (A, B, C)
    - Shared resources (GPU pool, memory)
    - Fork/join synchronization
    - Multiple stages per lane
    """
    net = petri_net(debug=False)
    
    # === ALL PLACES FIRST ===
    
    # Resources
    net = add_place(net, "gpu", initial_tokens=2)      # 2 GPU units
    net = add_place(net, "mem", initial_tokens=3)      # 3 memory slots
    net = add_place(net, "lock", initial_tokens=1)     # Output lock
    
    # Main flow
    net = add_place(net, "start", initial_tokens=1)
    net = add_place(net, "merged", initial_tokens=0)
    net = add_place(net, "done", initial_tokens=0)
    
    # Lane A places
    net = add_place(net, "A_wait", initial_tokens=0)
    net = add_place(net, "A_val", initial_tokens=0)
    net = add_place(net, "A_ok", initial_tokens=0)
    net = add_place(net, "A_tx", initial_tokens=0)
    net = add_place(net, "A_txd", initial_tokens=0)
    net = add_place(net, "A_agg", initial_tokens=0)
    net = add_place(net, "A_done", initial_tokens=0)
    
    # Lane B places
    net = add_place(net, "B_wait", initial_tokens=0)
    net = add_place(net, "B_val", initial_tokens=0)
    net = add_place(net, "B_ok", initial_tokens=0)
    net = add_place(net, "B_tx", initial_tokens=0)
    net = add_place(net, "B_txd", initial_tokens=0)
    net = add_place(net, "B_agg", initial_tokens=0)
    net = add_place(net, "B_done", initial_tokens=0)
    
    # Lane C places
    net = add_place(net, "C_wait", initial_tokens=0)
    net = add_place(net, "C_val", initial_tokens=0)
    net = add_place(net, "C_ok", initial_tokens=0)
    net = add_place(net, "C_tx", initial_tokens=0)
    net = add_place(net, "C_txd", initial_tokens=0)
    net = add_place(net, "C_agg", initial_tokens=0)
    net = add_place(net, "C_done", initial_tokens=0)
    
    # === TRANSITIONS ===
    
    # Fork: start -> all lanes
    net = add_transition(net, "fork",
        consume_from=[("start", 1)],
        produce_to=[("A_wait", 1), ("B_wait", 1), ("C_wait", 1)],
        priority=100)
    
    # Lane A transitions
    net = add_transition(net, "A_validate",
        consume_from=[("A_wait", 1), ("gpu", 1)],
        produce_to=[("A_val", 1)],
        priority=10)
    
    net = add_transition(net, "A_val_done",
        consume_from=[("A_val", 1)],
        produce_to=[("A_ok", 1), ("gpu", 1)])
    
    net = add_transition(net, "A_transform",
        consume_from=[("A_ok", 1), ("mem", 1)],
        produce_to=[("A_tx", 1)])
    
    net = add_transition(net, "A_tx_done",
        consume_from=[("A_tx", 1)],
        produce_to=[("A_txd", 1), ("mem", 1)])
    
    net = add_transition(net, "A_reduce",
        consume_from=[("A_txd", 1), ("gpu", 1)],
        produce_to=[("A_agg", 1)])
    
    net = add_transition(net, "A_agg_done",
        consume_from=[("A_agg", 1)],
        produce_to=[("A_done", 1), ("gpu", 1)])
    
    # Lane B transitions
    net = add_transition(net, "B_validate",
        consume_from=[("B_wait", 1), ("gpu", 1)],
        produce_to=[("B_val", 1)],
        priority=9)
    
    net = add_transition(net, "B_val_done",
        consume_from=[("B_val", 1)],
        produce_to=[("B_ok", 1), ("gpu", 1)])
    
    net = add_transition(net, "B_transform",
        consume_from=[("B_ok", 1), ("mem", 1)],
        produce_to=[("B_tx", 1)])
    
    net = add_transition(net, "B_tx_done",
        consume_from=[("B_tx", 1)],
        produce_to=[("B_txd", 1), ("mem", 1)])
    
    net = add_transition(net, "B_reduce",
        consume_from=[("B_txd", 1), ("gpu", 1)],
        produce_to=[("B_agg", 1)])
    
    net = add_transition(net, "B_agg_done",
        consume_from=[("B_agg", 1)],
        produce_to=[("B_done", 1), ("gpu", 1)])
    
    # Lane C transitions
    net = add_transition(net, "C_validate",
        consume_from=[("C_wait", 1), ("gpu", 1)],
        produce_to=[("C_val", 1)],
        priority=8)
    
    net = add_transition(net, "C_val_done",
        consume_from=[("C_val", 1)],
        produce_to=[("C_ok", 1), ("gpu", 1)])
    
    net = add_transition(net, "C_transform",
        consume_from=[("C_ok", 1), ("mem", 1)],
        produce_to=[("C_tx", 1)])
    
    net = add_transition(net, "C_tx_done",
        consume_from=[("C_tx", 1)],
        produce_to=[("C_txd", 1), ("mem", 1)])
    
    net = add_transition(net, "C_reduce",
        consume_from=[("C_txd", 1), ("gpu", 1)],
        produce_to=[("C_agg", 1)])
    
    net = add_transition(net, "C_agg_done",
        consume_from=[("C_agg", 1)],
        produce_to=[("C_done", 1), ("gpu", 1)])
    
    # === JOIN & MERGE ===
    # Join: wait for all lanes to complete
    net = add_transition(net, "join",
        consume_from=[("A_done", 1), ("B_done", 1), ("C_done", 1)],
        produce_to=[("merged", 1)])
    
    # Final merge (needs lock)
    net = add_transition(net, "merge",
        consume_from=[("merged", 1), ("lock", 1)],
        produce_to=[("done", 1), ("lock", 1)])
    
    return net


# =============================================================================
# MAIN
# =============================================================================

def main():
    print("\n" + "=" * 70)
    print("  STRESS TEST: Complex Parallel Pipeline")
    print("=" * 70)
    
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    
    print("""
Architecture:
                        ┌─── Lane A: validate→transform→aggregate ───┐
  [start] → [dispatch] ─┼─── Lane B: validate→transform→aggregate ───┼─ [join] → [merge] → [done]
                        └─── Lane C: validate→transform→aggregate ───┘

Resources: 2 GPUs, 3 memory slots, 1 output lock
Pregel computations triggered at each validate/transform/reduce step
    """)
    
    # Data for each lane
    data_A = [10, 20, 30]
    data_B = [15, 25, 35]
    data_C = [5, 40, 45]
    
    # Create pipeline
    pipeline = create_complex_pipeline()
    
    # Map transitions to Pregel computations
    pregel_factories = {
        # Lane A
        "A_validate": (lambda: create_validation_graph("A", data_A), "A_Valid"),
        "A_transform": (lambda: create_transform_graph("A", data_A), "A_Transform"),
        "A_reduce": (lambda: create_reduce_graph("A", [0.33, 0.67, 1.0]), "A_Reduce"),
        
        # Lane B  
        "B_validate": (lambda: create_validation_graph("B", data_B), "B_Valid"),
        "B_transform": (lambda: create_transform_graph("B", data_B), "B_Transform"),
        "B_reduce": (lambda: create_reduce_graph("B", [0.43, 0.71, 1.0]), "B_Reduce"),
        
        # Lane C
        "C_validate": (lambda: create_validation_graph("C", data_C), "C_Valid"),
        "C_transform": (lambda: create_transform_graph("C", data_C), "C_Transform"),
        "C_reduce": (lambda: create_reduce_graph("C", [0.11, 0.89, 1.0]), "C_Reduce"),
        
        # Final merge
        "merge": (lambda: create_merge_graph(), "FinalMerge"),
    }
    
    print("Generating animation (this may take a moment)...")
    
    render_hybrid_animation(
        pipeline,
        pregel_factories,
        os.path.join(OUTPUT_DIR, "hybrid_complex_pipeline"),
        max_steps=50,  # More steps for complex pipeline
        frame_delay=50,  # Faster animation
        title="Parallel Pipeline Stress Test"
    )
    
    print("\n" + "-" * 50)
    print("Generated: hybrid_complex_pipeline.gif")
    print("-" * 50)


if __name__ == "__main__":
    main()
