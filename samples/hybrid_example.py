"""
Hybrid Example: Petri Net + Pregel Working Together

This demonstrates how to combine both models:
- Petri Net: Controls the workflow/pipeline (when to compute)
- Pregel: Does the actual distributed computation (how to compute)

Example: A data processing pipeline where:
1. Data arrives (Petri net transition)
2. Preprocessing runs (Pregel graph computation)
3. Data moves to next stage (Petri net transition)
4. Main computation runs (different Pregel graph)
5. Results collected (Petri net)

This is a realistic pattern for workflow orchestration + graph computation.
"""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from DaggyD.pregel_core import (
    pregel, add_channel, add_node, run, reset as pregel_reset,
    render_animation as pregel_render_animation
)
from DaggyD.petri_net import (
    petri_net, add_place, add_transition, fire_transition, run as petri_run,
    get_enabled_transitions, render_animation as petri_render_animation
)
from DaggyD.hybrid import render_hybrid_animation

OUTPUT_DIR = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "example_outputs", "animations"
)


def create_preprocessing_graph(data_values):
    """
    Pregel graph that normalizes data (divides by max).
    """
    p = pregel(debug=False)
    
    max_val = max(data_values) if data_values else 1
    
    # Each node holds a value and normalizes it
    for i, val in enumerate(data_values):
        ch_name = f"data_{i}"
        p = add_channel(p, ch_name, "LastValue", initial_value=val)
        
        def make_normalize(idx, v, m):
            done = [False]
            def normalize(inputs):
                if done[0]:
                    return None
                done[0] = True
                return {"out": v / m}
            return normalize
        
        p = add_node(p, f"normalize_{i}", make_normalize(i, val, max_val),
            subscribe_to=[ch_name],
            write_to={"out": ch_name})
    
    return p


def create_aggregation_graph(data_values):
    """
    Pregel graph that computes sum and average.
    """
    p = pregel(debug=False)
    
    p = add_channel(p, "sum", "BinaryOperator", operator=lambda a, b: a + b, initial_value=0)
    p = add_channel(p, "count", "BinaryOperator", operator=lambda a, b: a + b, initial_value=0)
    
    for i, val in enumerate(data_values):
        ch_name = f"val_{i}"
        p = add_channel(p, ch_name, "LastValue", initial_value=val)
        
        def make_contribute(v):
            done = [False]
            def contribute(inputs):
                if done[0]:
                    return None
                done[0] = True
                return {"s": v, "c": 1}
            return contribute
        
        p = add_node(p, f"node_{i}", make_contribute(val),
            subscribe_to=[ch_name],
            write_to={"s": "sum", "c": "count"})
    
    return p


def create_pipeline_petri_net():
    """
    Petri net that orchestrates: Input -> Preprocess -> Compute -> Output
    """
    net = petri_net(debug=False)
    
    # Places represent stages/states
    net = add_place(net, "input_ready", initial_tokens=1)
    net = add_place(net, "preprocessing", initial_tokens=0)
    net = add_place(net, "preprocessed", initial_tokens=0)
    net = add_place(net, "computing", initial_tokens=0)
    net = add_place(net, "done", initial_tokens=0)
    
    # Resource: computation unit (only one computation at a time)
    net = add_place(net, "compute_unit", initial_tokens=1)
    
    # Transitions
    net = add_transition(net, "start_preprocess",
        consume_from=[("input_ready", 1), ("compute_unit", 1)],
        produce_to=[("preprocessing", 1)])
    
    net = add_transition(net, "finish_preprocess",
        consume_from=[("preprocessing", 1)],
        produce_to=[("preprocessed", 1), ("compute_unit", 1)])
    
    net = add_transition(net, "start_compute",
        consume_from=[("preprocessed", 1), ("compute_unit", 1)],
        produce_to=[("computing", 1)])
    
    net = add_transition(net, "finish_compute",
        consume_from=[("computing", 1)],
        produce_to=[("done", 1), ("compute_unit", 1)])
    
    return net


def run_hybrid_pipeline():
    """
    Run the hybrid pipeline: Petri net orchestrates, Pregel computes.
    """
    print("\n" + "=" * 60)
    print("  HYBRID PIPELINE: Petri Net + Pregel")
    print("=" * 60)
    
    # Sample data
    data = [10, 20, 30, 40, 50]
    print(f"\nInput data: {data}")
    
    # Create the orchestration Petri net
    pipeline = create_pipeline_petri_net()
    
    results = {}
    
    # Step through the pipeline
    step = 0
    while True:
        enabled = get_enabled_transitions(pipeline)
        if not enabled:
            break
        
        transition = enabled[0]
        step += 1
        
        print(f"\n[Step {step}] Firing: {transition}")
        
        # When certain transitions fire, run Pregel computations
        if transition == "start_preprocess":
            pipeline, _ = fire_transition(pipeline, transition)
            print("  -> Starting preprocessing Pregel graph...")
            preprocess = create_preprocessing_graph(data)
            channel_results = run(preprocess, max_supersteps=5)
            # Get normalized values from channel results
            normalized = []
            for i in range(len(data)):
                val = channel_results.get(f"data_{i}", data[i])
                normalized.append(val)
            results["normalized"] = normalized
            print(f"  -> Normalized: {normalized}")
            
        elif transition == "finish_preprocess":
            pipeline, _ = fire_transition(pipeline, transition)
            print("  -> Preprocessing complete, releasing compute unit")
            
        elif transition == "start_compute":
            pipeline, _ = fire_transition(pipeline, transition)
            print("  -> Starting aggregation Pregel graph...")
            agg = create_aggregation_graph(results["normalized"])
            channel_results = run(agg, max_supersteps=5)
            total = channel_results.get("sum", 0)
            count = channel_results.get("count", 0)
            results["sum"] = total
            results["average"] = total / count if count > 0 else 0
            print(f"  -> Sum: {total}, Average: {results['average']:.3f}")
            
        elif transition == "finish_compute":
            pipeline, _ = fire_transition(pipeline, transition)
            print("  -> Computation complete!")
        
        else:
            pipeline, _ = fire_transition(pipeline, transition)
    
    print(f"\n" + "-" * 40)
    print("PIPELINE COMPLETE")
    print(f"  Input:      {data}")
    print(f"  Normalized: {results.get('normalized', [])}")
    print(f"  Sum:        {results.get('sum', 0):.3f}")
    print(f"  Average:    {results.get('average', 0):.3f}")
    print("-" * 40)
    
    return pipeline, results


def generate_animations():
    """Generate animations including unified hybrid view."""
    print("\n" + "=" * 60)
    print("  GENERATING ANIMATIONS")
    print("=" * 60)
    
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    
    # 1. Pipeline Petri net animation (standalone)
    print("\n[1/4] Pipeline orchestration (Petri Net only)...")
    pipeline = create_pipeline_petri_net()
    petri_render_animation(pipeline,
        os.path.join(OUTPUT_DIR, "hybrid_01_pipeline"),
        max_steps=10, frame_delay=100, title="Pipeline Orchestration")
    
    # 2. Preprocessing Pregel animation (standalone)
    print("\n[2/4] Preprocessing (Pregel only)...")
    preprocess = create_preprocessing_graph([10, 20, 30, 40])
    pregel_render_animation(preprocess,
        os.path.join(OUTPUT_DIR, "hybrid_02_preprocess"),
        max_supersteps=5, frame_delay=100, title="Data Normalization")
    
    # 3. Aggregation Pregel animation (standalone)
    print("\n[3/4] Aggregation (Pregel only)...")
    agg = create_aggregation_graph([0.2, 0.4, 0.6, 0.8, 1.0])
    pregel_render_animation(agg,
        os.path.join(OUTPUT_DIR, "hybrid_03_aggregation"),
        max_supersteps=5, frame_delay=100, title="Sum Aggregation")
    
    # 4. UNIFIED HYBRID VIEW - Petri Net + Pregel together!
    print("\n[4/4] Unified hybrid view (Petri Net + Pregel)...")
    pipeline = create_pipeline_petri_net()
    
    # Define which transitions trigger which Pregel computations
    pregel_factories = {
        "start_preprocess": (
            lambda: create_preprocessing_graph([10, 20, 30, 40]),
            "Preprocess"
        ),
        "start_compute": (
            lambda: create_aggregation_graph([0.2, 0.4, 0.6, 0.8]),
            "Aggregate"
        )
    }
    
    render_hybrid_animation(
        pipeline,
        pregel_factories,
        os.path.join(OUTPUT_DIR, "hybrid_00_unified"),
        max_steps=10,
        frame_delay=80,
        title="Pipeline + Computation"
    )
    
    print("\n" + "-" * 40)
    print("Generated animations:")
    print("  - hybrid_00_unified.gif      (Petri Net + Pregel TOGETHER)")
    print("  - hybrid_01_pipeline.gif     (Petri Net only)")
    print("  - hybrid_02_preprocess.gif   (Pregel only)")
    print("  - hybrid_03_aggregation.gif  (Pregel only)")
    print("-" * 40)


if __name__ == "__main__":
    # Run the hybrid pipeline
    run_hybrid_pipeline()
    
    # Generate animations
    generate_animations()
