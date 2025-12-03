"""
Parallel Graph Examples - Lisp-like Functional Style

This demonstrates parallel execution and iterative graph propagation
using pure functional composition.
"""

import sys
import os
import time
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from DaggyD.pregel_core import (
    pregel, add_channel, add_node, run, reset,
    get_checkpoint, restore_checkpoint,
    save_graphviz, save_graphviz_dataflow,
    render_graphviz, render_graphviz_dataflow
)


def run_parallel_computation_example():
    """Parallel workers processing chunks of data."""
    print("\033[36m=== PREGEL PARALLEL COMPUTATION EXAMPLE ===\033[0m")
    print("This demonstrates parallel execution of independent nodes within supersteps\n")
    
    # Track which workers have processed (closure state)
    worker_done = {"w1": False, "w2": False, "w3": False, "w4": False}
    
    # Build graph functionally
    p = pregel(debug=True, parallel=True, max_workers=4)
    
    # Data source and worker output channels
    p = add_channel(p, "data_source", "LastValue", initial_value=[1, 2, 3, 4, 5, 6, 7, 8, 9, 10])
    p = add_channel(p, "worker_1_out", "LastValue")
    p = add_channel(p, "worker_2_out", "LastValue")
    p = add_channel(p, "worker_3_out", "LastValue")
    p = add_channel(p, "worker_4_out", "LastValue")
    p = add_channel(p, "aggregated_sum", "BinaryOperator", operator=lambda a, b: a + b, initial_value=0)
    
    # Worker functions
    def make_worker(worker_id, chunk_start, chunk_end):
        def process(inputs):
            if worker_done[f"w{worker_id}"]:
                return None
            data = inputs.get("data_source", [])
            chunk = data[chunk_start:chunk_end]
            result = sum(x * 2 for x in chunk)
            print(f"    Worker {worker_id}: Processing chunk {chunk}, result = {result}")
            time.sleep(0.05)
            worker_done[f"w{worker_id}"] = True
            return {"result": chunk, "partial": result}
        return process
    
    # Add worker nodes
    p = add_node(p, "worker_1", make_worker(1, 0, 3),
        subscribe_to=["data_source"],
        write_to={"result": "worker_1_out", "partial": "aggregated_sum"})
    
    p = add_node(p, "worker_2", make_worker(2, 3, 6),
        subscribe_to=["data_source"],
        write_to={"result": "worker_2_out", "partial": "aggregated_sum"})
    
    p = add_node(p, "worker_3", make_worker(3, 6, 8),
        subscribe_to=["data_source"],
        write_to={"result": "worker_3_out", "partial": "aggregated_sum"})
    
    p = add_node(p, "worker_4", make_worker(4, 8, 10),
        subscribe_to=["data_source"],
        write_to={"result": "worker_4_out", "partial": "aggregated_sum"})
    
    print("Configuration:")
    print("  - 4 parallel workers")
    print("  - Each worker processes a chunk of data once")
    print("  - Results are aggregated using BinaryOperator channel\n")
    
    start_time = time.time()
    final_state = run(p, max_supersteps=5)
    elapsed = time.time() - start_time
    
    # Save graphviz outputs
    output_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "example_outputs")
    os.makedirs(output_dir, exist_ok=True)
    base_path = os.path.join(output_dir, "11_parallel_computation")
    
    save_graphviz(p, f"{base_path}_graph.dot", title="Parallel Computation Graph")
    save_graphviz_dataflow(p, f"{base_path}_dataflow.dot", title="Parallel Computation Dataflow")
    render_graphviz(p, f"{base_path}_graph", format="png", title="Parallel Computation Graph")
    render_graphviz(p, f"{base_path}_graph", format="svg", title="Parallel Computation Graph")
    render_graphviz_dataflow(p, f"{base_path}_dataflow", format="png", title="Parallel Computation Dataflow")
    render_graphviz_dataflow(p, f"{base_path}_dataflow", format="svg", title="Parallel Computation Dataflow")
    
    print("\n\033[32m=== RESULTS ===\033[0m")
    print(f"Worker 1 processed: {final_state['worker_1_out']}")
    print(f"Worker 2 processed: {final_state['worker_2_out']}")
    print(f"Worker 3 processed: {final_state['worker_3_out']}")
    print(f"Worker 4 processed: {final_state['worker_4_out']}")
    print(f"Aggregated sum (all elements * 2): {final_state['aggregated_sum']}")
    print(f"Execution time: {elapsed:.3f} seconds")
    
    expected_sum = sum(x * 2 for x in range(1, 11))
    print(f"\nExpected sum: {expected_sum}")
    print(f"Verification: {'PASS' if final_state['aggregated_sum'] == expected_sum else 'FAIL'}")
    
    return p


def run_iterative_graph_propagation():
    """Value propagation through a cyclic graph."""
    print("\n\033[36m=== ITERATIVE GRAPH PROPAGATION EXAMPLE ===\033[0m")
    print("This demonstrates value propagation through a cyclic graph\n")
    
    # Build graph functionally
    p = pregel(debug=True, parallel=True, max_workers=4)
    
    p = add_channel(p, "node_a_val", "LastValue", initial_value=1)
    p = add_channel(p, "node_b_val", "LastValue", initial_value=0)
    p = add_channel(p, "node_c_val", "LastValue", initial_value=0)
    p = add_channel(p, "iteration_count", "BinaryOperator", operator=lambda a, b: a + b, initial_value=0)
    
    # Node A: reads from A and C, writes to B
    def node_a_compute(inputs):
        val_a = inputs.get("node_a_val", 0)
        val_c = inputs.get("node_c_val", 0)
        new_val = val_a + val_c
        if new_val > 100:
            return None
        print(f"    Node A: {val_a} + {val_c} = {new_val}")
        return {"out": new_val, "iter": 1}
    
    # Node B: reads from B, writes to C
    def node_b_compute(inputs):
        val = inputs.get("node_b_val")
        if val is None or val == 0 or val > 100:
            return None
        new_val = val * 2
        print(f"    Node B: {val} * 2 = {new_val}")
        return {"out": new_val}
    
    # Node C: reads from C, writes to A
    def node_c_compute(inputs):
        val = inputs.get("node_c_val")
        if val is None or val == 0 or val > 100:
            return None
        new_val = val + 1
        print(f"    Node C: {val} + 1 = {new_val}")
        return {"out": new_val}
    
    p = add_node(p, "node_a", node_a_compute,
        subscribe_to=["node_a_val", "node_c_val"],
        write_to={"out": "node_b_val", "iter": "iteration_count"})
    
    p = add_node(p, "node_b", node_b_compute,
        subscribe_to=["node_b_val"],
        write_to={"out": "node_c_val"})
    
    p = add_node(p, "node_c", node_c_compute,
        subscribe_to=["node_c_val"],
        write_to={"out": "node_a_val"})
    
    print("Graph structure: A -> B -> C -> A (cyclic)")
    print("  Node A: adds value from C")
    print("  Node B: multiplies by 2")
    print("  Node C: adds 1")
    print("  Terminates when value > 100\n")
    
    final_state = run(p, max_supersteps=20)
    
    # Save graphviz outputs
    output_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "example_outputs")
    os.makedirs(output_dir, exist_ok=True)
    base_path = os.path.join(output_dir, "12_iterative_propagation")
    
    save_graphviz(p, f"{base_path}_graph.dot", title="Iterative Propagation Graph")
    save_graphviz_dataflow(p, f"{base_path}_dataflow.dot", title="Iterative Propagation Dataflow")
    render_graphviz(p, f"{base_path}_graph", format="png", title="Iterative Propagation Graph")
    render_graphviz(p, f"{base_path}_graph", format="svg", title="Iterative Propagation Graph")
    render_graphviz_dataflow(p, f"{base_path}_dataflow", format="png", title="Iterative Propagation Dataflow")
    render_graphviz_dataflow(p, f"{base_path}_dataflow", format="svg", title="Iterative Propagation Dataflow")
    
    print("\n\033[32m=== FINAL STATE ===\033[0m")
    print(f"Node A final value: {final_state['node_a_val']}")
    print(f"Node B final value: {final_state['node_b_val']}")
    print(f"Node C final value: {final_state['node_c_val']}")
    print(f"Total iterations: {final_state['iteration_count']}")
    
    return p


def run_checkpoint_example():
    """Checkpoint and restore Pregel state."""
    print("\n\033[36m=== CHECKPOINT AND RESTORE EXAMPLE ===\033[0m")
    print("This demonstrates checkpointing and restoring Pregel state\n")
    
    # Build graph functionally
    p = pregel(debug=True)
    
    p = add_channel(p, "counter", "LastValue", initial_value=0)
    p = add_channel(p, "output", "Topic")
    
    def increment(inputs):
        val = inputs.get("counter", 0)
        if val < 5:
            return {"next": val + 1, "log": f"Count: {val + 1}"}
        return None
    
    p = add_node(p, "incrementer", increment,
        subscribe_to=["counter"],
        write_to={"next": "counter", "log": "output"})
    
    print("Running for 3 supersteps, then checkpointing...")
    
    # Run for 3 supersteps
    final_state = run(p, max_supersteps=3)
    
    # Get checkpoint
    checkpoint = get_checkpoint(p)
    checkpoint_value = p["channels"]["counter"]["read"]()
    print(f"\nCheckpoint saved at superstep {checkpoint['superstep']}")
    print(f"Counter value at checkpoint: {checkpoint_value}")
    
    print("\nContinuing execution to completion...")
    final_state = run(p, max_supersteps=10)
    
    final_counter = p["channels"]["counter"]["read"]()
    final_output = p["channels"]["output"]["read"]()
    print(f"\nFinal counter value: {final_counter}")
    print(f"Output log: {final_output}")
    
    print("\nRestoring from checkpoint...")
    p = restore_checkpoint(p, checkpoint)
    restored_counter = p["channels"]["counter"]["read"]()
    print(f"Counter value after restore: {restored_counter}")
    print(f"Superstep after restore: {p['superstep']}")
    
    # Verify restore worked
    print(f"\nVerification: {'PASS' if restored_counter == checkpoint_value else 'FAIL'}")


def main():
    run_parallel_computation_example()
    run_iterative_graph_propagation()
    run_checkpoint_example()


if __name__ == "__main__":
    main()
