"""
Maximum Value Example - Lisp-like Functional Style

This implements the Maximum Value algorithm from the original Pregel paper
using pure functional composition.

Each vertex propagates the maximum value it has seen to its neighbors.
"""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from DaggyD.pregel_core import (
    pregel, add_channel, add_node, run,
    save_graphviz, save_graphviz_dataflow,
    render_graphviz, render_graphviz_dataflow
)


def make_node_compute(node_id, vertex_channel, message_channel, out_channels):
    """Create a compute function for a node."""
    def compute(inputs):
        current_value = inputs.get(vertex_channel, 0)
        incoming_messages = inputs.get(message_channel, [])
        
        if incoming_messages:
            max_received = max(incoming_messages)
            if max_received > current_value:
                print(f"    Node {node_id}: Updating value from {current_value} to {max_received}")
                return {ch: max_received for ch in out_channels}
        else:
            print(f"    Node {node_id}: Initial value {current_value}")
            return {ch: current_value for ch in out_channels}
    
    return compute


def run_maximum_value_example():
    print("\033[36m=== PREGEL MAXIMUM VALUE EXAMPLE ===\033[0m")
    print("This implements the Maximum Value algorithm from the original Pregel paper")
    print("Each vertex propagates the maximum value it has seen to its neighbors\n")
    
    # Build the graph using functional composition
    # Graph structure:
    #   1 -- 2
    #   |    |
    #   3 -- 4
    
    # Create channels - functional pipeline style
    p = pregel(debug=True)
    
    # Vertex value channels
    p = add_channel(p, "vertex_1", "LastValue", initial_value=3)
    p = add_channel(p, "vertex_2", "LastValue", initial_value=6)
    p = add_channel(p, "vertex_3", "LastValue", initial_value=2)
    p = add_channel(p, "vertex_4", "LastValue", initial_value=1)
    
    # Message channels (Topic for collecting messages)
    p = add_channel(p, "messages_1", "Topic")
    p = add_channel(p, "messages_2", "Topic")
    p = add_channel(p, "messages_3", "Topic")
    p = add_channel(p, "messages_4", "Topic")
    
    # Global max aggregator
    p = add_channel(p, "global_max", "BinaryOperator", operator=max)
    
    # Add nodes with their compute functions
    # Node 1: sends to 2, 3
    p = add_node(p, "node_1",
        make_node_compute(1, "vertex_1", "messages_1", ["msg_2", "msg_3", "gmax"]),
        subscribe_to=["vertex_1", "messages_1"],
        write_to={"msg_2": "messages_2", "msg_3": "messages_3", "gmax": "global_max"}
    )
    
    # Node 2: sends to 1, 4
    p = add_node(p, "node_2",
        make_node_compute(2, "vertex_2", "messages_2", ["msg_1", "msg_4", "gmax"]),
        subscribe_to=["vertex_2", "messages_2"],
        write_to={"msg_1": "messages_1", "msg_4": "messages_4", "gmax": "global_max"}
    )
    
    # Node 3: sends to 1, 4
    p = add_node(p, "node_3",
        make_node_compute(3, "vertex_3", "messages_3", ["msg_1", "msg_4", "gmax"]),
        subscribe_to=["vertex_3", "messages_3"],
        write_to={"msg_1": "messages_1", "msg_4": "messages_4", "gmax": "global_max"}
    )
    
    # Node 4: sends to 2, 3
    p = add_node(p, "node_4",
        make_node_compute(4, "vertex_4", "messages_4", ["msg_2", "msg_3", "gmax"]),
        subscribe_to=["vertex_4", "messages_4"],
        write_to={"msg_2": "messages_2", "msg_3": "messages_3", "gmax": "global_max"}
    )
    
    print("\nGraph structure:")
    print("  1 -- 2")
    print("  |    |")
    print("  3 -- 4")
    print("\nInitial values: V1=3, V2=6, V3=2, V4=1")
    print("Expected result: All vertices should converge to 6 (the maximum)\n")
    
    # Run the computation
    final_state = run(p, max_supersteps=10)
    
    # Save graphviz outputs
    output_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "example_outputs")
    os.makedirs(output_dir, exist_ok=True)
    base_path = os.path.join(output_dir, "10_max_value")
    
    save_graphviz(p, f"{base_path}_graph.dot", title="Maximum Value Graph")
    save_graphviz_dataflow(p, f"{base_path}_dataflow.dot", title="Maximum Value Dataflow")
    render_graphviz(p, f"{base_path}_graph", format="png", title="Maximum Value Graph")
    render_graphviz(p, f"{base_path}_graph", format="svg", title="Maximum Value Graph")
    render_graphviz_dataflow(p, f"{base_path}_dataflow", format="png", title="Maximum Value Dataflow")
    render_graphviz_dataflow(p, f"{base_path}_dataflow", format="svg", title="Maximum Value Dataflow")
    
    print("\n\033[32m=== RESULTS ===\033[0m")
    print(f"Global maximum found: {final_state.get('global_max')}")
    print(f"Final message states:")
    for i in range(1, 5):
        msg_channel = f"messages_{i}"
        if msg_channel in final_state:
            print(f"  {msg_channel}: {final_state[msg_channel]}")
    
    # Verify
    expected = 6
    actual = final_state.get('global_max')
    print(f"\nVerification: {'PASS' if actual == expected else 'FAIL'} (expected {expected}, got {actual})")


if __name__ == "__main__":
    run_maximum_value_example()
