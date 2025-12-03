"""
PageRank - Lisp-like Functional Style

This implements PageRank using pure functional composition with the Pregel BSP model.

Architecture:
- Each vertex has a `rank_X` channel (LastValue) for its current rank
- Each vertex has an `inbox_X` channel (Accumulator with sum) for incoming contributions
- Accumulator resets after each superstep, so vertices only see NEW contributions

Graph structure (6 vertices):
    A -> B, C       (A has 3 incoming: C, E, F)
    B -> C, D       (B has 1 incoming: A)
    C -> A, D       (C has 2 incoming: A, B)
    D -> E          (D has 2 incoming: B, C)
    E -> A, F       (E has 1 incoming: D)
    F -> A          (F has 1 incoming: E)

Usage:
    python samples/pagerank_example.py
"""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from DagBi.pregel_core import (
    pregel, add_channel, add_node, run,
    save_graphviz, save_graphviz_dataflow,
    render_graphviz, render_graphviz_dataflow
)


# Graph structure
GRAPH = {
    "A": ["B", "C"],
    "B": ["C", "D"],
    "C": ["A", "D"],
    "D": ["E"],
    "E": ["A", "F"],
    "F": ["A"],
}

NUM_VERTICES = len(GRAPH)
DAMPING = 0.85
INITIAL_RANK = 1.0 / NUM_VERTICES
MAX_ITERATIONS = 30
CONVERGENCE_THRESHOLD = 0.0001


def make_vertex_compute(vertex_id, out_neighbors, debug=True):
    """
    Create a PageRank vertex compute function.
    
    Each vertex:
    - Reads its rank channel (for self-state) and inbox channel (for incoming contributions)
    - Computes new rank from incoming contributions
    - Sends contributions to neighbors' inboxes
    - Writes back to its own rank channel
    - Returns None when converged (vote to halt)
    """
    out_degree = len(out_neighbors)
    
    def compute(inputs):
        # Read current state
        state = inputs.get(f"rank_{vertex_id}", {})
        current_rank = state.get("rank", INITIAL_RANK)
        iteration = state.get("iteration", 0)
        
        # Read inbox (sum of contributions from neighbors)
        inbox = inputs.get(f"inbox_{vertex_id}") or 0.0
        
        # Stop after max iterations - vote to halt
        if iteration >= MAX_ITERATIONS:
            if debug:
                print(f"\033[33m[{vertex_id}] iter={iteration} DONE (max iterations reached)\033[0m")
            return None
        
        # Compute new rank from inbox (contributions from neighbors)
        if iteration == 0:
            # First iteration: use initial rank
            new_rank = current_rank
        else:
            # PageRank formula: (1-d)/N + d * sum(contributions)
            new_rank = (1 - DAMPING) / NUM_VERTICES + DAMPING * inbox
        
        # Check convergence (for reporting only)
        change = abs(new_rank - current_rank)
        is_converged = change < CONVERGENCE_THRESHOLD and iteration > 0
        
        if debug:
            status = "CONVERGED" if is_converged else ""
            print(f"\033[33m[{vertex_id}] iter={iteration} inbox={inbox:.4f} rank={current_rank:.4f}->{new_rank:.4f} change={change:.6f} {status}\033[0m")
        
        # Compute contribution to send to each neighbor
        contribution = new_rank / out_degree
        
        # Build outputs - always send messages and update state
        outputs = {
            # Update own rank state
            "rank": {"rank": new_rank, "iteration": iteration + 1, "converged": is_converged}
        }
        
        # Send contribution to each neighbor's inbox
        for neighbor in out_neighbors:
            outputs[f"to_{neighbor}"] = contribution
        
        return outputs
    
    return compute


def create_pagerank_graph(debug=True, parallel=True):
    """Create PageRank graph using functional composition."""
    p = pregel(debug=debug, parallel=parallel, max_workers=NUM_VERTICES)
    
    # Create channels for each vertex
    for vertex in GRAPH:
        # Rank channel: stores current rank state (persistent)
        p = add_channel(p, f"rank_{vertex}", "LastValue",
            initial_value={"rank": INITIAL_RANK, "iteration": 0, "converged": False})
        
        # Inbox channel: accumulates incoming contributions, resets each superstep
        p = add_channel(p, f"inbox_{vertex}", "Accumulator",
            operator=lambda a, b: a + b, initial_value=0.0)
    
    # Create vertex compute functions
    for vertex, out_neighbors in GRAPH.items():
        # Build write_to mapping
        write_to = {"rank": f"rank_{vertex}"}
        for neighbor in out_neighbors:
            write_to[f"to_{neighbor}"] = f"inbox_{neighbor}"
        
        p = add_node(p, f"vertex_{vertex}",
            make_vertex_compute(vertex, out_neighbors, debug=debug),
            subscribe_to=[f"rank_{vertex}", f"inbox_{vertex}"],
            write_to=write_to)
    
    return p


def run_pagerank(debug=True, parallel=True, max_supersteps=100, save_outputs=True):
    """Run PageRank algorithm."""
    print("\033[36m" + "=" * 60 + "\033[0m")
    print("\033[36m    PAGERANK - LISP-LIKE FUNCTIONAL STYLE\033[0m")
    print("\033[36m" + "=" * 60 + "\033[0m\n")
    
    print("Graph structure:")
    for v, edges in GRAPH.items():
        print(f"  {v} -> {', '.join(edges)}")
    print()
    
    # Create graph using functional composition
    p = create_pagerank_graph(debug=debug, parallel=parallel)
    
    # Run the computation
    final_state = run(p, max_supersteps=max_supersteps)
    
    # Save outputs if requested
    if save_outputs:
        output_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "example_outputs")
        os.makedirs(output_dir, exist_ok=True)
        base_path = os.path.join(output_dir, "09_pagerank")
        
        # Save Graphviz DOT files and render
        save_graphviz(p, f"{base_path}_graph.dot", title="PageRank Graph")
        save_graphviz_dataflow(p, f"{base_path}_dataflow.dot", title="PageRank Dataflow")
        render_graphviz(p, f"{base_path}_graph", format="png", title="PageRank Graph")
        render_graphviz(p, f"{base_path}_graph", format="svg", title="PageRank Graph")
        render_graphviz_dataflow(p, f"{base_path}_dataflow", format="png", title="PageRank Dataflow")
        render_graphviz_dataflow(p, f"{base_path}_dataflow", format="svg", title="PageRank Dataflow")
    
    # Extract final ranks
    ranks = {}
    for vertex in GRAPH:
        state = final_state.get(f"rank_{vertex}", {})
        ranks[vertex] = state.get("rank", 0) if isinstance(state, dict) else 0
    
    # Check if all converged
    all_converged = all(
        final_state.get(f"rank_{vertex}", {}).get("converged", False) 
        if isinstance(final_state.get(f"rank_{vertex}"), dict) else False
        for vertex in GRAPH
    )
    
    # Normalize
    total = sum(ranks.values())
    if total > 0:
        normalized = {v: r/total for v, r in ranks.items()}
    else:
        normalized = ranks
    
    print("\n\033[36m" + "=" * 60 + "\033[0m")
    print("\033[32m    RESULTS\033[0m")
    print(f"\033[35m    Supersteps: {p['superstep']}\033[0m")
    print(f"\033[35m    All converged: {all_converged}\033[0m")
    print("\033[36m" + "=" * 60 + "\033[0m")
    
    print("\nFinal PageRanks (normalized):")
    for v in sorted(normalized, key=lambda x: normalized[x], reverse=True):
        bar = "#" * int(normalized[v] * 50)
        print(f"  {v}: {normalized[v]:.4f} {bar}")
    
    return normalized, p['superstep'], all_converged


def main():
    print("\n" + "=" * 70)
    print("  PAGERANK TEST")
    print("=" * 70 + "\n")
    
    ranks, supersteps, converged = run_pagerank(debug=True, parallel=True, max_supersteps=50)
    
    # Verify
    print("\n\033[36m--- VERIFICATION ---\033[0m")
    
    total = sum(ranks.values())
    print(f"[{'OK' if 0.99 < total < 1.01 else 'FAIL'}] Ranks sum to {total:.4f}")
    
    max_v = max(ranks, key=lambda x: ranks[x])
    print(f"[{'OK' if max_v == 'A' else 'FAIL'}] Highest rank: {max_v} ({ranks[max_v]:.4f})")
    
    print(f"[{'OK' if converged else 'FAIL'}] Converged: {converged}")
    
    if converged and max_v == "A" and 0.99 < total < 1.01:
        print("\n\033[32m[ALL CHECKS PASSED]\033[0m")
    else:
        print("\n\033[31m[SOME CHECKS FAILED]\033[0m")


if __name__ == "__main__":
    main()
