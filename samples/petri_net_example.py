"""
PETRI NET EXAMPLES

This file demonstrates the Petri net capabilities of the DaggyD framework:
1. Producer-Consumer with bounded buffer
2. Dining Philosophers (deadlock-free)
3. Mutex with inhibitor arcs
4. Workflow with fork/join parallelism
5. Resource pool management
6. Deadlock detection

Each example includes:
- Petri net construction
- Execution
- Reachability analysis
- Graphviz export (DOT files and rendered images if Graphviz is installed)

Usage:
    python samples/petri_net_example.py

Outputs are saved to: example_outputs/
"""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from DaggyD.petri_net import PetriNet

# Output directory for all generated files
OUTPUT_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "example_outputs")


def ensure_output_dir():
    """Create output directory if it doesn't exist."""
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    print(f"\033[36m[OUTPUT DIR] {OUTPUT_DIR}\033[0m\n")


def save_outputs(net, name, title=None):
    """Save all outputs for a Petri net example."""
    base_path = os.path.join(OUTPUT_DIR, name)
    title = title or name.replace("_", " ").title()
    
    # Save DOT files
    net.save_graphviz(f"{base_path}_structure.dot", title=f"{title} - Structure")
    net.save_graphviz_reachability(f"{base_path}_reachability.dot", title=f"{title} - Reachability")
    
    # Try to render images (will skip if Graphviz not installed)
    net.render_graphviz(f"{base_path}_structure", format="png", title=f"{title} - Structure")
    net.render_graphviz_reachability(f"{base_path}_reachability", format="png", title=f"{title} - Reachability")
    
    # Also try SVG for better quality
    net.render_graphviz(f"{base_path}_structure", format="svg", title=f"{title} - Structure")
    net.render_graphviz_reachability(f"{base_path}_reachability", format="svg", title=f"{title} - Reachability")
    
    # Save analysis report
    report = net.analyze_reachability()
    with open(f"{base_path}_analysis.txt", "w") as f:
        f.write(report)
    print(f"\033[32m[EXPORT] Saved analysis to {base_path}_analysis.txt\033[0m")
    
    # Save JSON
    with open(f"{base_path}_model.json", "w") as f:
        f.write(net.to_json(indent=2))
    print(f"\033[32m[EXPORT] Saved model to {base_path}_model.json\033[0m")


def example_producer_consumer():
    """
    Classic producer-consumer with bounded buffer.
    
    Places:
    - buffer: holds produced items (capacity limited)
    - empty_slots: tracks available buffer space
    
    Transitions:
    - produce: creates item, uses slot
    - consume: removes item, frees slot
    """
    print("\n" + "=" * 70)
    print("  EXAMPLE 1: PRODUCER-CONSUMER WITH BOUNDED BUFFER")
    print("=" * 70 + "\n")
    
    net = PetriNet(debug=True)
    
    # Places
    BUFFER_SIZE = 3
    net.add_place("buffer", capacity=BUFFER_SIZE, initial_tokens=0)
    net.add_place("empty_slots", capacity=BUFFER_SIZE, initial_tokens=BUFFER_SIZE)
    
    # Transitions
    net.add_transition(
        "produce",
        consume_from=[("empty_slots", 1)],
        produce_to=[("buffer", 1)],
        action=lambda inputs: print("    [Action] Produced item")
    )
    
    net.add_transition(
        "consume",
        consume_from=[("buffer", 1)],
        produce_to=[("empty_slots", 1)],
        action=lambda inputs: print("    [Action] Consumed item")
    )
    
    print("\nRunning for 6 steps:")
    net.run(max_steps=6)
    
    print("\n" + net.analyze_reachability())
    
    # Save outputs
    net.reset()
    save_outputs(net, "01_producer_consumer", "Producer-Consumer")
    
    return net


def example_dining_philosophers():
    """
    Dining Philosophers - deadlock-free version.
    
    Two philosophers share two forks. Each needs both forks to eat.
    This version acquires both forks atomically, preventing deadlock.
    """
    print("\n" + "=" * 70)
    print("  EXAMPLE 2: DINING PHILOSOPHERS (DEADLOCK-FREE)")
    print("=" * 70 + "\n")
    
    net = PetriNet(debug=True)
    
    # Places
    net.add_place("fork1", initial_tokens=1)
    net.add_place("fork2", initial_tokens=1)
    net.add_place("phil1_thinking", initial_tokens=1)
    net.add_place("phil1_eating", initial_tokens=0)
    net.add_place("phil2_thinking", initial_tokens=1)
    net.add_place("phil2_eating", initial_tokens=0)
    
    # Philosopher 1
    net.add_transition(
        "phil1_pickup",
        consume_from=[("phil1_thinking", 1), ("fork1", 1), ("fork2", 1)],
        produce_to=[("phil1_eating", 1)],
        action=lambda _: print("    [Phil1] Started eating")
    )
    
    net.add_transition(
        "phil1_putdown",
        consume_from=[("phil1_eating", 1)],
        produce_to=[("phil1_thinking", 1), ("fork1", 1), ("fork2", 1)],
        action=lambda _: print("    [Phil1] Finished eating, thinking")
    )
    
    # Philosopher 2
    net.add_transition(
        "phil2_pickup",
        consume_from=[("phil2_thinking", 1), ("fork1", 1), ("fork2", 1)],
        produce_to=[("phil2_eating", 1)],
        action=lambda _: print("    [Phil2] Started eating")
    )
    
    net.add_transition(
        "phil2_putdown",
        consume_from=[("phil2_eating", 1)],
        produce_to=[("phil2_thinking", 1), ("fork1", 1), ("fork2", 1)],
        action=lambda _: print("    [Phil2] Finished eating, thinking")
    )
    
    print("\nRunning for 8 steps:")
    net.run(max_steps=8)
    
    print("\n" + net.analyze_reachability())
    
    # Save outputs
    net.reset()
    save_outputs(net, "02_dining_philosophers", "Dining Philosophers")
    
    return net


def example_mutex_inhibitor():
    """
    Mutual exclusion using inhibitor arcs.
    
    Two processes compete for a resource. Inhibitor arcs ensure
    a process can only acquire when the other is not using it.
    """
    print("\n" + "=" * 70)
    print("  EXAMPLE 3: MUTEX WITH INHIBITOR ARCS")
    print("=" * 70 + "\n")
    
    net = PetriNet(debug=True)
    
    # Places
    net.add_place("resource", initial_tokens=1)
    net.add_place("p1_idle", initial_tokens=1)
    net.add_place("p1_using", initial_tokens=0)
    net.add_place("p2_idle", initial_tokens=1)
    net.add_place("p2_using", initial_tokens=0)
    
    # Process 1 - inhibited by Process 2 using
    net.add_transition(
        "p1_acquire",
        consume_from=[("p1_idle", 1), ("resource", 1)],
        produce_to=[("p1_using", 1)],
        inhibitor_from=["p2_using"],  # Can't acquire if P2 is using
        action=lambda _: print("    [P1] Acquired resource")
    )
    
    net.add_transition(
        "p1_release",
        consume_from=[("p1_using", 1)],
        produce_to=[("p1_idle", 1), ("resource", 1)],
        action=lambda _: print("    [P1] Released resource")
    )
    
    # Process 2 - inhibited by Process 1 using
    net.add_transition(
        "p2_acquire",
        consume_from=[("p2_idle", 1), ("resource", 1)],
        produce_to=[("p2_using", 1)],
        inhibitor_from=["p1_using"],  # Can't acquire if P1 is using
        action=lambda _: print("    [P2] Acquired resource")
    )
    
    net.add_transition(
        "p2_release",
        consume_from=[("p2_using", 1)],
        produce_to=[("p2_idle", 1), ("resource", 1)],
        action=lambda _: print("    [P2] Released resource")
    )
    
    print("\nRunning for 10 steps:")
    net.run(max_steps=10)
    
    print("\n" + net.analyze_reachability())
    
    # Save outputs
    net.reset()
    save_outputs(net, "03_mutex_inhibitor", "Mutex with Inhibitor Arcs")
    
    return net


def example_workflow_fork_join():
    """
    Workflow with parallel branches (fork/join pattern).
    
    A task splits into two parallel branches, then joins back.
    """
    print("\n" + "=" * 70)
    print("  EXAMPLE 4: WORKFLOW WITH FORK/JOIN PARALLELISM")
    print("=" * 70 + "\n")
    
    net = PetriNet(debug=True)
    
    # Places
    net.add_place("start", initial_tokens=1)
    net.add_place("branch_a", initial_tokens=0)
    net.add_place("branch_b", initial_tokens=0)
    net.add_place("done_a", initial_tokens=0)
    net.add_place("done_b", initial_tokens=0)
    net.add_place("end", initial_tokens=0)
    
    # Fork: split into two branches
    net.add_transition(
        "fork",
        consume_from=[("start", 1)],
        produce_to=[("branch_a", 1), ("branch_b", 1)],
        action=lambda _: print("    [Fork] Split into parallel branches")
    )
    
    # Process branches
    net.add_transition(
        "process_a",
        consume_from=[("branch_a", 1)],
        produce_to=[("done_a", 1)],
        action=lambda _: print("    [Branch A] Processing complete")
    )
    
    net.add_transition(
        "process_b",
        consume_from=[("branch_b", 1)],
        produce_to=[("done_b", 1)],
        action=lambda _: print("    [Branch B] Processing complete")
    )
    
    # Join: wait for both branches
    net.add_transition(
        "join",
        consume_from=[("done_a", 1), ("done_b", 1)],
        produce_to=[("end", 1)],
        action=lambda _: print("    [Join] Both branches completed")
    )
    
    print("\nRunning until completion:")
    final_marking = net.run(max_steps=10)
    
    print("\n" + net.analyze_reachability())
    
    # Verify workflow completed
    if final_marking.get("end", 0) == 1:
        print("\n[SUCCESS] Workflow completed successfully!")
    else:
        print("\n[WARNING] Workflow did not reach end state")
    
    # Save outputs
    net.reset()
    save_outputs(net, "04_fork_join_workflow", "Fork-Join Workflow")
    
    return net


def example_resource_pool():
    """
    Resource pool management with multiple consumers.
    
    A pool of 3 resources shared by 5 workers.
    Each worker needs 1 resource to work.
    """
    print("\n" + "=" * 70)
    print("  EXAMPLE 5: RESOURCE POOL (3 RESOURCES, 5 WORKERS)")
    print("=" * 70 + "\n")
    
    net = PetriNet(debug=True)
    
    NUM_RESOURCES = 3
    NUM_WORKERS = 5
    
    # Resource pool
    net.add_place("pool", capacity=NUM_RESOURCES, initial_tokens=NUM_RESOURCES)
    
    # Workers
    for i in range(NUM_WORKERS):
        net.add_place(f"worker{i}_idle", initial_tokens=1)
        net.add_place(f"worker{i}_working", initial_tokens=0)
        
        net.add_transition(
            f"worker{i}_start",
            consume_from=[(f"worker{i}_idle", 1), ("pool", 1)],
            produce_to=[(f"worker{i}_working", 1)],
            action=lambda _, w=i: print(f"    [Worker {w}] Started work")
        )
        
        net.add_transition(
            f"worker{i}_finish",
            consume_from=[(f"worker{i}_working", 1)],
            produce_to=[(f"worker{i}_idle", 1), ("pool", 1)],
            action=lambda _, w=i: print(f"    [Worker {w}] Finished work")
        )
    
    print("\nRunning for 15 steps:")
    net.run(max_steps=15)
    
    print("\n" + net.analyze_reachability())
    
    # Save outputs
    net.reset()
    save_outputs(net, "05_resource_pool", "Resource Pool")
    
    return net


def example_deadlock_detection():
    """
    Demonstrate deadlock detection in reachability analysis.
    
    Creates a net that can reach a deadlock state.
    """
    print("\n" + "=" * 70)
    print("  EXAMPLE 6: DEADLOCK DETECTION")
    print("=" * 70 + "\n")
    
    net = PetriNet(debug=True)
    
    # A simple net that deadlocks
    net.add_place("p1", initial_tokens=1)
    net.add_place("p2", initial_tokens=0)
    net.add_place("p3", initial_tokens=0)
    
    # t1: p1 -> p2
    net.add_transition("t1", consume_from=[("p1", 1)], produce_to=[("p2", 1)])
    
    # t2: p2 -> p3 (one-way, no way back)
    net.add_transition("t2", consume_from=[("p2", 1)], produce_to=[("p3", 1)])
    
    # No transition from p3 -> deadlock!
    
    print("\nRunning until deadlock:")
    net.run(max_steps=10)
    
    print("\n" + net.analyze_reachability())
    
    # Save outputs
    net.reset()
    save_outputs(net, "06_deadlock_detection", "Deadlock Detection")
    
    return net


def example_dining_philosophers_deadlock():
    """
    Dining Philosophers - DEADLOCK VERSION.
    
    Two philosophers share two forks. Each philosopher picks up their
    left fork first, then tries to get their right fork. This creates
    a circular wait that leads to deadlock when both hold their left fork.
    
    The reachability graph will show all possible states and identify
    the deadlock state where both philosophers are stuck waiting.
    
    Deadlock occurs at: {fork1:0, fork2:0, phil1_has_left:1, phil2_has_left:1}
    """
    print("\n" + "=" * 70)
    print("  EXAMPLE 7: DINING PHILOSOPHERS (DEADLOCK VERSION)")
    print("=" * 70 + "\n")
    
    net = PetriNet(debug=True)
    
    # Forks (shared resources)
    net.add_place("fork1", initial_tokens=1)
    net.add_place("fork2", initial_tokens=1)
    
    # Philosopher 1 states
    net.add_place("phil1_thinking", initial_tokens=1)
    net.add_place("phil1_has_left", initial_tokens=0)  # Has fork1 only
    net.add_place("phil1_eating", initial_tokens=0)    # Has both forks
    
    # Philosopher 2 states  
    net.add_place("phil2_thinking", initial_tokens=1)
    net.add_place("phil2_has_left", initial_tokens=0)  # Has fork2 only
    net.add_place("phil2_eating", initial_tokens=0)    # Has both forks
    
    # Philosopher 1 transitions:
    # 1. Pick up left fork (fork1)
    net.add_transition(
        "phil1_take_left",
        consume_from=[("phil1_thinking", 1), ("fork1", 1)],
        produce_to=[("phil1_has_left", 1)],
        action=lambda _: print("    [Phil1] Picked up LEFT fork (fork1)")
    )
    
    # 2. Pick up right fork (fork2) - needs to already have left
    net.add_transition(
        "phil1_take_right",
        consume_from=[("phil1_has_left", 1), ("fork2", 1)],
        produce_to=[("phil1_eating", 1)],
        action=lambda _: print("    [Phil1] Picked up RIGHT fork (fork2) - EATING")
    )
    
    # 3. Release both forks
    net.add_transition(
        "phil1_release",
        consume_from=[("phil1_eating", 1)],
        produce_to=[("phil1_thinking", 1), ("fork1", 1), ("fork2", 1)],
        action=lambda _: print("    [Phil1] Released both forks - thinking")
    )
    
    # Philosopher 2 transitions (picks up fork2 first, then fork1):
    # 1. Pick up left fork (fork2 for phil2)
    net.add_transition(
        "phil2_take_left",
        consume_from=[("phil2_thinking", 1), ("fork2", 1)],
        produce_to=[("phil2_has_left", 1)],
        action=lambda _: print("    [Phil2] Picked up LEFT fork (fork2)")
    )
    
    # 2. Pick up right fork (fork1) - needs to already have left
    net.add_transition(
        "phil2_take_right",
        consume_from=[("phil2_has_left", 1), ("fork1", 1)],
        produce_to=[("phil2_eating", 1)],
        action=lambda _: print("    [Phil2] Picked up RIGHT fork (fork1) - EATING")
    )
    
    # 3. Release both forks
    net.add_transition(
        "phil2_release",
        consume_from=[("phil2_eating", 1)],
        produce_to=[("phil2_thinking", 1), ("fork1", 1), ("fork2", 1)],
        action=lambda _: print("    [Phil2] Released both forks - thinking")
    )
    
    print("This net CAN deadlock when both philosophers hold their left fork!")
    print("The reachability analysis will find all states and identify deadlocks.\n")
    
    # First, analyze reachability from initial state to show ALL possible states
    print("\n[REACHABILITY ANALYSIS FROM INITIAL STATE]")
    print(net.analyze_reachability())
    
    print("\nNow running to demonstrate the deadlock:")
    net.run(max_steps=8)
    
    # Save outputs (reset to initial state for clean export)
    net.reset()
    save_outputs(net, "07_dining_philosophers_deadlock", "Dining Philosophers (Deadlock)")
    
    return net


def example_weighted_arcs():
    """
    Demonstrate weighted arcs (multiple tokens consumed/produced).
    
    A chemical reaction that requires 2 H2 + 1 O2 -> 2 H2O
    """
    print("\n" + "=" * 70)
    print("  EXAMPLE 8: WEIGHTED ARCS (CHEMICAL REACTION)")
    print("=" * 70 + "\n")
    
    net = PetriNet(debug=True)
    
    # Reactants
    net.add_place("H2", initial_tokens=6)   # 6 hydrogen molecules
    net.add_place("O2", initial_tokens=3)   # 3 oxygen molecules
    net.add_place("H2O", initial_tokens=0)  # No water yet
    
    # Reaction: 2 H2 + 1 O2 -> 2 H2O
    net.add_transition(
        "react",
        consume_from=[("H2", 2), ("O2", 1)],  # Consumes 2 H2 and 1 O2
        produce_to=[("H2O", 2)],               # Produces 2 H2O
        action=lambda _: print("    [Reaction] 2 H2 + O2 -> 2 H2O")
    )
    
    print("\nRunning reaction:")
    net.run(max_steps=10)
    
    final = net.get_marking()
    print(f"\nFinal state: {final['H2']} H2, {final['O2']} O2, {final['H2O']} H2O")
    
    print("\n" + net.analyze_reachability())
    
    # Save outputs
    net.reset()
    save_outputs(net, "08_weighted_arcs", "Weighted Arcs (Chemical Reaction)")
    
    return net


def example_priority():
    """
    Demonstrate transition priorities.
    
    High priority transitions fire before low priority ones.
    """
    print("\n" + "=" * 70)
    print("  EXAMPLE 9: TRANSITION PRIORITIES")
    print("=" * 70 + "\n")
    
    net = PetriNet(debug=True)
    
    # Single resource
    net.add_place("resource", initial_tokens=1)
    net.add_place("low_got_it", initial_tokens=0)
    net.add_place("high_got_it", initial_tokens=0)
    
    # Low priority transition
    net.add_transition(
        "low_priority",
        consume_from=[("resource", 1)],
        produce_to=[("low_got_it", 1)],
        priority=1,
        action=lambda _: print("    [LOW] Got the resource")
    )
    
    # High priority transition
    net.add_transition(
        "high_priority",
        consume_from=[("resource", 1)],
        produce_to=[("high_got_it", 1)],
        priority=10,
        action=lambda _: print("    [HIGH] Got the resource")
    )
    
    print("\nBoth transitions want the resource, but high priority wins:")
    net.run(max_steps=1)
    
    final = net.get_marking()
    winner = "HIGH" if final["high_got_it"] == 1 else "LOW"
    print(f"\nWinner: {winner} priority transition")
    
    print("\n" + net.analyze_reachability())
    
    # Save outputs
    net.reset()
    save_outputs(net, "09_priority", "Transition Priorities")
    
    return net


def main():
    print("\n" + "#" * 70)
    print("#" + " " * 68 + "#")
    print("#" + "  PETRI NET EXAMPLES".center(68) + "#")
    print("#" + " " * 68 + "#")
    print("#" * 70)
    
    ensure_output_dir()
    
    examples = [
        ("Producer-Consumer", example_producer_consumer),
        ("Dining Philosophers", example_dining_philosophers),
        ("Mutex with Inhibitors", example_mutex_inhibitor),
        ("Fork/Join Workflow", example_workflow_fork_join),
        ("Resource Pool", example_resource_pool),
        ("Deadlock Detection", example_deadlock_detection),
        ("Dining Philosophers Deadlock", example_dining_philosophers_deadlock),
        ("Weighted Arcs", example_weighted_arcs),
        ("Transition Priorities", example_priority),
    ]
    
    for name, func in examples:
        try:
            func()
        except Exception as e:
            print(f"\n[ERROR] {name} failed: {e}")
            import traceback
            traceback.print_exc()
    
    print("\n" + "#" * 70)
    print("#" + "  ALL EXAMPLES COMPLETED".center(68) + "#")
    print("#" * 70)
    
    print(f"\n\033[36mOutput files saved to: {OUTPUT_DIR}\033[0m")
    print("\nGenerated files:")
    for f in sorted(os.listdir(OUTPUT_DIR)):
        print(f"  - {f}")
    print()


if __name__ == "__main__":
    main()
