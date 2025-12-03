# DagBi

[![License: AGPL v3](https://img.shields.io/badge/License-AGPL%20v3-blue.svg)](https://www.gnu.org/licenses/agpl-3.0)
[![Python 3.8+](https://img.shields.io/badge/python-3.8+-blue.svg)](https://www.python.org/downloads/)

> **A functional Python framework for graph processing, workflow orchestration, and concurrent systems modeling.**

DagBi combines three powerful computation models into one cohesive framework:

- **Pregel** - Google's BSP (Bulk Synchronous Parallel) model for graph algorithms
- **Petri Nets** - Token-based concurrent systems with deadlock detection  
- **DaggyD** - DAG execution with dependency resolution

## License

This project is licensed under the **GNU Affero General Public License v3.0 (AGPL-3.0)**.

This means:
- You can use, modify, and distribute this software
- If you modify it, you must release your modifications under AGPL-3.0
- If you run a modified version on a server, you must make the source available to users
- See [LICENSE](LICENSE) for full terms

---

## Quick Start

### Installation

```bash
git clone https://github.com/yourusername/DagBi.git
cd DagBi
pip install -e .

# For visualization (optional)
brew install graphviz imagemagick  # macOS
# apt install graphviz imagemagick  # Ubuntu
```

### Example 1: Counter (Pregel)

```python
from DagBi.pregel_core import pregel, add_channel, add_node, run

# Create graph with a counter channel
p = pregel()
p = add_channel(p, "count", "LastValue", initial_value=0)

# Node increments until 5
def increment(inputs):
    val = inputs.get("count", 0)
    return {"out": val + 1} if val < 5 else None

p = add_node(p, "counter", increment,
    subscribe_to=["count"],
    write_to={"out": "count"})

result = run(p)
print(result["count"])  # Output: 5
```

### Example 2: Producer-Consumer (Petri Net)

```python
from DagBi.petri_net import PetriNet

net = PetriNet()
net.add_place("buffer", capacity=3, initial_tokens=0)
net.add_place("empty", capacity=3, initial_tokens=3)

net.add_transition("produce",
    consume_from=[("empty", 1)],
    produce_to=[("buffer", 1)])

net.add_transition("consume",
    consume_from=[("buffer", 1)],
    produce_to=[("empty", 1)])

result = net.run(max_steps=10)
print(result)  # Token distribution
```

### Example 3: Deadlock Detection

```python
from DagBi.petri_net import PetriNet

# Dining philosophers that CAN deadlock
net = PetriNet()

# Forks and philosopher states
net.add_place("fork1", initial_tokens=1)
net.add_place("fork2", initial_tokens=1)
net.add_place("phil1_thinking", initial_tokens=1)
net.add_place("phil1_has_fork", initial_tokens=0)
net.add_place("phil2_thinking", initial_tokens=1)
net.add_place("phil2_has_fork", initial_tokens=0)

# Phil1: take left fork first
net.add_transition("phil1_take_left",
    consume_from=[("phil1_thinking", 1), ("fork1", 1)],
    produce_to=[("phil1_has_fork", 1)])

# Phil2: take left fork first (fork2)
net.add_transition("phil2_take_left",
    consume_from=[("phil2_thinking", 1), ("fork2", 1)],
    produce_to=[("phil2_has_fork", 1)])

# Analyze for deadlocks
print(net.analyze_reachability())
# Output: Deadlock-free: No, Deadlock states: 1
```

---

## Core API

### Pregel (Graph Processing)

The Pregel engine implements BSP (Bulk Synchronous Parallel) computation:

```
Superstep N: [Plan] -> [Execute Nodes] -> [Update Channels] -> Superstep N+1
```

#### Functions

| Function | Description |
|----------|-------------|
| `pregel(debug=False, parallel=False)` | Create a new Pregel graph |
| `add_channel(p, name, type, **kwargs)` | Add a communication channel |
| `add_node(p, name, func, subscribe_to, write_to)` | Add a compute node |
| `run(p, max_supersteps=1000)` | Run to completion |
| `run_step(p)` | Execute one superstep |
| `reset(p)` | Reset to initial state |

#### Channel Types

| Type | Behavior | Use Case |
|------|----------|----------|
| `LastValue` | Stores most recent value | Vertex state |
| `Topic` | Accumulates all values as list | Message history |
| `BinaryOperator` | Aggregates with custom operator | Global reductions (sum, max) |
| `Accumulator` | Like BinaryOperator but resets each step | Per-superstep inboxes |
| `Ephemeral` | Clears after one superstep | One-time triggers |

#### Example: PageRank

```python
from DagBi.pregel_core import pregel, add_channel, add_node, run

GRAPH = {"A": ["B", "C"], "B": ["C"], "C": ["A"]}
DAMPING = 0.85

p = pregel()

# Create channels for each vertex
for v in GRAPH:
    p = add_channel(p, f"rank_{v}", "LastValue", 
        initial_value=1.0/len(GRAPH))
    p = add_channel(p, f"inbox_{v}", "Accumulator",
        operator=lambda a, b: a + b, initial_value=0.0)

# Create compute nodes
for v, neighbors in GRAPH.items():
    def make_compute(vid, neighs, degree):
        def compute(inputs):
            rank = inputs.get(f"rank_{vid}", 0)
            inbox = inputs.get(f"inbox_{vid}", 0)
            
            # PageRank formula
            new_rank = (1-DAMPING)/len(GRAPH) + DAMPING * inbox
            
            # Send contribution to neighbors
            outputs = {"rank": new_rank}
            for n in neighs:
                outputs[f"to_{n}"] = new_rank / degree
            return outputs
        return compute
    
    write_to = {"rank": f"rank_{v}"}
    for n in neighbors:
        write_to[f"to_{n}"] = f"inbox_{n}"
    
    p = add_node(p, f"node_{v}",
        make_compute(v, neighbors, len(neighbors)),
        subscribe_to=[f"rank_{v}", f"inbox_{v}"],
        write_to=write_to)

result = run(p, max_supersteps=20)
for v in GRAPH:
    print(f"{v}: {result[f'rank_{v}']:.4f}")
```

---

### Petri Nets (Concurrent Systems)

Petri nets model concurrent systems with places (hold tokens) and transitions (move tokens).

```
    [Place]  ---->  |Transition|  ---->  [Place]
    (tokens)         (fires)            (receives)
```

#### Functions

| Function | Description |
|----------|-------------|
| `petri_net(debug=False)` | Create a new Petri net |
| `add_place(net, name, capacity, initial_tokens)` | Add a place |
| `add_transition(net, name, consume_from, produce_to, inhibitor_from, priority)` | Add a transition |
| `run(net, max_steps=1000)` | Run to completion |
| `get_marking(net)` | Get current token distribution |
| `is_enabled(net, transition)` | Check if transition can fire |
| `analyze_reachability(net)` | Generate deadlock analysis report |

#### OOP API

```python
from DagBi.petri_net import PetriNet

net = PetriNet(debug=True)
net.add_place("p1", capacity=5, initial_tokens=3)
net.add_transition("t1", 
    consume_from=[("p1", 1)],
    produce_to=[("p2", 1)],
    inhibitor_from=["blocked"],  # Won't fire if "blocked" has tokens
    priority=10)                 # Higher priority fires first

net.run(max_steps=100)
print(net.analyze_reachability())
```

#### Transition Options

| Option | Description |
|--------|-------------|
| `consume_from` | List of `(place, weight)` to consume tokens from |
| `produce_to` | List of `(place, weight)` to produce tokens to |
| `inhibitor_from` | List of places that block firing when non-empty |
| `priority` | Higher values fire first when multiple enabled |
| `action` | Optional callback `lambda inputs: ...` |

---

### Visualization

Both engines support Graphviz visualization and animated GIFs.

```python
# Static export
from DagBi.pregel_core import save_graphviz, render_graphviz

save_graphviz(p, "graph.dot")
render_graphviz(p, "graph", format="png")

# Animated GIF
from DagBi.pregel_core import render_animation

render_animation(p, "animation", 
    max_supersteps=20,
    frame_delay=100,
    title="My Algorithm")
```

#### Petri Net Reachability Graph

```python
# Exports reachability graph showing all states and deadlocks
net.save_graphviz_reachability("reachability.dot")
net.render_graphviz_reachability("reachability", format="png")

# Animations show reachability graph when deadlock detected
from DagBi.petri_net import render_animation
render_animation(net, "petri_anim", max_steps=10, title="Workflow")
```

---

## Examples

### Running Examples

```bash
# PageRank
python samples/pagerank_example.py

# Petri net patterns (9 examples including deadlock detection)
python samples/petri_net_example.py

# Generate all animated GIFs
python samples/animated_examples.py

# Hybrid Petri + Pregel workflow
python samples/hybrid_example.py
```

### Example Files

| File | Description |
|------|-------------|
| `samples/petri_net_example.py` | 9 Petri net patterns including deadlock detection |
| `samples/pagerank_example.py` | PageRank algorithm |
| `samples/animated_examples.py` | 20 animated GIF examples |
| `samples/hybrid_example.py` | Combined Petri + Pregel workflows |

### Output Directory

Generated files are saved to `example_outputs/`:
- `*.dot` - Graphviz source files
- `*.png`, `*.svg` - Rendered images
- `*_analysis.txt` - Reachability analysis reports
- `animations/*.gif` - Step-by-step execution animations

---

## Testing

```bash
# Run all tests
python -m pytest tests/ -v

# With coverage
python -m pytest tests/ --cov=DagBi
```

---

## Project Structure

```
DagBi/
├── DagBi/
│   ├── pregel_core.py    # Pregel BSP engine
│   ├── petri_net.py      # Petri net engine
│   ├── channels.py       # Channel implementations
│   ├── hybrid.py         # Hybrid visualization
│   └── main.py           # Original DAG executor
├── samples/              # Example scripts
├── tests/                # Test suite
└── example_outputs/      # Generated visualizations
```

---

## Best Practices

### 1. Use Factory Functions for Closures

```python
# WRONG - captures wrong variable
for v in vertices:
    def compute(inputs):
        return {"out": v}  # Always last vertex!

# CORRECT - factory captures correct value
def make_compute(vertex):
    def compute(inputs):
        return {"out": vertex}
    return compute
```

### 2. Return None to Halt

```python
def compute(inputs):
    if done:
        return None  # Node stops executing
    return {"out": value}
```

### 3. Use Reachability Analysis

```python
report = net.analyze_reachability()
if "Deadlock-free:   No" in report:
    print("WARNING: System can deadlock!")
```

---

## Acknowledgments

- Based on Google's [Pregel paper](https://research.google/pubs/pub36726/)
- Inspired by LangGraph's architecture

---

## License

**GNU Affero General Public License v3.0 (AGPL-3.0)**

Copyright (C) 2024

This program is free software: you can redistribute it and/or modify it under the terms of the GNU Affero General Public License as published by the Free Software Foundation, either version 3 of the License, or (at your option) any later version.

This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU Affero General Public License for more details.

You should have received a copy of the GNU Affero General Public License along with this program. If not, see <https://www.gnu.org/licenses/>.
