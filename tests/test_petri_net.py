"""
Unit tests for Petri Net functionality.
"""

import unittest
import sys
import os
import tempfile
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from DaggyD.channels import create_place_channel
from DaggyD.petri_net import PetriNet


class TestPlaceChannel(unittest.TestCase):
    """Tests for the Place channel type."""
    
    def test_place_initial_tokens(self):
        """Test place initialization with tokens."""
        place = create_place_channel(initial_tokens=5)
        self.assertEqual(place["read"](), 5)
    
    def test_place_capacity(self):
        """Test place capacity limits."""
        place = create_place_channel(capacity=3, initial_tokens=2)
        self.assertEqual(place["get_capacity"](), 3)
        self.assertTrue(place["can_produce"](1))
        self.assertFalse(place["can_produce"](2))
    
    def test_place_write_and_checkpoint(self):
        """Test adding tokens and checkpoint."""
        place = create_place_channel(initial_tokens=0)
        place["write"](3)
        self.assertEqual(place["read"](), 0)  # Not yet committed
        place["checkpoint"]()
        self.assertEqual(place["read"](), 3)  # Now committed
    
    def test_place_consume(self):
        """Test consuming tokens."""
        place = create_place_channel(initial_tokens=5)
        self.assertTrue(place["can_consume"](3))
        self.assertFalse(place["can_consume"](6))
        
        place["consume"](3)
        place["checkpoint"]()
        self.assertEqual(place["read"](), 2)
    
    def test_place_overflow_error(self):
        """Test that overflow raises error."""
        place = create_place_channel(capacity=3, initial_tokens=2)
        place["write"](5)  # Would result in 7 tokens
        with self.assertRaises(RuntimeError):
            place["checkpoint"]()
    
    def test_place_underflow_error(self):
        """Test that underflow raises error."""
        place = create_place_channel(initial_tokens=2)
        place["consume"](5)  # Would result in -3 tokens
        with self.assertRaises(RuntimeError):
            place["checkpoint"]()
    
    def test_place_state_management(self):
        """Test get_state and set_state."""
        place = create_place_channel(capacity=10, initial_tokens=5)
        state = place["get_state"]()
        
        self.assertEqual(state["tokens"], 5)
        self.assertEqual(state["capacity"], 10)
        
        place2 = create_place_channel()
        place2["set_state"](state)
        self.assertEqual(place2["read"](), 5)
    
    def test_place_clear(self):
        """Test clearing place to initial state."""
        place = create_place_channel(initial_tokens=3)
        place["write"](5)
        place["checkpoint"]()
        self.assertEqual(place["read"](), 8)
        
        place["clear"]()
        self.assertEqual(place["read"](), 3)  # Back to initial
    
    def test_place_type(self):
        """Test place type field."""
        place = create_place_channel()
        self.assertEqual(place["type"], "Place")


class TestPetriNetBasic(unittest.TestCase):
    """Basic Petri net functionality tests."""
    
    def test_add_place(self):
        """Test adding places to net."""
        net = PetriNet(debug=False)
        net.add_place("p1", initial_tokens=5)
        net.add_place("p2", capacity=10)
        
        marking = net.get_marking()
        self.assertEqual(marking["p1"], 5)
        self.assertEqual(marking["p2"], 0)
    
    def test_add_transition(self):
        """Test adding transitions."""
        net = PetriNet(debug=False)
        net.add_place("p1", initial_tokens=1)
        net.add_place("p2")
        
        net.add_transition(
            "t1",
            consume_from=[("p1", 1)],
            produce_to=[("p2", 1)]
        )
        
        self.assertIn("t1", net.transitions)
    
    def test_duplicate_place_error(self):
        """Test that duplicate place names raise error."""
        net = PetriNet(debug=False)
        net.add_place("p1")
        with self.assertRaises(ValueError):
            net.add_place("p1")
    
    def test_duplicate_transition_error(self):
        """Test that duplicate transition names raise error."""
        net = PetriNet(debug=False)
        net.add_place("p1")
        net.add_transition("t1", consume_from=[("p1", 1)])
        with self.assertRaises(ValueError):
            net.add_transition("t1", consume_from=[("p1", 1)])
    
    def test_invalid_place_in_transition(self):
        """Test that referencing non-existent place raises error."""
        net = PetriNet(debug=False)
        with self.assertRaises(ValueError):
            net.add_transition("t1", consume_from=[("nonexistent", 1)])


class TestPetriNetFiring(unittest.TestCase):
    """Tests for transition firing."""
    
    def test_is_enabled(self):
        """Test transition enablement check."""
        net = PetriNet(debug=False)
        net.add_place("p1", initial_tokens=2)
        net.add_place("p2")
        net.add_transition("t1", consume_from=[("p1", 1)], produce_to=[("p2", 1)])
        net.add_transition("t2", consume_from=[("p1", 3)])  # Needs 3, only has 2
        
        self.assertTrue(net.is_enabled("t1"))
        self.assertFalse(net.is_enabled("t2"))
    
    def test_fire_transition(self):
        """Test firing a transition."""
        net = PetriNet(debug=False)
        net.add_place("p1", initial_tokens=3)
        net.add_place("p2", initial_tokens=0)
        net.add_transition("t1", consume_from=[("p1", 2)], produce_to=[("p2", 1)])
        
        net.fire_transition("t1")
        
        marking = net.get_marking()
        self.assertEqual(marking["p1"], 1)
        self.assertEqual(marking["p2"], 1)
    
    def test_fire_disabled_transition_error(self):
        """Test that firing disabled transition raises error."""
        net = PetriNet(debug=False)
        net.add_place("p1", initial_tokens=0)
        net.add_transition("t1", consume_from=[("p1", 1)])
        
        with self.assertRaises(RuntimeError):
            net.fire_transition("t1")
    
    def test_weighted_arcs(self):
        """Test arc weights (consume/produce multiple tokens)."""
        net = PetriNet(debug=False)
        net.add_place("p1", initial_tokens=6)
        net.add_place("p2", initial_tokens=0)
        net.add_transition("t1", consume_from=[("p1", 3)], produce_to=[("p2", 2)])
        
        net.fire_transition("t1")
        
        marking = net.get_marking()
        self.assertEqual(marking["p1"], 3)  # 6 - 3 = 3
        self.assertEqual(marking["p2"], 2)  # 0 + 2 = 2
    
    def test_action_function(self):
        """Test that action function is called on firing."""
        net = PetriNet(debug=False)
        net.add_place("p1", initial_tokens=1)
        
        action_called = [False]
        def my_action(inputs):
            action_called[0] = True
            return "result"
        
        net.add_transition("t1", consume_from=[("p1", 1)], action=my_action)
        
        result = net.fire_transition("t1")
        self.assertTrue(action_called[0])
        self.assertEqual(result, "result")


class TestPetriNetInhibitor(unittest.TestCase):
    """Tests for inhibitor arcs."""
    
    def test_inhibitor_blocks_when_tokens(self):
        """Test that inhibitor arc blocks firing when place has tokens."""
        net = PetriNet(debug=False)
        net.add_place("p1", initial_tokens=1)
        net.add_place("inhibit", initial_tokens=1)  # Has token
        net.add_place("p2")
        
        net.add_transition(
            "t1",
            consume_from=[("p1", 1)],
            produce_to=[("p2", 1)],
            inhibitor_from=["inhibit"]
        )
        
        # Should be blocked by inhibitor
        self.assertFalse(net.is_enabled("t1"))
    
    def test_inhibitor_allows_when_empty(self):
        """Test that inhibitor arc allows firing when place is empty."""
        net = PetriNet(debug=False)
        net.add_place("p1", initial_tokens=1)
        net.add_place("inhibit", initial_tokens=0)  # Empty
        net.add_place("p2")
        
        net.add_transition(
            "t1",
            consume_from=[("p1", 1)],
            produce_to=[("p2", 1)],
            inhibitor_from=["inhibit"]
        )
        
        # Should be enabled since inhibitor place is empty
        self.assertTrue(net.is_enabled("t1"))


class TestPetriNetRun(unittest.TestCase):
    """Tests for running the Petri net."""
    
    def test_run_to_completion(self):
        """Test running net until no transitions enabled."""
        net = PetriNet(debug=False)
        net.add_place("start", initial_tokens=1)
        net.add_place("end", initial_tokens=0)
        net.add_transition("t1", consume_from=[("start", 1)], produce_to=[("end", 1)])
        
        final = net.run(max_steps=10)
        
        self.assertEqual(final["start"], 0)
        self.assertEqual(final["end"], 1)
    
    def test_run_max_steps(self):
        """Test that run respects max_steps limit."""
        net = PetriNet(debug=False)
        net.add_place("p1", initial_tokens=100)
        net.add_transition("t1", consume_from=[("p1", 1)], produce_to=[("p1", 1)])  # Loop
        
        final = net.run(max_steps=5)
        
        self.assertEqual(net.step, 5)
    
    def test_reset(self):
        """Test resetting net to initial state."""
        net = PetriNet(debug=False)
        net.add_place("p1", initial_tokens=5)
        net.add_place("p2", initial_tokens=0)
        net.add_transition("t1", consume_from=[("p1", 1)], produce_to=[("p2", 1)])
        
        net.run(max_steps=3)
        self.assertNotEqual(net.get_marking(), {"p1": 5, "p2": 0})
        
        net.reset()
        self.assertEqual(net.get_marking(), {"p1": 5, "p2": 0})
        self.assertEqual(net.step, 0)


class TestPetriNetReachability(unittest.TestCase):
    """Tests for reachability analysis."""
    
    def test_reachability_simple(self):
        """Test basic reachability graph computation."""
        net = PetriNet(debug=False)
        net.add_place("p1", initial_tokens=1)
        net.add_place("p2", initial_tokens=0)
        net.add_transition("t1", consume_from=[("p1", 1)], produce_to=[("p2", 1)])
        
        graph = net.compute_reachability_graph()
        
        self.assertEqual(graph["num_markings"], 2)
        self.assertEqual(graph["num_edges"], 1)
        self.assertTrue(graph["is_complete"])
    
    def test_reachability_detects_deadlock(self):
        """Test that deadlock states are detected."""
        net = PetriNet(debug=False)
        net.add_place("p1", initial_tokens=1)
        net.add_place("p2")
        net.add_transition("t1", consume_from=[("p1", 1)], produce_to=[("p2", 1)])
        # No transition from p2 - deadlock
        
        graph = net.compute_reachability_graph()
        
        self.assertEqual(len(graph["deadlock_markings"]), 1)
    
    def test_reachability_bounded(self):
        """Test boundedness detection."""
        net = PetriNet(debug=False)
        net.add_place("p1", capacity=3, initial_tokens=1)
        net.add_transition("t1", consume_from=[("p1", 1)], produce_to=[("p1", 1)])
        
        graph = net.compute_reachability_graph()
        
        self.assertTrue(graph["is_bounded"])
    
    def test_analyze_reachability_report(self):
        """Test that analysis generates a report."""
        net = PetriNet(debug=False)
        net.add_place("p1", initial_tokens=1)
        net.add_transition("t1", consume_from=[("p1", 1)])
        
        report = net.analyze_reachability()
        
        self.assertIn("PETRI NET REACHABILITY ANALYSIS", report)
        self.assertIn("Places:", report)
        self.assertIn("Transitions:", report)


class TestPetriNetGraphviz(unittest.TestCase):
    """Tests for Graphviz export."""
    
    def test_to_graphviz(self):
        """Test Graphviz DOT export."""
        net = PetriNet(debug=False)
        net.add_place("p1", initial_tokens=2)
        net.add_place("p2")
        net.add_transition("t1", consume_from=[("p1", 1)], produce_to=[("p2", 1)])
        
        dot = net.to_graphviz()
        
        self.assertIn("digraph PetriNet", dot)
        self.assertIn('"p1"', dot)
        self.assertIn('"p2"', dot)
        self.assertIn('"t1"', dot)
    
    def test_to_graphviz_reachability(self):
        """Test reachability graph Graphviz export."""
        net = PetriNet(debug=False)
        net.add_place("p1", initial_tokens=1)
        net.add_place("p2")
        net.add_transition("t1", consume_from=[("p1", 1)], produce_to=[("p2", 1)])
        
        dot = net.to_graphviz_reachability()
        
        self.assertIn("digraph ReachabilityGraph", dot)
        self.assertIn("M0", dot)
        self.assertIn("M1", dot)
    
    def test_save_graphviz(self):
        """Test saving Graphviz to file."""
        net = PetriNet(debug=False)
        net.add_place("p1", initial_tokens=1)
        net.add_transition("t1", consume_from=[("p1", 1)])
        
        with tempfile.NamedTemporaryFile(suffix=".dot", delete=False) as f:
            filepath = f.name
        
        try:
            net.save_graphviz(filepath)
            with open(filepath, 'r') as f:
                content = f.read()
            self.assertIn("digraph", content)
        finally:
            os.unlink(filepath)


class TestPetriNetSerialization(unittest.TestCase):
    """Tests for serialization."""
    
    def test_to_dict(self):
        """Test dictionary serialization."""
        net = PetriNet(debug=False)
        net.add_place("p1", capacity=5, initial_tokens=2)
        net.add_transition("t1", consume_from=[("p1", 1)])
        
        data = net.to_dict()
        
        self.assertIn("places", data)
        self.assertIn("transitions", data)
        self.assertIn("marking", data)
        self.assertEqual(data["places"]["p1"]["tokens"], 2)
    
    def test_to_json(self):
        """Test JSON serialization."""
        net = PetriNet(debug=False)
        net.add_place("p1", initial_tokens=1)
        
        json_str = net.to_json()
        
        self.assertIn('"p1"', json_str)
        self.assertIn('"tokens"', json_str)


class TestPetriNetPriority(unittest.TestCase):
    """Tests for transition priority."""
    
    def test_priority_ordering(self):
        """Test that higher priority transitions fire first."""
        net = PetriNet(debug=False)
        net.add_place("p1", initial_tokens=1)
        net.add_place("result_low")
        net.add_place("result_high")
        
        # Both consume from p1, but high priority should win
        net.add_transition("low", consume_from=[("p1", 1)], produce_to=[("result_low", 1)], priority=0)
        net.add_transition("high", consume_from=[("p1", 1)], produce_to=[("result_high", 1)], priority=10)
        
        enabled = net.get_enabled_transitions()
        self.assertEqual(enabled[0], "high")  # High priority first


if __name__ == "__main__":
    unittest.main()
