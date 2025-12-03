import unittest
import sys
import os
import tempfile
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from DagBi.pregel_core import Pregel, PregelParallel, create_node_builder, create_pregel_graph


class TestPregelCore(unittest.TestCase):
    
    def test_pregel_initialization(self):
        pregel = Pregel(debug=False)
        self.assertEqual(pregel.superstep, 0)
        self.assertEqual(len(pregel.nodes), 0)
        self.assertEqual(len(pregel.channels), 0)
    
    def test_pregel_parallel_initialization(self):
        pregel = Pregel(debug=False, parallel=True, max_workers=4)
        self.assertEqual(pregel.superstep, 0)
        self.assertTrue(pregel.parallel)
        self.assertEqual(pregel.max_workers, 4)
    
    def test_add_channel(self):
        pregel = Pregel(debug=False)
        pregel.add_channel("test_channel", "LastValue", initial_value=42)
        self.assertIn("test_channel", pregel.channels)
        self.assertEqual(pregel.channels["test_channel"]["read"](), 42)
    
    def test_add_node(self):
        pregel = Pregel(debug=False)
        
        def test_func():
            return "test_output"
        
        pregel.add_node("test_node", test_func, [], {})
        self.assertIn("test_node", pregel.nodes)
        self.assertEqual(pregel.nodes["test_node"]["func"], test_func)
    
    def test_node_builder(self):
        builder = create_node_builder("test_node")
        
        def compute_func(inputs):
            return {"output": inputs.get("input", 0) * 2}
        
        builder["subscribe_to_fn"]("input_channel")
        builder["write_to_fn"](output="output_channel")
        builder["do_fn"](compute_func)
        
        node = builder["build_fn"]()
        self.assertEqual(node["name"], "test_node")
        self.assertIn("input_channel", node["subscribe_to"])
        self.assertIn("output", node["write_to"])
    
    def test_simple_computation(self):
        pregel = Pregel(debug=False)
        
        pregel.add_channel("input", "LastValue", initial_value=5)
        pregel.add_channel("output", "LastValue")
        
        @pregel.add_to_registry("doubler", subscribe_to=["input"], write_to={"result": "output"})
        def double_value(inputs):
            val = inputs.get("input", 0)
            return {"result": val * 2}
        
        final_state = pregel.run(max_supersteps=5)
        self.assertEqual(final_state["output"], 10)
    
    def test_simple_computation_parallel(self):
        pregel = Pregel(debug=False, parallel=True)
        
        pregel.add_channel("input", "LastValue", initial_value=5)
        pregel.add_channel("output", "LastValue")
        
        @pregel.add_to_registry("doubler", subscribe_to=["input"], write_to={"result": "output"})
        def double_value(inputs):
            val = inputs.get("input", 0)
            return {"result": val * 2}
        
        final_state = pregel.run(max_supersteps=5)
        self.assertEqual(final_state["output"], 10)
    
    def test_topic_channel(self):
        pregel = Pregel(debug=False)
        
        pregel.add_channel("messages", "Topic")
        pregel.add_channel("trigger", "LastValue", initial_value=True)
        
        @pregel.add_to_registry("sender", subscribe_to=["trigger"], write_to={"msg": "messages"})
        def send_message(inputs):
            if inputs.get("trigger"):
                return {"msg": "Hello"}
            return None
        
        pregel.run(max_supersteps=1)
        
        pregel.channels["messages"]["write"]("World")
        pregel.channels["messages"]["checkpoint"]()
        
        messages = pregel.channels["messages"]["read"]()
        self.assertIn("Hello", messages)
        self.assertIn("World", messages)
    
    def test_binary_operator_channel(self):
        pregel = Pregel(debug=False)
        
        pregel.add_channel("sum", "BinaryOperator", operator=lambda a, b: a + b, initial_value=0)
        pregel.add_channel("trigger", "LastValue", initial_value=True)
        
        @pregel.add_to_registry("adder", subscribe_to=["trigger"], write_to={"value": "sum"})
        def add_values(inputs):
            if inputs.get("trigger"):
                return {"value": 10}
            return None
        
        pregel.run(max_supersteps=1)
        
        pregel.channels["sum"]["write"](20)
        pregel.channels["sum"]["write"](30)
        pregel.channels["sum"]["checkpoint"]()
        
        total = pregel.channels["sum"]["read"]()
        self.assertEqual(total, 60)
    
    def test_accumulator_channel(self):
        """Test Accumulator channel in Pregel - aggregates per superstep, then resets."""
        pregel = Pregel(debug=False)
        
        # Set up channels
        pregel.add_channel("iteration", "LastValue", initial_value=0)
        pregel.add_channel("inbox", "Accumulator", operator=lambda a, b: a + b, initial_value=0.0)
        pregel.add_channel("result", "LastValue")
        
        # Node that writes multiple values to inbox
        @pregel.add_to_registry("sender1", subscribe_to=["iteration"], write_to={"msg": "inbox"})
        def send1(inputs):
            iter_num = inputs.get("iteration", 0)
            if iter_num < 3:
                return {"msg": 10.0}
            return None
        
        @pregel.add_to_registry("sender2", subscribe_to=["iteration"], write_to={"msg": "inbox"})
        def send2(inputs):
            iter_num = inputs.get("iteration", 0)
            if iter_num < 3:
                return {"msg": 20.0}
            return None
        
        # Node that reads aggregated inbox and advances iteration
        @pregel.add_to_registry("receiver", subscribe_to=["inbox", "iteration"], write_to={"out": "result", "iter": "iteration"})
        def receive(inputs):
            inbox_val = inputs.get("inbox") or 0.0
            iter_num = inputs.get("iteration", 0)
            if iter_num < 3:
                return {"out": inbox_val, "iter": iter_num + 1}
            return None
        
        final_state = pregel.run(max_supersteps=10)
        
        # After first superstep: inbox should have 30.0 (10 + 20)
        # After second superstep: inbox again 30.0 (fresh aggregation)
        # The result channel should have the last inbox value read
        self.assertEqual(final_state["iteration"], 3)
        # Last superstep's inbox should be 30.0 (or 0 if no writes happened)
        self.assertIn(final_state["result"], [0.0, 30.0])
    
    def test_accumulator_channel_pregel_message_passing(self):
        """Test Accumulator channel for Pregel-style message passing between nodes."""
        pregel = Pregel(debug=False)
        
        # Three nodes send contributions to a central aggregator
        pregel.add_channel("trigger", "LastValue", initial_value=True)
        pregel.add_channel("aggregator", "Accumulator", operator=lambda a, b: a + b, initial_value=0)
        pregel.add_channel("final_sum", "LastValue")
        
        @pregel.add_to_registry("node_a", subscribe_to=["trigger"], write_to={"contrib": "aggregator"})
        def node_a(inputs):
            if inputs.get("trigger"):
                return {"contrib": 100}
            return None
        
        @pregel.add_to_registry("node_b", subscribe_to=["trigger"], write_to={"contrib": "aggregator"})
        def node_b(inputs):
            if inputs.get("trigger"):
                return {"contrib": 200}
            return None
        
        @pregel.add_to_registry("node_c", subscribe_to=["trigger"], write_to={"contrib": "aggregator"})
        def node_c(inputs):
            if inputs.get("trigger"):
                return {"contrib": 300}
            return None
        
        @pregel.add_to_registry("collector", subscribe_to=["aggregator"], write_to={"sum": "final_sum"})
        def collector(inputs):
            agg_val = inputs.get("aggregator") or 0
            if agg_val > 0:
                return {"sum": agg_val}
            return None
        
        final_state = pregel.run(max_supersteps=5)
        
        # The aggregator should have summed 100 + 200 + 300 = 600
        self.assertEqual(final_state["final_sum"], 600)
    
    def test_ephemeral_channel(self):
        pregel = Pregel(debug=False)
        
        pregel.add_channel("ephemeral", "Ephemeral")
        pregel.add_channel("persistent", "LastValue")
        pregel.add_channel("trigger", "LastValue", initial_value=True)
        
        @pregel.add_to_registry("writer", subscribe_to=["trigger"], write_to={"temp": "ephemeral", "perm": "persistent"})
        def write_values(inputs):
            if inputs.get("trigger"):
                return {"temp": "temporary", "perm": "permanent"}
            return None
        
        pregel.run(max_supersteps=1)
        
        self.assertEqual(pregel.channels["persistent"]["read"](), "permanent")
        self.assertIsNone(pregel.channels["ephemeral"]["read"]())
    
    def test_multiple_nodes(self):
        pregel = Pregel(debug=False)
        
        pregel.add_channel("input", "LastValue", initial_value=2)
        pregel.add_channel("intermediate", "LastValue")
        pregel.add_channel("output", "LastValue")
        
        @pregel.add_to_registry("node1", subscribe_to=["input"], write_to={"result": "intermediate"})
        def multiply_by_3(inputs):
            return {"result": inputs.get("input", 0) * 3}
        
        @pregel.add_to_registry("node2", subscribe_to=["intermediate"], write_to={"result": "output"})
        def add_10(inputs):
            val = inputs.get("intermediate")
            if val is not None:
                return {"result": val + 10}
            return None
        
        final_state = pregel.run(max_supersteps=5)
        self.assertEqual(final_state["output"], 16)
    
    def test_multiple_nodes_parallel(self):
        pregel = Pregel(debug=False, parallel=True)
        
        pregel.add_channel("input", "LastValue", initial_value=10)
        pregel.add_channel("output_a", "LastValue")
        pregel.add_channel("output_b", "LastValue")
        pregel.add_channel("output_c", "LastValue")
        
        @pregel.add_to_registry("node_a", subscribe_to=["input"], write_to={"result": "output_a"})
        def process_a(inputs):
            return {"result": inputs.get("input", 0) * 2}
        
        @pregel.add_to_registry("node_b", subscribe_to=["input"], write_to={"result": "output_b"})
        def process_b(inputs):
            return {"result": inputs.get("input", 0) * 3}
        
        @pregel.add_to_registry("node_c", subscribe_to=["input"], write_to={"result": "output_c"})
        def process_c(inputs):
            return {"result": inputs.get("input", 0) * 4}
        
        final_state = pregel.run(max_supersteps=5)
        self.assertEqual(final_state["output_a"], 20)
        self.assertEqual(final_state["output_b"], 30)
        self.assertEqual(final_state["output_c"], 40)
    
    def test_node_deactivation_on_error(self):
        pregel = Pregel(debug=False)
        
        pregel.add_channel("input", "LastValue", initial_value=1)
        pregel.add_channel("output", "LastValue")
        
        @pregel.add_to_registry("faulty_node", subscribe_to=["input"], write_to={"result": "output"})
        def will_fail(inputs):
            raise ValueError("Intentional error")
        
        pregel.run(max_supersteps=2)
        
        self.assertEqual(pregel.executed["faulty_node"], "failed")
        self.assertFalse(pregel.nodes["faulty_node"]["active"])
    
    def test_reset_functionality(self):
        pregel = Pregel(debug=False)
        
        pregel.add_channel("counter", "LastValue", initial_value=0)
        
        @pregel.add_to_registry("incrementer", subscribe_to=["counter"], write_to={"result": "counter"})
        def increment(inputs):
            return {"result": inputs.get("counter", 0) + 1}
        
        pregel.run(max_supersteps=3)
        self.assertGreater(pregel.superstep, 0)
        
        pregel.reset()
        self.assertEqual(pregel.superstep, 0)
        self.assertTrue(all(node["active"] for node in pregel.nodes.values()))
    
    def test_checkpoint_and_restore(self):
        pregel = Pregel(debug=False)
        
        pregel.add_channel("value", "LastValue", initial_value=100)
        pregel.add_channel("output", "LastValue")
        
        @pregel.add_to_registry("processor", subscribe_to=["value"], write_to={"result": "output"})
        def process(inputs):
            return {"result": inputs.get("value", 0) * 2}
        
        pregel.run(max_supersteps=1)
        checkpoint = pregel.get_checkpoint()
        
        self.assertEqual(checkpoint["superstep"], 1)
        self.assertIn("value", checkpoint["channel_states"])
        
        pregel.reset()
        self.assertEqual(pregel.superstep, 0)
        
        pregel.restore_checkpoint(checkpoint)
        self.assertEqual(pregel.superstep, 1)
    
    def test_save_and_load_db(self):
        pregel = Pregel(debug=False)
        
        pregel.add_channel("value", "LastValue", initial_value=42)
        pregel.add_channel("output", "LastValue")
        
        @pregel.add_to_registry("processor", subscribe_to=["value"], write_to={"result": "output"})
        def process(inputs):
            return {"result": inputs.get("value", 0) + 8}
        
        pregel.run(max_supersteps=1)
        
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
            db_path = f.name
        
        try:
            pregel.save_to_db(db_path)
            
            pregel2 = Pregel(debug=False)
            pregel2.add_channel("value", "LastValue")
            pregel2.add_channel("output", "LastValue")
            
            @pregel2.add_to_registry("processor", subscribe_to=["value"], write_to={"result": "output"})
            def process2(inputs):
                return {"result": inputs.get("value", 0) + 8}
            
            loaded = pregel2.load_from_db(db_path)
            self.assertTrue(loaded)
            self.assertEqual(pregel2.superstep, pregel.superstep)
        finally:
            os.unlink(db_path)
    
    def test_create_pregel_graph(self):
        def double_func(inputs):
            return {"result": inputs.get("input", 0) * 2}
        
        config = {
            "channels": [
                {"name": "input", "type": "LastValue", "initial_value": 5},
                {"name": "output", "type": "LastValue"}
            ],
            "nodes": [
                {
                    "name": "doubler",
                    "func": double_func,
                    "subscribe_to": ["input"],
                    "write_to": {"result": "output"}
                }
            ]
        }
        
        pregel = create_pregel_graph(config, debug=False)
        final_state = pregel.run(max_supersteps=5)
        self.assertEqual(final_state["output"], 10)


class TestPregelParallel(unittest.TestCase):
    
    def test_pregel_parallel_basic(self):
        pregel = PregelParallel(debug=False, use_processes=False)
        
        pregel.add_channel("input", "LastValue", initial_value=10)
        pregel.add_channel("output", "LastValue")
        
        @pregel.add_to_registry("processor", subscribe_to=["input"], write_to={"result": "output"})
        def process(inputs):
            return {"result": inputs.get("input", 0) * 5}
        
        final_state = pregel.run(max_supersteps=5)
        self.assertEqual(final_state["output"], 50)
    
    def test_pregel_parallel_multiple_nodes(self):
        pregel = PregelParallel(debug=False, max_workers=4, use_processes=False)
        
        pregel.add_channel("input", "LastValue", initial_value=5)
        pregel.add_channel("a_out", "LastValue")
        pregel.add_channel("b_out", "LastValue")
        pregel.add_channel("c_out", "LastValue")
        pregel.add_channel("d_out", "LastValue")
        
        @pregel.add_to_registry("node_a", subscribe_to=["input"], write_to={"result": "a_out"})
        def node_a(inputs):
            return {"result": inputs.get("input", 0) + 1}
        
        @pregel.add_to_registry("node_b", subscribe_to=["input"], write_to={"result": "b_out"})
        def node_b(inputs):
            return {"result": inputs.get("input", 0) + 2}
        
        @pregel.add_to_registry("node_c", subscribe_to=["input"], write_to={"result": "c_out"})
        def node_c(inputs):
            return {"result": inputs.get("input", 0) + 3}
        
        @pregel.add_to_registry("node_d", subscribe_to=["input"], write_to={"result": "d_out"})
        def node_d(inputs):
            return {"result": inputs.get("input", 0) + 4}
        
        final_state = pregel.run(max_supersteps=5)
        
        self.assertEqual(final_state["a_out"], 6)
        self.assertEqual(final_state["b_out"], 7)
        self.assertEqual(final_state["c_out"], 8)
        self.assertEqual(final_state["d_out"], 9)
    
    def test_pregel_parallel_aggregation(self):
        pregel = PregelParallel(debug=False, use_processes=False)
        
        produced = {"p1": False, "p2": False, "p3": False}
        
        pregel.add_channel("input", "LastValue", initial_value=True)
        pregel.add_channel("sum", "BinaryOperator", operator=lambda a, b: a + b, initial_value=0)
        
        @pregel.add_to_registry("producer_1", subscribe_to=["input"], write_to={"val": "sum"})
        def produce_1(inputs):
            if not produced["p1"] and inputs.get("input"):
                produced["p1"] = True
                return {"val": 10}
            return None
        
        @pregel.add_to_registry("producer_2", subscribe_to=["input"], write_to={"val": "sum"})
        def produce_2(inputs):
            if not produced["p2"] and inputs.get("input"):
                produced["p2"] = True
                return {"val": 20}
            return None
        
        @pregel.add_to_registry("producer_3", subscribe_to=["input"], write_to={"val": "sum"})
        def produce_3(inputs):
            if not produced["p3"] and inputs.get("input"):
                produced["p3"] = True
                return {"val": 30}
            return None
        
        final_state = pregel.run(max_supersteps=5)
        self.assertEqual(final_state["sum"], 60)
    
    def test_pregel_parallel_reset(self):
        pregel = PregelParallel(debug=False, use_processes=False)
        
        pregel.add_channel("counter", "LastValue", initial_value=0)
        
        @pregel.add_to_registry("inc", subscribe_to=["counter"], write_to={"result": "counter"})
        def increment(inputs):
            val = inputs.get("counter", 0)
            if val < 3:
                return {"result": val + 1}
            return None
        
        pregel.run(max_supersteps=10)
        self.assertGreater(pregel.superstep, 0)
        
        pregel.reset()
        self.assertEqual(pregel.superstep, 0)


class TestPregelGraphviz(unittest.TestCase):
    """Tests for Pregel Graphviz export functionality."""
    
    def test_to_graphviz_basic(self):
        """Test basic Graphviz DOT export."""
        pregel = Pregel(debug=False)
        pregel.add_channel("input", "LastValue", initial_value=10)
        pregel.add_channel("output", "LastValue")
        
        @pregel.add_to_registry("processor", subscribe_to=["input"], write_to={"result": "output"})
        def process(inputs):
            return {"result": inputs.get("input", 0) * 2}
        
        dot = pregel.to_graphviz()
        
        self.assertIn("digraph PregelGraph", dot)
        self.assertIn("ch_input", dot)
        self.assertIn("ch_output", dot)
        self.assertIn("node_processor", dot)
    
    def test_to_graphviz_with_title(self):
        """Test Graphviz export with title."""
        pregel = Pregel(debug=False)
        pregel.add_channel("data", "LastValue", initial_value=42)
        
        dot = pregel.to_graphviz(title="Test Graph")
        
        self.assertIn('label="Test Graph"', dot)
    
    def test_to_graphviz_channel_types(self):
        """Test that different channel types have different colors."""
        pregel = Pregel(debug=False)
        pregel.add_channel("last_value", "LastValue", initial_value=1)
        pregel.add_channel("topic", "Topic")
        pregel.add_channel("binary_op", "BinaryOperator", operator=lambda a, b: a + b, initial_value=0)
        pregel.add_channel("ephemeral", "Ephemeral")
        pregel.add_channel("accum", "Accumulator", operator=lambda a, b: a + b, initial_value=0)
        
        dot = pregel.to_graphviz()
        
        # Check that all channels are represented
        self.assertIn("ch_last_value", dot)
        self.assertIn("ch_topic", dot)
        self.assertIn("ch_binary_op", dot)
        self.assertIn("ch_ephemeral", dot)
        self.assertIn("ch_accum", dot)
        
        # Check channel type labels
        self.assertIn("[LastValue]", dot)
        self.assertIn("[Topic]", dot)
        self.assertIn("[BinaryOperator]", dot)
        self.assertIn("[Ephemeral]", dot)
        self.assertIn("[Accumulator]", dot)
    
    def test_to_graphviz_node_status(self):
        """Test that node status is reflected in the graph."""
        pregel = Pregel(debug=False)
        pregel.add_channel("input", "LastValue", initial_value=5)
        pregel.add_channel("output", "LastValue")
        
        @pregel.add_to_registry("doubler", subscribe_to=["input"], write_to={"result": "output"})
        def double_value(inputs):
            return {"result": inputs.get("input", 0) * 2}
        
        # Before running
        dot_before = pregel.to_graphviz()
        self.assertIn("(active)", dot_before)
        
        # After running
        pregel.run(max_supersteps=2)
        dot_after = pregel.to_graphviz()
        self.assertIn("(succeeded)", dot_after)
    
    def test_to_graphviz_edges(self):
        """Test that subscription and write edges are correct."""
        pregel = Pregel(debug=False)
        pregel.add_channel("input", "LastValue", initial_value=1)
        pregel.add_channel("output", "LastValue")
        
        @pregel.add_to_registry("node1", subscribe_to=["input"], write_to={"out": "output"})
        def node1(inputs):
            return {"out": inputs.get("input", 0) + 1}
        
        dot = pregel.to_graphviz()
        
        # Check subscription edge (channel -> node)
        self.assertIn('"ch_input" -> "node_node1"', dot)
        
        # Check write edge (node -> channel)
        self.assertIn('"node_node1" -> "ch_output"', dot)
    
    def test_to_graphviz_dataflow(self):
        """Test dataflow graph export."""
        pregel = Pregel(debug=False)
        pregel.add_channel("ch1", "LastValue", initial_value=1)
        pregel.add_channel("ch2", "LastValue")
        pregel.add_channel("ch3", "LastValue")
        
        @pregel.add_to_registry("producer", subscribe_to=["ch1"], write_to={"out": "ch2"})
        def produce(inputs):
            return {"out": inputs.get("ch1", 0) + 1}
        
        @pregel.add_to_registry("consumer", subscribe_to=["ch2"], write_to={"out": "ch3"})
        def consume(inputs):
            val = inputs.get("ch2")
            if val is not None:
                return {"out": val + 1}
            return None
        
        dot = pregel.to_graphviz_dataflow(title="Dataflow Test")
        
        self.assertIn("digraph PregelDataflow", dot)
        self.assertIn('label="Dataflow Test"', dot)
        self.assertIn('"producer"', dot)
        self.assertIn('"consumer"', dot)
        # Should show edge from producer to consumer via ch2
        self.assertIn('"producer" -> "consumer"', dot)
        self.assertIn('label="ch2"', dot)
    
    def test_save_graphviz(self):
        """Test saving Graphviz to file."""
        pregel = Pregel(debug=False)
        pregel.add_channel("data", "LastValue", initial_value=42)
        
        @pregel.add_to_registry("node", subscribe_to=["data"], write_to={})
        def process(inputs):
            return None
        
        with tempfile.NamedTemporaryFile(suffix=".dot", delete=False) as f:
            filepath = f.name
        
        try:
            pregel.save_graphviz(filepath)
            with open(filepath, 'r') as f:
                content = f.read()
            self.assertIn("digraph PregelGraph", content)
        finally:
            os.unlink(filepath)
    
    def test_save_graphviz_dataflow(self):
        """Test saving dataflow graph to file."""
        pregel = Pregel(debug=False)
        pregel.add_channel("data", "LastValue", initial_value=42)
        
        with tempfile.NamedTemporaryFile(suffix=".dot", delete=False) as f:
            filepath = f.name
        
        try:
            pregel.save_graphviz_dataflow(filepath)
            with open(filepath, 'r') as f:
                content = f.read()
            self.assertIn("digraph PregelDataflow", content)
        finally:
            os.unlink(filepath)
    
    def test_to_graphviz_without_values(self):
        """Test Graphviz export without showing channel values."""
        pregel = Pregel(debug=False)
        pregel.add_channel("secret", "LastValue", initial_value="sensitive_data")
        
        dot_with_values = pregel.to_graphviz(show_channel_values=True)
        dot_without_values = pregel.to_graphviz(show_channel_values=False)
        
        # With values should show the actual value
        self.assertIn("sensitive_data", dot_with_values)
        
        # Without values should not show the actual value
        self.assertNotIn("sensitive_data", dot_without_values)
        # But should still have the channel name
        self.assertIn("secret", dot_without_values)


if __name__ == "__main__":
    unittest.main()
