import unittest
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from DaggyD.pregel_core import Pregel


class TestIntegration(unittest.TestCase):
    
    def test_maximum_value_propagation(self):
        """
        Integration test for the Maximum Value algorithm from the Pregel paper.
        Tests that the maximum value propagates correctly through a graph.
        """
        pregel = Pregel(debug=False)
        
        pregel.add_channel("vertex_1", "LastValue", initial_value=3)
        pregel.add_channel("vertex_2", "LastValue", initial_value=6)
        pregel.add_channel("vertex_3", "LastValue", initial_value=2)
        pregel.add_channel("vertex_4", "LastValue", initial_value=1)
        
        pregel.add_channel("messages_1", "Topic")
        pregel.add_channel("messages_2", "Topic")
        pregel.add_channel("messages_3", "Topic")
        pregel.add_channel("messages_4", "Topic")
        
        pregel.add_channel("global_max", "BinaryOperator", operator=max)
        
        @pregel.add_to_registry(
            "node_1",
            subscribe_to=["vertex_1", "messages_1"],
            write_to={"messages_2": "messages_2", "messages_3": "messages_3", "global_max": "global_max"}
        )
        def compute_node_1(inputs):
            current_value = inputs.get("vertex_1", 0)
            incoming_messages = inputs.get("messages_1", [])
            
            if incoming_messages:
                max_received = max(incoming_messages)
                if max_received > current_value:
                    return {
                        "messages_2": max_received,
                        "messages_3": max_received,
                        "global_max": max_received
                    }
            else:
                return {
                    "messages_2": current_value,
                    "messages_3": current_value,
                    "global_max": current_value
                }
        
        @pregel.add_to_registry(
            "node_2",
            subscribe_to=["vertex_2", "messages_2"],
            write_to={"messages_1": "messages_1", "messages_4": "messages_4", "global_max": "global_max"}
        )
        def compute_node_2(inputs):
            current_value = inputs.get("vertex_2", 0)
            incoming_messages = inputs.get("messages_2", [])
            
            if incoming_messages:
                max_received = max(incoming_messages)
                if max_received > current_value:
                    return {
                        "messages_1": max_received,
                        "messages_4": max_received,
                        "global_max": max_received
                    }
            else:
                return {
                    "messages_1": current_value,
                    "messages_4": current_value,
                    "global_max": current_value
                }
        
        @pregel.add_to_registry(
            "node_3",
            subscribe_to=["vertex_3", "messages_3"],
            write_to={"messages_1": "messages_1", "messages_4": "messages_4", "global_max": "global_max"}
        )
        def compute_node_3(inputs):
            current_value = inputs.get("vertex_3", 0)
            incoming_messages = inputs.get("messages_3", [])
            
            if incoming_messages:
                max_received = max(incoming_messages)
                if max_received > current_value:
                    return {
                        "messages_1": max_received,
                        "messages_4": max_received,
                        "global_max": max_received
                    }
            else:
                return {
                    "messages_1": current_value,
                    "messages_4": current_value,
                    "global_max": current_value
                }
        
        @pregel.add_to_registry(
            "node_4",
            subscribe_to=["vertex_4", "messages_4"],
            write_to={"messages_2": "messages_2", "messages_3": "messages_3", "global_max": "global_max"}
        )
        def compute_node_4(inputs):
            current_value = inputs.get("vertex_4", 0)
            incoming_messages = inputs.get("messages_4", [])
            
            if incoming_messages:
                max_received = max(incoming_messages)
                if max_received > current_value:
                    return {
                        "messages_2": max_received,
                        "messages_3": max_received,
                        "global_max": max_received
                    }
            else:
                return {
                    "messages_2": current_value,
                    "messages_3": current_value,
                    "global_max": current_value
                }
        
        final_state = pregel.run(max_supersteps=10)
        
        self.assertEqual(final_state["global_max"], 6)
        
        for i in range(1, 5):
            msg_channel = f"messages_{i}"
            if msg_channel in final_state and final_state[msg_channel]:
                self.assertEqual(max(final_state[msg_channel]), 6)
    
    def test_multi_stage_pipeline(self):
        """
        Test a multi-stage data processing pipeline using Pregel.
        Uses state tracking to ensure single execution.
        """
        pregel = Pregel(debug=False)
        
        processed_state = {"filter": False, "transform": False, "aggregate": False}
        
        pregel.add_channel("raw_data", "LastValue", initial_value=[1, 2, 3, 4, 5])
        pregel.add_channel("filtered", "LastValue")
        pregel.add_channel("transformed", "LastValue")
        pregel.add_channel("aggregated", "LastValue")
        
        @pregel.add_to_registry("filter_stage", subscribe_to=["raw_data"], write_to={"output": "filtered"})
        def filter_data(inputs):
            if processed_state["filter"]:
                return None
            data = inputs.get("raw_data", [])
            filtered = [x for x in data if x > 2]
            processed_state["filter"] = True
            return {"output": filtered}
        
        @pregel.add_to_registry("transform_stage", subscribe_to=["filtered"], write_to={"output": "transformed"})
        def transform_data(inputs):
            if processed_state["transform"]:
                return None
            data = inputs.get("filtered")
            if data is not None:
                transformed = [x * 2 for x in data]
                processed_state["transform"] = True
                return {"output": transformed}
            return None
        
        @pregel.add_to_registry("aggregate_stage", subscribe_to=["transformed"], write_to={"sum": "aggregated"})
        def aggregate_data(inputs):
            if processed_state["aggregate"]:
                return None
            data = inputs.get("transformed")
            if data is not None:
                total = sum(data)
                processed_state["aggregate"] = True
                return {"sum": total}
            return None
        
        final_state = pregel.run(max_supersteps=10)
        
        self.assertEqual(final_state["filtered"], [3, 4, 5])
        self.assertEqual(final_state["transformed"], [6, 8, 10])
        self.assertEqual(final_state["aggregated"], 24)
    
    def test_graph_with_cycles(self):
        """
        Test that Pregel correctly handles graphs with cycles.
        """
        pregel = Pregel(debug=False)
        
        pregel.add_channel("counter_a", "LastValue", initial_value=1)
        pregel.add_channel("counter_b", "LastValue", initial_value=0)
        pregel.add_channel("stop_signal", "LastValue", initial_value=False)
        
        @pregel.add_to_registry("node_a", subscribe_to=["counter_b"], write_to={"value": "counter_a", "stop": "stop_signal"})
        def process_a(inputs):
            val = inputs.get("counter_b")
            if val is not None and val < 5:
                return {"value": val + 1, "stop": False}
            elif val is not None:
                return {"value": val, "stop": True}
            return None
        
        @pregel.add_to_registry("node_b", subscribe_to=["counter_a"], write_to={"value": "counter_b"})
        def process_b(inputs):
            val = inputs.get("counter_a")
            if val is not None and val <= 5:
                return {"value": val + 1}
            return None
        
        final_state = pregel.run(max_supersteps=20)
        
        self.assertGreaterEqual(final_state["counter_a"], 5)
        self.assertGreaterEqual(final_state["counter_b"], 5)
    
    def test_complex_workflow_with_all_channel_types(self):
        """
        Integration test using all channel types in a complex workflow.
        """
        pregel = Pregel(debug=False)
        
        pregel.add_channel("input", "LastValue", initial_value="start")
        pregel.add_channel("messages", "Topic")
        pregel.add_channel("max_value", "BinaryOperator", operator=max, initial_value=0)
        pregel.add_channel("temp_state", "Ephemeral")
        pregel.add_channel("final_output", "LastValue")
        
        @pregel.add_to_registry("producer", subscribe_to=["input"], write_to={"msg": "messages", "temp": "temp_state", "max": "max_value"})
        def produce_data(inputs):
            trigger = inputs.get("input")
            if trigger == "start":
                return {
                    "msg": "message_1",
                    "temp": "temporary_data",
                    "max": 10
                }
            return None
        
        @pregel.add_to_registry("consumer", subscribe_to=["messages", "temp_state"], write_to={"output": "final_output", "max": "max_value"})
        def consume_data(inputs):
            messages = inputs.get("messages", [])
            temp = inputs.get("temp_state")
            
            if messages or temp:
                result = f"Processed {len(messages)} messages"
                if temp:
                    result += f" with temp: {temp}"
                return {
                    "output": result,
                    "max": 20
                }
            return None
        
        final_state = pregel.run(max_supersteps=5)
        
        self.assertIn("Processed", final_state.get("final_output", ""))
        self.assertEqual(final_state["max_value"], 20)
        self.assertIsNone(final_state["temp_state"])
    
    def test_termination_condition(self):
        """
        Test that Pregel correctly terminates when no nodes are active.
        Uses state to track iterations and terminate.
        """
        pregel = Pregel(debug=False)
        
        counter_state = {"value": 3, "done": False}
        
        pregel.add_channel("countdown", "LastValue", initial_value=3)
        pregel.add_channel("result", "LastValue")
        
        @pregel.add_to_registry("counter", subscribe_to=["countdown"], write_to={"value": "countdown", "result": "result"})
        def count_down(inputs):
            if counter_state["done"]:
                return None
            val = counter_state["value"]
            if val > 0:
                counter_state["value"] = val - 1
                if counter_state["value"] == 0:
                    counter_state["done"] = True
                return {"value": val - 1, "result": f"Count: {val}"}
            return None
        
        final_state = pregel.run(max_supersteps=10)
        
        self.assertEqual(final_state["countdown"], 0)
        self.assertEqual(final_state["result"], "Count: 1")


if __name__ == "__main__":
    unittest.main()