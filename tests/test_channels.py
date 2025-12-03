import unittest
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from DagBi.channels import (
    create_last_value_channel,
    create_topic_channel,
    create_binary_operator_channel,
    create_ephemeral_channel,
    create_accumulator_channel
)


class TestChannels(unittest.TestCase):
    
    def test_last_value_channel(self):
        channel = create_last_value_channel(initial_value=10)
        
        self.assertEqual(channel["read"](), 10)
        self.assertFalse(channel["is_empty"]())
        
        channel["write"](20)
        self.assertTrue(channel["has_updates"]())
        self.assertEqual(channel["read"](), 20)
        
        checkpoint_val = channel["checkpoint"]()
        self.assertEqual(checkpoint_val, 20)
        
        channel["clear"]()
        self.assertFalse(channel["has_updates"]())
        self.assertEqual(channel["read"](), 20)
        
        channel["restore"](30)
        self.assertEqual(channel["read"](), 30)
        self.assertFalse(channel["has_updates"]())
    
    def test_last_value_channel_empty(self):
        channel = create_last_value_channel()
        
        self.assertIsNone(channel["read"]())
        self.assertTrue(channel["is_empty"]())
        
        channel["write"](5)
        self.assertFalse(channel["is_empty"]())
    
    def test_topic_channel(self):
        channel = create_topic_channel()
        
        self.assertEqual(channel["read"](), [])
        self.assertTrue(channel["is_empty"]())
        
        channel["write"]("message1")
        channel["write"]("message2")
        self.assertTrue(channel["has_updates"]())
        self.assertEqual(channel["read"](), [])
        
        checkpoint_val = channel["checkpoint"]()
        self.assertEqual(checkpoint_val, ["message1", "message2"])
        self.assertEqual(channel["read"](), ["message1", "message2"])
        # BSP: has_updates() stays True until consume() is called (for next plan phase)
        self.assertTrue(channel["has_updates"]())
        channel["consume"]()  # Mark messages as consumed
        self.assertFalse(channel["has_updates"]())
        
        channel["write"]("message3")
        channel["checkpoint"]()
        self.assertEqual(channel["read"](), ["message1", "message2", "message3"])
        
        channel["clear"]()
        self.assertEqual(channel["read"](), [])
        self.assertTrue(channel["is_empty"]())
        
        channel["restore"](["restored1", "restored2"])
        self.assertEqual(channel["read"](), ["restored1", "restored2"])
    
    def test_binary_operator_channel_sum(self):
        channel = create_binary_operator_channel(lambda a, b: a + b, initial_value=0)
        
        self.assertEqual(channel["read"](), 0)
        self.assertFalse(channel["is_empty"]())
        
        channel["write"](10)
        channel["write"](20)
        channel["write"](30)
        self.assertTrue(channel["has_updates"]())
        self.assertEqual(channel["read"](), 0)
        
        checkpoint_val = channel["checkpoint"]()
        self.assertEqual(checkpoint_val, 60)
        self.assertEqual(channel["read"](), 60)
        # BSP: has_updates() stays True until consume() is called
        self.assertTrue(channel["has_updates"]())
        channel["consume"]()
        self.assertFalse(channel["has_updates"]())
        
        channel["write"](40)
        channel["checkpoint"]()
        self.assertEqual(channel["read"](), 100)
    
    def test_binary_operator_channel_max(self):
        channel = create_binary_operator_channel(max)
        
        self.assertIsNone(channel["read"]())
        self.assertTrue(channel["is_empty"]())
        
        channel["write"](5)
        channel["write"](10)
        channel["write"](3)
        channel["checkpoint"]()
        self.assertEqual(channel["read"](), 10)
        
        channel["write"](15)
        channel["write"](8)
        channel["checkpoint"]()
        self.assertEqual(channel["read"](), 15)
    
    def test_binary_operator_channel_string_concat(self):
        channel = create_binary_operator_channel(lambda a, b: a + b, initial_value="")
        
        channel["write"]("Hello")
        channel["write"](" ")
        channel["write"]("World")
        channel["checkpoint"]()
        self.assertEqual(channel["read"](), "Hello World")
    
    def test_ephemeral_channel(self):
        channel = create_ephemeral_channel()
        
        self.assertIsNone(channel["read"]())
        self.assertTrue(channel["is_empty"]())
        
        channel["write"]("temporary_data")
        self.assertFalse(channel["is_empty"]())
        self.assertTrue(channel["has_updates"]())
        self.assertEqual(channel["read"](), "temporary_data")
        
        checkpoint_val = channel["checkpoint"]()
        self.assertEqual(checkpoint_val, "temporary_data")
        self.assertIsNone(channel["read"]())
        self.assertTrue(channel["is_empty"]())
        # BSP: has_updates() stays True until consume() is called
        self.assertTrue(channel["has_updates"]())
        channel["consume"]()
        self.assertFalse(channel["has_updates"]())
        
        channel["write"]("another_temp")
        self.assertEqual(channel["read"](), "another_temp")
        
        channel["clear"]()
        self.assertIsNone(channel["read"]())
        self.assertTrue(channel["is_empty"]())
    
    def test_ephemeral_channel_restore(self):
        channel = create_ephemeral_channel()
        
        channel["write"]("some_value")
        channel["restore"]("ignored_value")
        
        self.assertIsNone(channel["read"]())
        self.assertTrue(channel["is_empty"]())
    
    def test_channel_type_field(self):
        last_value = create_last_value_channel()
        topic = create_topic_channel()
        binary_op = create_binary_operator_channel(max)
        ephemeral = create_ephemeral_channel()
        accumulator = create_accumulator_channel(lambda a, b: a + b, initial_value=0)
        
        self.assertEqual(last_value["type"], "LastValue")
        self.assertEqual(topic["type"], "Topic")
        self.assertEqual(binary_op["type"], "BinaryOperator")
        self.assertEqual(ephemeral["type"], "Ephemeral")
        self.assertEqual(accumulator["type"], "Accumulator")
    
    def test_accumulator_channel_sum(self):
        """Test Accumulator channel with sum operator - aggregates then resets."""
        channel = create_accumulator_channel(lambda a, b: a + b, initial_value=0)
        
        # Initial state
        self.assertEqual(channel["read"](), 0)
        self.assertFalse(channel["has_updates"]())
        
        # Write values
        channel["write"](10)
        channel["write"](20)
        channel["write"](30)
        self.assertTrue(channel["has_updates"]())
        # Before checkpoint, read returns the committed value (initial)
        self.assertEqual(channel["read"](), 0)
        
        # Checkpoint aggregates and commits
        checkpoint_val = channel["checkpoint"]()
        self.assertEqual(checkpoint_val, 60)  # 0 + 10 + 20 + 30
        self.assertEqual(channel["read"](), 60)
        
        # Consume to clear has_updates flag
        channel["consume"]()
        self.assertFalse(channel["has_updates"]())
        
        # KEY DIFFERENCE from BinaryOperator: next checkpoint resets to initial_value
        # if no new writes occurred
        checkpoint_val2 = channel["checkpoint"]()
        self.assertEqual(checkpoint_val2, 0)  # Resets to initial_value!
        self.assertEqual(channel["read"](), 0)
    
    def test_accumulator_channel_multiple_supersteps(self):
        """Test Accumulator across multiple supersteps - simulates BSP iterations."""
        channel = create_accumulator_channel(lambda a, b: a + b, initial_value=0.0)
        
        # Superstep 1: writes from multiple sources
        channel["write"](0.1)
        channel["write"](0.2)
        channel["write"](0.3)
        channel["checkpoint"]()
        self.assertAlmostEqual(channel["read"](), 0.6, places=10)
        channel["consume"]()
        
        # Superstep 2: new writes only
        channel["write"](0.5)
        channel["checkpoint"]()
        self.assertAlmostEqual(channel["read"](), 0.5, places=10)  # Not 1.1!
        channel["consume"]()
        
        # Superstep 3: no writes - should reset to initial
        channel["checkpoint"]()
        self.assertEqual(channel["read"](), 0.0)
    
    def test_accumulator_channel_max_operator(self):
        """Test Accumulator with max operator."""
        channel = create_accumulator_channel(max, initial_value=float('-inf'))
        
        channel["write"](5)
        channel["write"](15)
        channel["write"](10)
        channel["checkpoint"]()
        self.assertEqual(channel["read"](), 15)
        channel["consume"]()
        
        # Next superstep with different values
        channel["write"](8)
        channel["write"](3)
        channel["checkpoint"]()
        self.assertEqual(channel["read"](), 8)  # Only considers this superstep's writes
    
    def test_accumulator_channel_state_management(self):
        """Test get_state and set_state for checkpointing."""
        channel = create_accumulator_channel(lambda a, b: a + b, initial_value=0)
        
        channel["write"](100)
        channel["checkpoint"]()
        
        # Get state
        state = channel["get_state"]()
        self.assertEqual(state["value"], 100)
        self.assertEqual(state["initial_value"], 0)
        self.assertEqual(state["pending_values"], [])
        
        # Create new channel and restore state
        channel2 = create_accumulator_channel(lambda a, b: a + b, initial_value=0)
        channel2["set_state"](state)
        self.assertEqual(channel2["read"](), 100)
    
    def test_accumulator_channel_clear_and_restore(self):
        """Test clear and restore operations."""
        channel = create_accumulator_channel(lambda a, b: a + b, initial_value=5)
        
        channel["write"](10)
        channel["checkpoint"]()
        self.assertEqual(channel["read"](), 15)
        
        # Clear resets to initial value
        channel["clear"]()
        self.assertEqual(channel["read"](), 5)
        self.assertFalse(channel["has_updates"]())
        
        # Restore sets a specific value
        channel["restore"](42)
        self.assertEqual(channel["read"](), 42)
    
    def test_accumulator_channel_is_empty(self):
        """Test is_empty for Accumulator channel."""
        channel = create_accumulator_channel(lambda a, b: a + b, initial_value=None)
        
        self.assertTrue(channel["is_empty"]())
        
        channel["write"](10)
        self.assertFalse(channel["is_empty"]())
        
        channel["checkpoint"]()
        self.assertFalse(channel["is_empty"]())  # Has committed value
        
        # After another checkpoint with no writes, resets to None
        channel["checkpoint"]()
        self.assertTrue(channel["is_empty"]())


if __name__ == "__main__":
    unittest.main()