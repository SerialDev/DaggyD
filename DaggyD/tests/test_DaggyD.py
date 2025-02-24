import unittest
from DaggyD import example_function

class TestDaggyD(unittest.TestCase):
    def test_example_function(self):
        self.assertEqual(example_function(), "Hello from DaggyD!")

if __name__ == "__main__":
    unittest.main()
