"""
DISTRIBUTED WORD COUNT STRESS TEST

Tests Pregel with many parallel nodes and fan-out/fan-in patterns.
Tests:
- Large number of parallel workers (mapper nodes)
- Fan-out from single source to many workers
- Fan-in aggregation from many workers to single reducer
- Topic channels accumulating many messages
- BinaryOperator for final aggregation

Pipeline:
    SOURCE (1 node)
        |
        v (fan-out to N mappers)
    MAPPER_1, MAPPER_2, ..., MAPPER_N (parallel)
        |
        v (fan-in to aggregator)
    COMBINER (aggregates partial counts)
        |
        v
    FINALIZER (produces final output)

Usage:
    python samples/wordcount_stress_example.py
"""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from DagBi.pregel_core import Pregel
import hashlib
import time


# Configuration
NUM_MAPPERS = 8
CHUNK_SIZE = 100  # words per mapper

# Sample text for word counting (repeated to create volume)
SAMPLE_TEXT = """
The Pregel computational model is a powerful framework for large-scale graph processing.
It uses the Bulk Synchronous Parallel model where computation proceeds in supersteps.
Each vertex processes messages from the previous superstep and sends messages for the next.
The model is simple yet powerful enough to express many graph algorithms efficiently.
Vertices can vote to halt when they have no more work, reducing computation.
The framework handles distribution, fault tolerance, and synchronization automatically.
This allows developers to focus on the algorithm rather than distributed systems concerns.
Pregel has inspired many similar systems including Apache Giraph and GraphX.
The think like a vertex paradigm makes it easy to reason about graph computations.
Message passing between vertices is the primary communication mechanism in Pregel.
""" * 20  # Repeat to create more data


def create_wordcount_engine(text, num_mappers=NUM_MAPPERS, debug=True, parallel=True):
    """Create a Pregel engine for distributed word counting."""
    engine = Pregel(debug=debug, parallel=parallel, max_workers=num_mappers + 2)
    
    # Split text into chunks for mappers
    words = text.lower().split()
    # Remove punctuation
    words = [''.join(c for c in w if c.isalnum()) for w in words]
    words = [w for w in words if w]  # Remove empty strings
    
    chunk_size = len(words) // num_mappers
    chunks = []
    for i in range(num_mappers):
        start = i * chunk_size
        end = start + chunk_size if i < num_mappers - 1 else len(words)
        chunks.append(words[start:end])
    
    # Channels
    engine.add_channel("source_trigger", "LastValue", initial_value=True)
    
    # Each mapper has an input channel
    for i in range(num_mappers):
        engine.add_channel(f"mapper_{i}_input", "LastValue")
    
    # Aggregator collects partial counts from all mappers
    engine.add_channel("partial_counts", "Topic")
    
    # Final aggregated counts
    engine.add_channel("final_counts", "LastValue")
    
    # Word count aggregator (total words processed)
    engine.add_channel("total_words", "BinaryOperator", operator=lambda a, b: a + b, initial_value=0)
    
    # Completion tracking
    engine.add_channel("mappers_done", "BinaryOperator", operator=lambda a, b: a + b, initial_value=0)
    engine.add_channel("complete", "LastValue", initial_value=False)
    
    # Source node: distributes chunks to mappers
    mapper_outputs = {f"chunk_{i}": f"mapper_{i}_input" for i in range(num_mappers)}
    
    @engine.add_to_registry(
        "source",
        subscribe_to=["source_trigger"],
        write_to=mapper_outputs
    )
    def distribute_chunks(inputs):
        """Distribute text chunks to mapper nodes."""
        if not inputs.get("source_trigger"):
            return None
        
        print(f"\033[36m[SOURCE] Distributing {len(words)} words to {num_mappers} mappers\033[0m")
        
        outputs = {}
        for i, chunk in enumerate(chunks):
            outputs[f"chunk_{i}"] = chunk
            print(f"\033[36m[SOURCE] Mapper {i}: {len(chunk)} words\033[0m")
        
        return outputs
    
    # Create mapper nodes
    for mapper_id in range(num_mappers):
        @engine.add_to_registry(
            f"mapper_{mapper_id}",
            subscribe_to=[f"mapper_{mapper_id}_input"],
            write_to={"counts": "partial_counts", "num_words": "total_words", "done": "mappers_done"}
        )
        def map_words(inputs, mid=mapper_id):
            """Count words in a chunk."""
            chunk = inputs.get(f"mapper_{mid}_input")
            if chunk is None:
                return None
            
            # Count words
            word_counts = {}
            for word in chunk:
                word_counts[word] = word_counts.get(word, 0) + 1
            
            print(f"\033[33m[MAPPER-{mid}] Processed {len(chunk)} words, {len(word_counts)} unique\033[0m")
            
            return {
                "counts": word_counts,
                "num_words": len(chunk),
                "done": 1
            }
    
    # Combiner node: aggregates partial counts
    @engine.add_to_registry(
        "combiner",
        subscribe_to=["partial_counts", "mappers_done"],
        write_to={"final": "final_counts"}
    )
    def combine_counts(inputs):
        """Combine partial word counts from all mappers."""
        partial_counts_list = inputs.get("partial_counts", [])
        mappers_done = inputs.get("mappers_done", 0)
        
        if not partial_counts_list:
            return None
        
        # Wait for all mappers
        if mappers_done < num_mappers:
            print(f"\033[35m[COMBINER] Waiting for mappers ({mappers_done}/{num_mappers})\033[0m")
            return None
        
        # Merge all partial counts
        final_counts = {}
        for partial in partial_counts_list:
            for word, count in partial.items():
                final_counts[word] = final_counts.get(word, 0) + count
        
        print(f"\033[32m[COMBINER] Combined {len(partial_counts_list)} partial results\033[0m")
        print(f"\033[32m[COMBINER] Total unique words: {len(final_counts)}\033[0m")
        
        return {"final": final_counts}
    
    # Finalizer: produces sorted output
    @engine.add_to_registry(
        "finalizer",
        subscribe_to=["final_counts", "total_words"],
        write_to={"done": "complete"}
    )
    def finalize_output(inputs):
        """Produce final sorted word count output."""
        counts = inputs.get("final_counts")
        total_words = inputs.get("total_words", 0)
        
        if counts is None:
            return None
        
        # Sort by count (descending)
        sorted_counts = sorted(counts.items(), key=lambda x: -x[1])
        
        print(f"\033[32m[FINALIZER] Word count complete!\033[0m")
        print(f"\033[32m[FINALIZER] Total words processed: {total_words}\033[0m")
        print(f"\033[32m[FINALIZER] Unique words: {len(counts)}\033[0m")
        print(f"\033[32m[FINALIZER] Top 10 words:\033[0m")
        for word, count in sorted_counts[:10]:
            print(f"\033[32m    {word}: {count}\033[0m")
        
        return {"done": True}
    
    return engine, words


def run_wordcount(num_mappers=NUM_MAPPERS, debug=True, parallel=True):
    """Run distributed word count."""
    print("\033[36m" + "=" * 60 + "\033[0m")
    print("\033[36m    DISTRIBUTED WORD COUNT STRESS TEST\033[0m")
    print(f"\033[36m    Mappers: {num_mappers}, Parallel: {parallel}\033[0m")
    print("\033[36m" + "=" * 60 + "\033[0m\n")
    
    engine, words = create_wordcount_engine(
        SAMPLE_TEXT, 
        num_mappers=num_mappers,
        debug=debug, 
        parallel=parallel
    )
    
    start_time = time.time()
    final_state = engine.run(max_supersteps=20)
    elapsed = time.time() - start_time
    
    print("\n\033[36m" + "=" * 60 + "\033[0m")
    print("\033[32m    WORD COUNT COMPLETED\033[0m")
    print(f"\033[35m    Supersteps: {engine.superstep}\033[0m")
    print(f"\033[35m    Time: {elapsed:.3f}s\033[0m")
    print(f"\033[35m    Complete: {final_state.get('complete', False)}\033[0m")
    print("\033[36m" + "=" * 60 + "\033[0m")
    
    return final_state, engine.superstep, elapsed, words


def verify_wordcount(final_state, original_words):
    """Verify word count results."""
    print("\n\033[36m--- VERIFICATION ---\033[0m")
    
    # Check completion
    assert final_state.get("complete") == True, "Should be marked complete"
    print("[OK] Marked complete")
    
    # Check total words
    total_words = final_state.get("total_words", 0)
    expected_total = len(original_words)
    assert total_words == expected_total, f"Total words mismatch: {total_words} vs {expected_total}"
    print(f"[OK] Total words: {total_words}")
    
    # Check final counts exist
    final_counts = final_state.get("final_counts")
    assert final_counts is not None, "Final counts should exist"
    assert len(final_counts) > 0, "Should have some word counts"
    print(f"[OK] Unique words: {len(final_counts)}")
    
    # Verify counts sum to total
    count_sum = sum(final_counts.values())
    assert count_sum == expected_total, f"Counts sum mismatch: {count_sum} vs {expected_total}"
    print(f"[OK] Counts sum correctly: {count_sum}")
    
    # Spot check a known word
    if "pregel" in final_counts:
        # "pregel" should appear multiple times in our sample
        assert final_counts["pregel"] >= 20, f"Expected many 'pregel' occurrences"
        print(f"[OK] Word 'pregel' count: {final_counts['pregel']}")
    
    print("\n\033[32m[VERIFICATION PASSED]\033[0m")


def stress_test():
    """Run stress tests with different configurations."""
    print("\n" + "=" * 70)
    print("  STRESS TEST: SCALING MAPPERS")
    print("=" * 70 + "\n")
    
    results = []
    
    for num_mappers in [2, 4, 8, 16]:
        print(f"\n--- Testing with {num_mappers} mappers ---\n")
        final_state, supersteps, elapsed, words = run_wordcount(
            num_mappers=num_mappers,
            debug=False,
            parallel=True
        )
        results.append({
            "mappers": num_mappers,
            "supersteps": supersteps,
            "time": elapsed,
            "words": len(words)
        })
        verify_wordcount(final_state, words)
    
    print("\n" + "=" * 70)
    print("  STRESS TEST RESULTS")
    print("=" * 70)
    print(f"\n{'Mappers':<10} {'Supersteps':<12} {'Time (s)':<12} {'Words':<10}")
    print("-" * 44)
    for r in results:
        print(f"{r['mappers']:<10} {r['supersteps']:<12} {r['time']:<12.3f} {r['words']:<10}")
    
    print("\n\033[32m" + "=" * 70 + "\033[0m")
    print("\033[32m  ALL STRESS TESTS PASSED\033[0m")
    print("\033[32m" + "=" * 70 + "\033[0m")


def main():
    # Run with debug output first
    print("\n" + "=" * 70)
    print("  RUNNING WORD COUNT WITH DEBUG OUTPUT")
    print("=" * 70 + "\n")
    
    final_state, supersteps, elapsed, words = run_wordcount(
        num_mappers=4, 
        debug=True, 
        parallel=True
    )
    verify_wordcount(final_state, words)
    
    # Run stress tests
    stress_test()


if __name__ == "__main__":
    main()
