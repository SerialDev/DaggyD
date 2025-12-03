"""
DOC → IMPLEMENTATION PLAN ENGINE

A complete example demonstrating a Pregel-based document processing pipeline
that converts documentation into structured implementation plans.

Pregel automatically handles parallel execution of independent nodes within
each superstep - that's the whole point of the BSP model. You just define
the graph topology and let Pregel manage the parallelism.

Processing Graph:
    DOCUMENT_SOURCE → PARSE_CONTENT (P1) + EXTRACT_CODE (P2)  [parallel]
                    → STRUCTURE_MODEL (M1) + MP_ANALYZER (M2) [parallel]
                    → MERGE_PLAN (R3)
                    → BROADCAST_PLAN (B4)
                    → CODE_GEN_L (U5) + CODE_GEN_R (U6)       [parallel]
                    → VALIDATOR (C7)
                    → FEEDBACK (F8)

Usage:
    python samples/doc_plan_engine_example.py
"""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from DaggyD.pregel_core import Pregel


def parse_content(raw_docs):
    import re
    import hashlib
    print("\033[36m[P1:PARSE] Starting document parsing\033[0m")
    
    result = {
        "sections": [],
        "entities": [],
        "tokens": [],
        "ambiguities": [],
        "metadata": {}
    }
    
    docs_list = [raw_docs] if isinstance(raw_docs, str) else raw_docs if isinstance(raw_docs, list) else [str(raw_docs)]
    
    for doc_idx, doc in enumerate(docs_list):
        doc_id = hashlib.md5(doc.encode()).hexdigest()[:8]
        
        for match in re.finditer(r'^(#{1,6})\s+(.+)$', doc, re.MULTILINE):
            result["sections"].append({
                "level": len(match.group(1)),
                "title": match.group(2).strip(),
                "start_pos": match.start(),
                "doc_id": doc_id
            })
        
        entity_patterns = {
            "function": r'\bdef\s+(\w+)\s*\(',
            "class": r'\bclass\s+(\w+)\s*[:\(]',
            "endpoint": r'(?:GET|POST|PUT|DELETE|PATCH)\s+[\'"]?(/[\w/{}]+)',
            "variable": r'\b([A-Z_][A-Z0-9_]+)\s*=',
            "parameter": r'@param\s+(\w+)',
        }
        
        for entity_type, pattern in entity_patterns.items():
            for match in re.finditer(pattern, doc):
                result["entities"].append({
                    "type": entity_type,
                    "name": match.group(1),
                    "doc_id": doc_id
                })
        
        for pattern, desc in [(r'\bTODO\b', "incomplete"), (r'\bmay\b', "uncertain"), (r'\boptional\b', "optional")]:
            for match in re.finditer(pattern, doc, re.IGNORECASE):
                ctx_start, ctx_end = max(0, match.start() - 40), min(len(doc), match.end() + 40)
                result["ambiguities"].append({"type": desc, "context": doc[ctx_start:ctx_end].strip()})
        
        result["metadata"][doc_id] = {"length": len(doc), "lines": doc.count('\n') + 1}
    
    print(f"\033[32m[P1:PARSE] Done: {len(result['sections'])} sections, {len(result['entities'])} entities\033[0m")
    return result


def extract_code(raw_docs):
    import re
    print("\033[36m[P2:CODE] Starting code extraction\033[0m")
    
    result = {"snippets": [], "patterns": [], "concurrency_hints": [], "constraints": []}
    docs_list = [raw_docs] if isinstance(raw_docs, str) else raw_docs if isinstance(raw_docs, list) else [str(raw_docs)]
    
    for doc in docs_list:
        for match in re.finditer(r'```(\w*)\n(.*?)```', doc, re.DOTALL):
            result["snippets"].append({"language": match.group(1) or "unknown", "code": match.group(2).strip()})
        
        pattern_checks = {
            "iteration": [r'\bfor\s+\w+\s+in\b', r'\bwhile\b'],
            "parallel": [r'\bPool\b', r'\bProcess\b', r'\bmultiprocessing\b'],
            "io": [r'\bopen\(', r'\bread\(', r'\bwrite\('],
        }
        for ptype, patterns in pattern_checks.items():
            for p in patterns:
                if re.search(p, doc):
                    result["patterns"].append({"type": ptype, "pattern": p})
        
        concurrency_checks = [
            (r'\bshared\s+state\b', "shared_state", "high"),
            (r'\block\b', "locking", "medium"),
            (r'\bpool\b', "pool", "low"),
            (r'\bworker\b', "worker", "low"),
            (r'\bparallel\b', "parallel", "medium"),
        ]
        for pattern, hint_type, risk in concurrency_checks:
            if re.search(pattern, doc, re.IGNORECASE):
                result["concurrency_hints"].append({"type": hint_type, "risk": risk})
        
        for pattern, ctype in [(r'must\s+be', "requirement"), (r'maximum\s+\d+', "limit"), (r'timeout', "timeout")]:
            for m in re.finditer(pattern, doc, re.IGNORECASE):
                result["constraints"].append({"type": ctype, "context": doc[max(0,m.start()-20):m.end()+20]})
    
    print(f"\033[32m[P2:CODE] Done: {len(result['snippets'])} snippets, {len(result['patterns'])} patterns\033[0m")
    return result


def build_architecture(parsed_doc):
    print("\033[36m[M1:ARCH] Building architecture model\033[0m")
    
    result = {
        "phases": [
            {"name": "initialization", "order": 0},
            {"name": "input_processing", "order": 1},
            {"name": "transformation", "order": 2},
            {"name": "aggregation", "order": 3},
            {"name": "output", "order": 4}
        ],
        "modules": [],
        "style": {"no_classes": True, "imports_in_functions": True, "procedural": True}
    }
    
    sections = parsed_doc.get("sections", [])
    for s in sections:
        if s["level"] <= 2:
            mod_name = ''.join(c for c in s["title"].lower().replace(" ", "_") if c.isalnum() or c == '_')
            if mod_name:
                result["modules"].append({"name": mod_name, "functions": []})
    
    functions = [e for e in parsed_doc.get("entities", []) if e["type"] == "function"]
    for f in functions:
        if result["modules"]:
            result["modules"][0]["functions"].append(f["name"])
        else:
            result["modules"].append({"name": "core", "functions": [f["name"]]})
    
    print(f"\033[32m[M1:ARCH] Done: {len(result['phases'])} phases, {len(result['modules'])} modules\033[0m")
    return result


def analyze_multiprocessing(code_fragments):
    import multiprocessing
    print("\033[36m[M2:MP] Analyzing multiprocessing opportunities\033[0m")
    
    patterns = code_fragments.get("patterns", [])
    hints = code_fragments.get("concurrency_hints", [])
    
    has_iteration = any(p["type"] == "iteration" for p in patterns)
    has_parallel = any(p["type"] == "parallel" for p in patterns)
    has_io = any(p["type"] == "io" for p in patterns)
    high_risk = any(h["risk"] == "high" for h in hints)
    
    if has_parallel or has_iteration:
        pattern = "process_pool_with_manager" if high_risk else "process_pool"
    elif has_io:
        pattern = "thread_pool"
    else:
        pattern = "sequential"
    
    cpu_count = multiprocessing.cpu_count()
    result = {
        "recommended_pattern": pattern,
        "parallelizable": has_iteration or has_parallel,
        "risks": [h for h in hints if h["risk"] in ("high", "medium")],
        "pool_config": {"workers": min(cpu_count, 8), "max_workers": cpu_count},
        "worker_contract": {"stateless": True, "serializable": True, "idempotent": True}
    }
    
    print(f"\033[32m[M2:MP] Done: pattern={pattern}, {len(result['risks'])} risks\033[0m")
    return result


def merge_plan(architecture, mp_strategy):
    print("\033[36m[R3:MERGE] Merging architecture and MP strategy\033[0m")
    
    phases = architecture.get("phases", [])
    workers = mp_strategy.get("pool_config", {}).get("workers", 4)
    parallelizable = mp_strategy.get("parallelizable", False)
    
    result = {
        "overview": {
            "phases": len(phases),
            "modules": len(architecture.get("modules", [])),
            "pattern": mp_strategy.get("recommended_pattern")
        },
        "architecture": architecture,
        "concurrency": {
            "pattern": mp_strategy.get("recommended_pattern"),
            "workers": workers,
            "risks": mp_strategy.get("risks", [])
        },
        "pipeline": [
            {"phase": p["name"], "order": p["order"], "parallel": parallelizable and p["name"] in ("transformation", "aggregation")}
            for p in phases
        ],
        "execution_order": [
            {"step": 1, "action": "initialize"},
            {"step": 2, "action": "setup_workers"},
            {"step": 3, "action": "process"},
            {"step": 4, "action": "aggregate"},
            {"step": 5, "action": "output"}
        ]
    }
    
    print(f"\033[32m[R3:MERGE] Done: {len(result['pipeline'])} stages\033[0m")
    return result


def broadcast_plan(plan):
    print("\033[36m[B4:BROADCAST] Broadcasting to generators\033[0m")
    
    result = {
        "for_main_codegen": {
            "modules": plan.get("architecture", {}).get("modules", []),
            "phases": plan.get("architecture", {}).get("phases", []),
            "style": plan.get("architecture", {}).get("style", {})
        },
        "for_worker_codegen": {
            "concurrency": plan.get("concurrency", {}),
            "worker_contract": {"stateless": True, "serializable": True}
        },
        "for_validator": {
            "pipeline": plan.get("pipeline", []),
            "execution_order": plan.get("execution_order", [])
        }
    }
    
    print(f"\033[32m[B4:BROADCAST] Done: distributed to 3 targets\033[0m")
    return result


def generate_main_code(broadcast):
    print("\033[36m[U5:MAIN] Generating main code\033[0m")
    
    config = broadcast.get("for_main_codegen", {})
    modules = config.get("modules", [])
    phases = config.get("phases", [])
    
    code_parts = []
    
    lines = ["def run_pipeline(data, config=None):", "    import json", "    ", '    print("\\033[36m[START]\\033[0m")', "    results = {}"]
    for p in phases:
        lines.append(f'    print("\\033[35m[PHASE] {p["name"]}\\033[0m")')
        lines.append(f"    results['{p['name']}'] = process_{p['name']}(data)")
    lines.extend(['    print("\\033[32m[DONE]\\033[0m")', "    return results"])
    code_parts.append({"name": "main_pipeline", "type": "entry", "code": '\n'.join(lines)})
    
    for mod in modules:
        name = mod.get("name", "module")
        funcs = mod.get("functions", [])
        mlines = [f"def process_{name}(data):", f'    print("\\033[33m[{name.upper()}]\\033[0m")', "    result = {}"]
        for fn in funcs[:3]:
            mlines.append(f"    result['{fn}'] = None  # TODO")
        mlines.extend(["    return result"])
        code_parts.append({"name": name, "type": "module", "code": '\n'.join(mlines)})
    
    code_parts.append({
        "name": "logging",
        "type": "utility", 
        "code": 'def log(msg, color=35): print(f"\\033[{color}m{msg}\\033[0m")'
    })
    
    result = {"sections": code_parts, "total_lines": sum(c["code"].count('\n') + 1 for c in code_parts)}
    print(f"\033[32m[U5:MAIN] Done: {len(code_parts)} sections, {result['total_lines']} lines\033[0m")
    return result


def generate_worker_code(broadcast):
    print("\033[36m[U6:WORKER] Generating worker code\033[0m")
    
    config = broadcast.get("for_worker_codegen", {})
    concurrency = config.get("concurrency", {})
    workers = concurrency.get("workers", 4)
    pattern = concurrency.get("pattern", "process_pool")
    
    code_parts = []
    
    if "thread" in pattern:
        pool_code = f'''def create_pool(n={workers}):
    from concurrent.futures import ThreadPoolExecutor
    return ThreadPoolExecutor(max_workers=n)'''
    else:
        pool_code = f'''def create_pool(n={workers}):
    import multiprocessing
    return multiprocessing.Pool(processes=n)'''
    code_parts.append({"name": "pool", "type": "concurrency", "code": pool_code})
    
    code_parts.append({
        "name": "worker",
        "type": "concurrency",
        "code": '''def worker(item):
    try:
        return {"input": item, "output": item, "error": None}
    except Exception as e:
        return {"input": item, "output": None, "error": str(e)}'''
    })
    
    code_parts.append({
        "name": "sync",
        "type": "concurrency",
        "code": '''def create_shared_state():
    import multiprocessing
    m = multiprocessing.Manager()
    return {"results": m.list(), "lock": m.Lock()}'''
    })
    
    code_parts.append({
        "name": "retry",
        "type": "utility",
        "code": '''def with_retry(fn, retries=3):
    def wrapper(*a, **kw):
        for i in range(retries):
            try: return fn(*a, **kw)
            except Exception as e:
                if i == retries - 1: raise
    return wrapper'''
    })
    
    result = {"sections": code_parts, "total_lines": sum(c["code"].count('\n') + 1 for c in code_parts)}
    print(f"\033[32m[U6:WORKER] Done: {len(code_parts)} sections\033[0m")
    return result


def validate_plan(main_code, worker_code, plan):
    print("\033[36m[C7:VALIDATE] Validating plan\033[0m")
    
    result = {"valid": True, "errors": [], "warnings": [], "suggestions": [], "checks_passed": [], "checks_failed": []}
    
    if plan.get("pipeline"):
        orders = [p.get("order", -1) for p in plan["pipeline"]]
        if orders == sorted(orders):
            result["checks_passed"].append("pipeline_order")
        else:
            result["checks_failed"].append("pipeline_order")
            result["errors"].append("Pipeline phases not in order")
            result["valid"] = False
    
    if main_code.get("sections") and worker_code.get("sections"):
        result["checks_passed"].append("code_completeness")
    else:
        result["checks_failed"].append("code_completeness")
        result["errors"].append("Missing code sections")
        result["valid"] = False
    
    all_code = ' '.join(s.get("code", "") for s in main_code.get("sections", []) + worker_code.get("sections", []))
    if "class " not in all_code:
        result["checks_passed"].append("procedural_style")
    else:
        result["warnings"].append("Found class definitions - procedural style preferred")
    
    if "global " not in all_code:
        result["checks_passed"].append("no_globals")
    else:
        result["warnings"].append("Found global variables")
    
    result["checks_passed"].append("concurrency_safety")
    
    if plan.get("concurrency", {}).get("pattern") == "sequential":
        result["suggestions"].append("Consider parallel processing for better performance")
    
    status = "PASSED" if result["valid"] else "FAILED"
    color = 32 if result["valid"] else 31
    print(f"\033[{color}m[C7:VALIDATE] {status}: {len(result['checks_passed'])} passed, {len(result['checks_failed'])} failed\033[0m")
    return result


def generate_feedback(validation):
    print("\033[36m[F8:FEEDBACK] Generating feedback\033[0m")
    
    is_valid = validation.get("valid", False)
    
    result = {
        "status": "complete" if is_valid else "needs_revision",
        "summary": {
            "valid": is_valid,
            "errors": len(validation.get("errors", [])),
            "warnings": len(validation.get("warnings", [])),
            "suggestions": len(validation.get("suggestions", []))
        },
        "action_items": []
    }
    
    for e in validation.get("errors", []):
        result["action_items"].append({"type": "error", "action": f"Fix: {e}", "priority": "high"})
    for w in validation.get("warnings", []):
        result["action_items"].append({"type": "warning", "action": f"Review: {w}", "priority": "medium"})
    for s in validation.get("suggestions", []):
        result["action_items"].append({"type": "suggestion", "action": s, "priority": "low"})
    
    status = "COMPLETE" if is_valid else "NEEDS REVISION"
    color = 32 if is_valid else 33
    print(f"\033[{color}m[F8:FEEDBACK] {status}: {len(result['action_items'])} action items\033[0m")
    return result


def create_doc_plan_engine(debug=True, parallel=True, max_workers=None):
    engine = Pregel(debug=debug, parallel=parallel, max_workers=max_workers)
    
    engine.add_channel("raw_docs", "LastValue")
    engine.add_channel("parsed_doc", "LastValue")
    engine.add_channel("code_fragments", "LastValue")
    engine.add_channel("architecture", "LastValue")
    engine.add_channel("mp_strategy", "LastValue")
    engine.add_channel("merged_plan", "LastValue")
    engine.add_channel("broadcast", "LastValue")
    engine.add_channel("main_code", "LastValue")
    engine.add_channel("worker_code", "LastValue")
    engine.add_channel("validation", "LastValue")
    engine.add_channel("feedback", "LastValue")
    engine.add_channel("complete", "LastValue", initial_value=False)
    
    # Pregel nodes: return output to send messages, return None to "vote to halt"
    # Nodes are automatically reactivated when they receive new messages
    
    @engine.add_to_registry("P1_parse", subscribe_to=["raw_docs"], write_to={"out": "parsed_doc"})
    def p1(inputs):
        """Parse raw documentation - runs when raw_docs has new message"""
        raw = inputs.get("raw_docs")
        return {"out": parse_content(raw)}
    
    @engine.add_to_registry("P2_extract", subscribe_to=["raw_docs"], write_to={"out": "code_fragments"})
    def p2(inputs):
        """Extract code fragments - runs in parallel with P1"""
        raw = inputs.get("raw_docs")
        return {"out": extract_code(raw)}
    
    @engine.add_to_registry("M1_architecture", subscribe_to=["parsed_doc"], write_to={"out": "architecture"})
    def m1(inputs):
        """Build architecture model from parsed doc"""
        parsed = inputs.get("parsed_doc")
        return {"out": build_architecture(parsed)}
    
    @engine.add_to_registry("M2_multiprocessing", subscribe_to=["code_fragments"], write_to={"out": "mp_strategy"})
    def m2(inputs):
        """Analyze multiprocessing opportunities - runs in parallel with M1"""
        code = inputs.get("code_fragments")
        return {"out": analyze_multiprocessing(code)}
    
    @engine.add_to_registry("R3_merge", subscribe_to=["architecture", "mp_strategy"], write_to={"out": "merged_plan"})
    def r3(inputs):
        """Merge architecture and MP strategy into unified plan"""
        arch = inputs.get("architecture")
        mp = inputs.get("mp_strategy")
        return {"out": merge_plan(arch, mp)}
    
    @engine.add_to_registry("B4_broadcast", subscribe_to=["merged_plan"], write_to={"out": "broadcast"})
    def b4(inputs):
        """Broadcast plan to code generators"""
        plan = inputs.get("merged_plan")
        return {"out": broadcast_plan(plan)}
    
    @engine.add_to_registry("U5_main_codegen", subscribe_to=["broadcast"], write_to={"out": "main_code"})
    def u5(inputs):
        """Generate main code - runs in parallel with U6"""
        bc = inputs.get("broadcast")
        return {"out": generate_main_code(bc)}
    
    @engine.add_to_registry("U6_worker_codegen", subscribe_to=["broadcast"], write_to={"out": "worker_code"})
    def u6(inputs):
        """Generate worker code - runs in parallel with U5"""
        bc = inputs.get("broadcast")
        return {"out": generate_worker_code(bc)}
    
    @engine.add_to_registry("C7_validate", subscribe_to=["main_code", "worker_code", "merged_plan"], write_to={"out": "validation"})
    def c7(inputs):
        """Validate the complete plan and generated code"""
        main = inputs.get("main_code")
        worker = inputs.get("worker_code")
        plan = inputs.get("merged_plan")
        return {"out": validate_plan(main, worker, plan)}
    
    @engine.add_to_registry("F8_feedback", subscribe_to=["validation"], write_to={"out": "feedback", "done": "complete"})
    def f8(inputs):
        """Generate final feedback - terminal node"""
        val = inputs.get("validation")
        return {"out": generate_feedback(val), "done": True}
    
    return engine


def run_doc_plan_engine(docs, debug=True, parallel=True, max_workers=None, max_supersteps=20):
    print("\033[36m" + "=" * 60 + "\033[0m")
    print("\033[36m    DOC → IMPLEMENTATION PLAN ENGINE\033[0m")
    print("\033[36m    Pregel handles parallelism automatically via BSP model\033[0m")
    print("\033[36m" + "=" * 60 + "\033[0m\n")
    
    engine = create_doc_plan_engine(debug=debug, parallel=parallel, max_workers=max_workers)
    
    engine.channels["raw_docs"]["write"](docs)
    engine.channels["raw_docs"]["checkpoint"]()
    
    final_state = engine.run(max_supersteps=max_supersteps)
    
    result = {
        "parsed_doc": final_state.get("parsed_doc"),
        "code_fragments": final_state.get("code_fragments"),
        "architecture": final_state.get("architecture"),
        "mp_strategy": final_state.get("mp_strategy"),
        "implementation_plan": final_state.get("merged_plan"),
        "generated_code": {
            "main": final_state.get("main_code"),
            "workers": final_state.get("worker_code")
        },
        "validation": final_state.get("validation"),
        "feedback": final_state.get("feedback"),
        "complete": final_state.get("complete"),
        "supersteps": engine.superstep
    }
    
    print("\n\033[36m" + "=" * 60 + "\033[0m")
    print("\033[32m    ENGINE COMPLETED\033[0m")
    print(f"\033[35m    Supersteps: {result['supersteps']}\033[0m")
    v = result.get('validation', {})
    print(f"\033[35m    Valid: {v.get('valid') if v else 'N/A'}\033[0m")
    print("\033[36m" + "=" * 60 + "\033[0m")
    
    return result


def export_plan(result, output_dir):
    import json
    
    os.makedirs(output_dir, exist_ok=True)
    paths = {}
    
    plan_path = os.path.join(output_dir, "implementation_plan.json")
    with open(plan_path, "w") as f:
        json.dump(result.get("implementation_plan", {}), f, indent=2)
    paths["plan"] = plan_path
    print(f"\033[32m[EXPORT] {plan_path}\033[0m")
    
    main_code = result.get("generated_code", {}).get("main", {})
    if main_code:
        main_path = os.path.join(output_dir, "generated_main.py")
        with open(main_path, "w") as f:
            for s in main_code.get("sections", []):
                f.write(f"# === {s.get('name', 'section')} ===\n{s.get('code', '')}\n\n")
        paths["main_code"] = main_path
        print(f"\033[32m[EXPORT] {main_path}\033[0m")
    
    worker_code = result.get("generated_code", {}).get("workers", {})
    if worker_code:
        worker_path = os.path.join(output_dir, "generated_workers.py")
        with open(worker_path, "w") as f:
            for s in worker_code.get("sections", []):
                f.write(f"# === {s.get('name', 'section')} ===\n{s.get('code', '')}\n\n")
        paths["worker_code"] = worker_path
        print(f"\033[32m[EXPORT] {worker_path}\033[0m")
    
    report_path = os.path.join(output_dir, "validation.json")
    with open(report_path, "w") as f:
        json.dump(result.get("validation", {}), f, indent=2)
    paths["report"] = report_path
    print(f"\033[32m[EXPORT] {report_path}\033[0m")
    
    return paths


SAMPLE_DOC = """
# Data Processing API

## Overview
This API processes large datasets using parallel workers.

## Architecture
The system uses a producer-consumer pattern with:
- Input queue for raw data
- Worker pool for transformation
- Output aggregator for results

## Functions

### process_batch
```python
def process_batch(items, n_workers=4):
    import multiprocessing
    
    pool = multiprocessing.Pool(n_workers)
    results = pool.map(transform_item, items)
    pool.close()
    pool.join()
    return results
```

@param items List of items to process
@param n_workers Number of parallel workers

### transform_item
```python
def transform_item(item):
    return item * 2
```

@param item Single item to transform

## Configuration
- MAX_WORKERS = 8
- BATCH_SIZE = 1000
- TIMEOUT_SECONDS = 300

## Error Handling
- Retry count: maximum 3 attempts
- Rate limit: 100 requests per second

## Notes
- Shared state should be avoided
- Workers must be stateless
"""


def main():
    print("\033[36m" + "=" * 70 + "\033[0m")
    print("\033[36m  DOC → IMPLEMENTATION PLAN ENGINE EXAMPLE\033[0m")
    print("\033[36m" + "=" * 70 + "\033[0m")
    print("""
This example demonstrates a Pregel-based document processing pipeline.
Pregel automatically parallelizes independent nodes within each superstep.

Pipeline stages that run in parallel (same superstep):
  - P1 + P2: Parse content and extract code simultaneously  
  - M1 + M2: Build architecture and analyze MP simultaneously
  - U5 + U6: Generate main and worker code simultaneously

Sequential stages (dependencies require ordering):
  - R3: Merge (needs M1 + M2)
  - B4: Broadcast (needs R3)
  - C7: Validate (needs U5 + U6)
  - F8: Feedback (needs C7)
""")
    
    result = run_doc_plan_engine(SAMPLE_DOC, debug=True, parallel=True, max_workers=4)
    
    print("\n\033[36m--- SUMMARY ---\033[0m")
    
    if result.get("parsed_doc"):
        p = result["parsed_doc"]
        print(f"Parsed: {len(p.get('sections', []))} sections, {len(p.get('entities', []))} entities")
    
    if result.get("architecture"):
        a = result["architecture"]
        print(f"Architecture: {len(a.get('phases', []))} phases, {len(a.get('modules', []))} modules")
    
    if result.get("mp_strategy"):
        m = result["mp_strategy"]
        print(f"MP Strategy: {m.get('recommended_pattern')}, {m.get('pool_config', {}).get('workers')} workers")
    
    if result.get("validation"):
        v = result["validation"]
        print(f"Validation: {'PASSED' if v.get('valid') else 'FAILED'}, {len(v.get('checks_passed', []))} checks passed")
    
    if result.get("feedback"):
        f = result["feedback"]
        print(f"Status: {f.get('status')}, {len(f.get('action_items', []))} action items")
    
    import tempfile
    output_dir = tempfile.mkdtemp(prefix="doc_plan_")
    print(f"\n\033[36m--- EXPORTING TO {output_dir} ---\033[0m")
    export_plan(result, output_dir)
    
    print("\n\033[32m" + "=" * 70 + "\033[0m")
    print("\033[32m  EXAMPLE COMPLETED\033[0m")
    print("\033[32m" + "=" * 70 + "\033[0m")


if __name__ == "__main__":
    main()
