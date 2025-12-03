import traceback
from collections import deque


def extract_traceback_info(traceback_str):
    import os
    import re

    lines = traceback_str.strip().split("\n")
    last_trace = None
    error_msg = None

    for i in range(len(lines) - 1, -1, -1):
        if "File " in lines[i] and ", line " in lines[i]:
            last_trace = lines[i]
            error_msg = lines[i + 1] if i + 1 < len(lines) else "Unknown Error"
            break

    if last_trace:
        match = re.search(r'File "(.*?)", line (\d+), in (.*)', last_trace)
        if match:
            file_path, line_number, function_name = match.groups()
            short_path = "/".join(file_path.replace("\\", "/").split("/")[-3:])
            return f"{error_msg.strip()} (File: {short_path}, Line: {line_number}, Function: {function_name})"

    return "Unknown Error"


def stringify_truncated(obj, max_len=500):
    from numpy import ndarray
    from pandas import DataFrame, Series
    from itertools import islice

    def slice_iterable(it, n_head=3, n_tail=2):
        it = iter(it)
        head = list(islice(it, n_head))
        tail = list(it)[-n_tail:] if n_tail else []
        return (
            head + ["..."] + tail
            if len(head) + len(tail) < len(head) + len(tail)
            else head + tail
        )

    if isinstance(obj, (ndarray, Series)):
        obj = obj.flat if isinstance(obj, ndarray) else iter(obj)
    elif isinstance(obj, DataFrame):
        col_list = list(obj.columns)
        col_trunc = (
            col_list[:3] + (["..."] if len(col_list) > 5 else []) + col_list[-2:]
            if len(col_list) > 5
            else col_list
        )

        row_trunc = [
            {col: row[idx] for idx, col in enumerate(col_trunc)}
            for row in islice(obj.itertuples(index=False, name=None), 3)
        ]

        if len(obj) > 5:
            row_trunc.append("...")
            row_trunc.extend(
                [
                    {col: row[idx] for idx, col in enumerate(col_trunc)}
                    for row in islice(
                        obj.itertuples(index=False, name=None), len(obj) - 2, None
                    )
                ]
            )
        obj = row_trunc
    elif isinstance(obj, (set, frozenset)):
        obj = iter(obj)

    if hasattr(obj, "__iter__") and not isinstance(obj, (str, bytes, dict)):
        obj = slice_iterable(obj)

    s = str(obj)
    return s if len(s) <= max_len else s[:max_len] + "..."


class DaggyD:
    def __init__(self):
        self.functions = {}
        self.executed = {}
        self.ready_queue = deque()
        self.output = {}
        print("\033[36m[INIT] DAG Executor initialized\033[0m")

    def add_function(
        self,
        name,
        func,
        input_deps,
        failure_deps,
        input_mapping,
        extra_args_mapping=None,
        extra_kwargs=None,
    ):
        if name in self.functions:
            raise ValueError(f"\033[31mFunction {name} already exists\033[0m")
        extra_args_mapping = extra_args_mapping or {}
        extra_kwargs = extra_kwargs or {}
        print(
            f"\033[36m[REGISTER] {name}: input_deps={stringify_truncated(input_deps)}, failure_deps={failure_deps}, input_mapping={stringify_truncated(input_mapping)}, extra_args_mapping={extra_args_mapping}, extra_kwargs={extra_kwargs}\033[0m"
        )
        self.functions[name] = (
            func,
            input_deps,
            failure_deps,
            input_mapping,
            extra_args_mapping,
            extra_kwargs,
        )

    def add_to_registry(
        self,
        name,
        input_deps,
        failure_deps,
        input_mapping,
        extra_args_mapping=None,
        extra_kwargs=None,
    ):
        def decorator(func):
            self.add_function(
                name,
                func,
                input_deps,
                failure_deps,
                input_mapping,
                extra_args_mapping,
                extra_kwargs,
            )
            return func

        return decorator

    def execute(self, start_name, initial_outputs=None):
        if start_name not in self.functions:
            raise ValueError(f"\033[31mFunction {start_name} not found\033[0m")
        print(f"\033[36m\n[EXECUTION START] {start_name}\033[0m")

        # Initialize all functions to "not started".
        for name in self.functions:
            self.executed[name] = "not started"
            print(f"\033[33m[INIT] {name}: status=not started\033[0m")

        # Preload any initial outputs and mark them as succeeded.
        if initial_outputs:
            for name, output in initial_outputs.items():
                self.output[name] = output
                self.executed[name] = "succeeded"
                print(
                    f"\033[32m[PRELOAD] {name}: output={stringify_truncated(output)}\033[0m"
                )

        # Enqueue the starting function.
        self.ready_queue.append(start_name)
        self.executed[start_name] = "ready"
        print(f"\033[35m[QUEUE] {start_name} added to ready queue\033[0m")

        while self.ready_queue:
            current_name = self.ready_queue.popleft()

            # --- OVERRIDE: Force error_handler to be "ready" if errors remain ---
            if current_name == "error_handler":
                if (
                    current_name in self.output
                    and isinstance(self.output[current_name], list)
                    and len(self.output[current_name]) > 0
                ):
                    self.executed[current_name] = "ready"
            elif self.executed[current_name] != "ready":
                print(
                    f"\033[33m[SKIP] {current_name}: status={self.executed[current_name]}\033[0m"
                )
                continue
            # --- END OVERRIDE ---

            (
                func,
                input_deps,
                failure_deps,
                input_mapping,
                extra_args_mapping,
                extra_kwargs,
            ) = self.functions[current_name]

            # Check that all input dependencies have succeeded.
            if not all(
                self.executed.get(dep, "not started") == "succeeded"
                for dep in input_deps
            ):
                print(
                    f"\033[33m[DELAY] {current_name}: unmet dependencies {stringify_truncated(input_deps)}\033[0m"
                )
                continue

            # Build positional (args) and keyword (kwargs) parameters.
            args = []
            kwargs = extra_kwargs.copy()
            for dep, mapping in input_mapping.items():
                output = self.output.get(dep, None)
                if output is None:
                    raise ValueError(
                        f"\033[31mMissing required input {dep} for {current_name}\033[0m"
                    )
                if isinstance(mapping, int):
                    while len(args) <= mapping:
                        args.append(None)
                    args[mapping] = output
                elif isinstance(mapping, str):
                    kwargs[mapping] = output
                else:
                    raise ValueError(
                        f"\033[31mInvalid input mapping for {dep}: {mapping}\033[0m"
                    )
            for key, value in extra_args_mapping.items():
                if isinstance(key, int):
                    while len(args) <= key:
                        args.append(None)
                    args[key] = value
                elif isinstance(key, str):
                    kwargs[key] = value
                else:
                    raise ValueError(
                        f"\033[31mInvalid extra_args_mapping key for {current_name}: {key}\033[0m"
                    )
            final_args = [arg for arg in args if arg is not None]

            print(
                f"\033[35m[EXECUTING] {current_name}: args={stringify_truncated(final_args)}, kwargs={kwargs}\033[0m"
            )

            try:
                # Prepend initial output if available.
                if initial_outputs and current_name in initial_outputs:
                    final_args.insert(0, initial_outputs[current_name])
                failure_kwargs = {}

                # --- ERROR HANDLER BRANCH ---
                if current_name == "error_handler":
                    if (
                        current_name in self.output
                        and isinstance(self.output[current_name], list)
                        and self.output[current_name]
                    ):
                        # Pop one error record.
                        failure_kwargs = self.output[current_name].pop(0)
                        print(
                            f"            - Failure step: {failure_kwargs.get('error_step', 'UNKNOWN_STEP')}"
                        )
                        print(
                            f"            - ERROR MESSAGE: ❌ {failure_kwargs.get('error_message', 'UNKNOWN_ERROR')}"
                        )
                        # Requeue error_handler only if there are still errors left.
                        if len(self.output[current_name]) > 0:
                            self.ready_queue.append(current_name)
                            self.executed[current_name] = "ready"
                            print(
                                f"\033[35m[QUEUE] {current_name} re-added to process remaining errors ({len(self.output[current_name])} left)\033[0m"
                            )
                    # If no error details were found, skip this execution.
                    if not failure_kwargs:
                        print(
                            f"\033[33m[INFO] No error details for {current_name}; skipping its execution.\033[0m"
                        )
                        continue
                # --- END ERROR HANDLER BRANCH ---

                # Execute the function.
                output = func(*final_args, **kwargs, **failure_kwargs)
                # For non-error handlers, store the output normally.
                if current_name != "error_handler":
                    self.output[current_name] = output
                    self.executed[current_name] = "succeeded"
                    print(
                        f"\033[32m[SUCCESS] {current_name}: output={stringify_truncated(output)}\033[0m"
                    )
                else:
                    # For error_handler, do NOT overwrite self.output["error_handler"]
                    # Instead, just indicate that this error record was processed.
                    if not (
                        current_name in self.output
                        and isinstance(self.output[current_name], list)
                        and self.output[current_name]
                    ):
                        self.executed[current_name] = "succeeded"
                        print(
                            f"\033[32m[SUCCESS] {current_name}: All errors processed.\033[0m"
                        )

                # Enqueue any dependent functions.
                for dep in self.get_success_dependencies(current_name):
                    if self.is_ready(dep):
                        self.ready_queue.append(dep)
                        self.executed[dep] = "ready"
                        print(
                            f"\033[35m[QUEUE] {dep} added due to success of {current_name}\033[0m"
                        )

            except Exception as e:
                self.executed[current_name] = "failed"
                error_message = str(traceback.format_exc())
                # Create an independent error record.
                error_record = {
                    "error_step": str(current_name),
                    "error_message": error_message,
                }
                print(f"\033[31m[FAIL] {current_name}: {e}\033[0m")
                print(f"\033[33m[ERROR TRACE] {error_message}\033[0m")

                for dep in failure_deps:
                    self.executed[dep] = "ready"
                    self.ready_queue.append(dep)
                    print(
                        f"\033[35m[QUEUE] {dep} added due to failure of {current_name}\033[0m"
                    )
                    if dep not in self.output:
                        self.output[dep] = []
                    elif not isinstance(self.output[dep], list):
                        self.output[dep] = [self.output[dep]]
                    self.output[dep].append(error_record)
                    print(
                        f"\033[35m[STORE] Error recorded for {dep}: {error_record['error_step']}\033[0m"
                    )

    # ------------------------------------------------------------------------- #

    def get_success_dependencies(self, name):
        success_deps = [
            func_name
            for func_name, (_, input_deps, _, _, _, _) in self.functions.items()
            if name in input_deps
        ]
        print(
            f"\033[35m[LOOKUP] Success dependencies for {name}: {success_deps}\033[0m"
        )
        return success_deps

    def is_ready(self, name):
        if self.executed[name] != "not started":
            print(
                f"\033[33m[CHECK] {name} not ready, status={self.executed[name]}\033[0m"
            )
            return False
        if any(
            self.executed.get(dep, "not started") != "succeeded"
            for dep in self.functions[name][1]
        ):
            print(f"\033[33m[CHECK] {name} not ready, dependencies incomplete\033[0m")
            return False
        print(f"\033[32m[CHECK] {name} is ready to execute\033[0m")
        return True


# Standalone function to add a function to multiple DAGs
def add_f_to_registry(
    dags,
    name,
    input_deps,
    failure_deps,
    input_mapping,
    extra_args_mapping=None,
    extra_kwargs=None,
):
    def decorator(func):
        for dag in dags:
            dag.add_function(
                name,
                func,
                input_deps,
                failure_deps,
                input_mapping,
                extra_args_mapping,
                extra_kwargs,
            )
        return func

    return decorator


import sqlite3
import pickle
from collections import deque


def save_daggy(daggy_instance, db_path):
    conn = sqlite3.connect(db_path)
    conn.execute(
        "CREATE TABLE IF NOT EXISTS dag_def (id INTEGER PRIMARY KEY, functions BLOB)"
    )
    serialized_functions = pickle.dumps(daggy_instance.functions)
    conn.execute(
        "REPLACE INTO dag_def (id, functions) VALUES (?, ?)", (1, serialized_functions)
    )
    conn.commit()
    conn.close()


def load_daggy(db_path):
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    cur.execute("SELECT functions FROM dag_def WHERE id=?", (1,))
    row = cur.fetchone()
    conn.close()
    if row:
        functions = pickle.loads(row[0])
        daggy_instance = DaggyD()
        daggy_instance.functions = functions
        return daggy_instance
    raise ValueError("No saved DaggyD instance found.")


def save_execution_state(daggy_instance, db_path):
    conn = sqlite3.connect(db_path)
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS execution_state (
            id INTEGER PRIMARY KEY, output BLOB, executed BLOB, ready_queue BLOB)
    """
    )
    serialized_output = pickle.dumps(daggy_instance.output)
    serialized_executed = pickle.dumps(daggy_instance.executed)
    serialized_ready_queue = pickle.dumps(list(daggy_instance.ready_queue))
    conn.execute(
        """
        REPLACE INTO execution_state (id, output, executed, ready_queue) 
        VALUES (?, ?, ?, ?)""",
        (1, serialized_output, serialized_executed, serialized_ready_queue),
    )
    conn.commit()
    conn.close()


def load_execution_state(daggy_instance, db_path):
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    cur.execute(
        """
        SELECT output, executed, ready_queue FROM execution_state WHERE id=?""",
        (1,),
    )
    row = cur.fetchone()
    conn.close()
    if row:
        daggy_instance.output = pickle.loads(row[0])
        daggy_instance.executed = pickle.loads(row[1])
        daggy_instance.ready_queue = deque(pickle.loads(row[2]))
        return daggy_instance
    raise ValueError("No saved execution state found.")


def reset_execution_state(daggy_instance):
    daggy_instance.executed = {name: "not started" for name in daggy_instance.functions}
    daggy_instance.output = {}
    daggy_instance.ready_queue = deque()


def visualize_dag(daggy_instance):
    from collections import defaultdict

    deps_graph = defaultdict(list)

    for name, (
        _,
        input_deps,
        failure_deps,
        _,
        _,
        _,
    ) in daggy_instance.functions.items():
        for dep in input_deps:
            deps_graph[dep].append(name)
        for dep in failure_deps:
            deps_graph[dep].append(
                f"{name} \033[31m(failure path)\033[0m"
            )  # Red for failure paths

    def dfs(node, depth=0, visited=None):
        if visited is None:
            visited = set()

        color = (
            "\033[36m" if depth % 2 == 0 else "\033[35m"
        )  # Alternates between cyan & magenta
        print(f"{'  ' * depth}{color}- {node}\033[0m")

        visited.add(node)
        for child in deps_graph.get(node, []):
            if child not in visited:
                dfs(child, depth + 1, visited)

    roots = [
        name
        for name in daggy_instance.functions
        if all(name not in deps for deps in deps_graph.values())
    ]

    print("\n\033[36m=== DAG Dependency Visualization ===\033[0m")

    if not roots:
        print("\033[33m(No root nodes found)\033[0m")  # Yellow for warnings

    for root in roots:
        dfs(root)

    print("\033[36m====================================\033[0m\n")


def visualize_state(daggy_instance):
    print("\n\033[36m=== DAG Execution State ===\033[0m")

    for name in daggy_instance.functions:
        status = daggy_instance.executed.get(name, "not started")
        output = daggy_instance.output.get(name, "<no output yet>")

        print(f"\n\033[35mNode:\033[0m {name}")
        if status == "completed":
            status_color = "\033[32m"  # Green for success
        elif status == "error":
            status_color = "\033[31m"  # Red for errors
        else:
            status_color = "\033[33m"  # Yellow for in-progress/not started

        print(f"  \033[35mStatus:\033[0m {status_color}{status}\033[0m")
        print(f"  \033[35mOutput:\033[0m {stringify_truncated(output, max_len=200)}")

    rqueue = list(daggy_instance.ready_queue)
    print("\n\033[36m--- Ready Queue ---\033[0m")
    if rqueue:
        for i, node in enumerate(rqueue, 1):
            print(f"  {i}. {node}")
    else:
        print("  \033[33m(empty)\033[0m")

    print("\033[36m===========================\033[0m\n")
