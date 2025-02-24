import traceback
from collections import deque


class DaggyD:
    def __init__(self):
        self.functions = {}
        self.executed = {}
        self.output = {}
        self.ready_queue = deque()

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
            f"\033[36m[REGISTER] {name}: input_deps={input_deps}, failure_deps={failure_deps}, input_mapping={input_mapping}, extra_args_mapping={extra_args_mapping}, extra_kwargs={extra_kwargs}\033[0m"
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
        for name in self.functions:
            self.executed[name] = "not started"
            print(f"\033[33m[INIT] {name}: status=not started\033[0m")

        if initial_outputs:
            for name, output in initial_outputs.items():
                if name not in self.functions:
                    raise ValueError(
                        f"\033[31mInitial output for non-existent function {name}\033[0m"
                    )
                self.output[name] = output
                self.executed[name] = "succeeded"
                print(f"\033[32m[PRELOAD] {name}: output={output}\033[0m")

        self.ready_queue.append(start_name)
        self.executed[start_name] = "ready"
        print(f"\033[35m[QUEUE] {start_name} added to ready queue\033[0m")

        while self.ready_queue:
            current_name = self.ready_queue.popleft()
            print(f"\033[34m\n[PROCESSING] {current_name}\033[0m")
            if self.executed[current_name] != "ready":
                print(
                    f"\033[33m[SKIP] {current_name}: status={self.executed[current_name]}\033[0m"
                )
                continue

            (
                func,
                input_deps,
                failure_deps,
                input_mapping,
                extra_args_mapping,
                extra_kwargs,
            ) = self.functions[current_name]

            if not all(
                self.executed.get(dep, "not started") == "succeeded"
                for dep in input_deps
            ):
                print(
                    f"\033[33m[DELAY] {current_name}: unmet dependencies {input_deps}\033[0m"
                )
                continue

            args = []
            kwargs = extra_kwargs.copy()
            for dep, mapping in input_mapping.items():
                output = self.output[dep]
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
                f"\033[35m[EXECUTING] {current_name}: args={final_args}, kwargs={kwargs}\033[0m"
            )

            try:
                output = func(*final_args, **kwargs)
                self.output[current_name] = output
                self.executed[current_name] = "succeeded"
                print(f"\033[32m[SUCCESS] {current_name}: output={output}\033[0m")

                for dep in self.get_success_dependencies(current_name):
                    if self.is_ready(dep):
                        self.ready_queue.append(dep)
                        self.executed[dep] = "ready"
                        print(
                            f"\033[35m[QUEUE] {dep} added due to success of {current_name}\033[0m"
                        )
            except Exception as e:
                self.executed[current_name] = "failed"
                print(f"\033[31m[FAIL] {current_name}: {e}\033[0m")
                traceback.print_exc()

                for dep in failure_deps:
                    if self.is_ready(dep):
                        self.ready_queue.append(dep)
                        self.executed[dep] = "ready"
                        print(
                            f"\033[35m[QUEUE] {dep} added due to failure of {current_name}\033[0m"
                        )

                for dep in self.get_success_dependencies(current_name):
                    if self.executed[dep] == "not started":
                        self.executed[dep] = "failed"
                        print(
                            f"\033[31m[CASCADE FAIL] {dep} marked failed due to {current_name}\033[0m"
                        )

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


# extra args at ARbitrary position
dag = DaggyD()


@dag.add_to_registry("func1", [], ["error_handler"], {})
def func1():
    print("Executing func1")
    return "output_from_func1"


@dag.add_to_registry(
    "func2",
    ["func1"],
    ["error_handler"],
    {"func1": 1},
    extra_args_mapping={0: "extra_at_0"},
)
def func2(param0, param_from_func1):
    print(
        f"Executing func2 with param0: {param0}, param_from_func1: {param_from_func1}"
    )
    return f"output_from_func2_with_{param0}_and_{param_from_func1}"


@dag.add_to_registry("error_handler", [], [], {})
def error_handler():
    print("Error occurred, handling failure")
    return "error_handled"


print("\n=== Example 1: Extra Args at Arbitrary Position ===")
dag.execute("func1")

print("\nFinal Outputs:")
print(f"Func1: {dag.output.get('func1', 'Not executed')}")
print(f"Func2: {dag.output.get('func2', 'Not executed')}")
print(f"Error Handler: {dag.output.get('error_handler', 'Not executed')}")


# Extra args at KWARG
dag = DaggyD()


@dag.add_to_registry("func1", [], ["error_handler"], {})
def func1():
    print("Executing func1")
    return "output_from_func1"


@dag.add_to_registry(
    "func2",
    ["func1"],
    ["error_handler"],
    {"func1": 0},
    extra_args_mapping={"extra_param": "extra_value"},
)
def func2(param_from_func1, extra_param=None):
    print(
        f"Executing func2 with param_from_func1: {param_from_func1}, extra_param: {extra_param}"
    )
    return f"output_from_func2_with_{param_from_func1}_and_{extra_param}"


@dag.add_to_registry("error_handler", [], [], {})
def error_handler():
    print("Error occurred, handling failure")
    return "error_handled"


print("\n=== Example 2: Extra Args as Kwarg ===")
dag.execute("func1")

print("\nFinal Outputs:")
print(f"Func1: {dag.output.get('func1', 'Not executed')}")
print(f"Func2: {dag.output.get('func2', 'Not executed')}")
print(f"Error Handler: {dag.output.get('error_handler', 'Not executed')}")


# Mixed mapping with extra args and kwargs
dag = DaggyD()


@dag.add_to_registry("func1", [], ["error_handler"], {})
def func1():
    print("Executing func1")
    return "output_from_func1"


@dag.add_to_registry(
    "func2",
    ["func1"],
    ["error_handler"],
    {"func1": 1},
    extra_args_mapping={0: "func2_extra_at_0"},
)
def func2(extra_param_at_0, param_from_func1):
    print(
        f"Executing func2 with extra_param_at_0: {extra_param_at_0}, param_from_func1: {param_from_func1}"
    )
    return "output_from_func2"


@dag.add_to_registry(
    "func3",
    ["func1", "func2"],
    ["error_handler"],
    {"func1": 1, "func2": "kwarg2"},
    extra_args_mapping={0: "func3_extra_at_0", "extra_kwarg": "extra_value"},
    extra_kwargs={"another_kwarg": "kwarg_value"},
)
def func3(arg0, param_from_func1, kwarg2=None, extra_kwarg=None, another_kwarg=None):
    print(
        f"Executing func3 with arg0: {arg0}, param_from_func1: {param_from_func1}, "
        f"kwarg2: {kwarg2}, extra_kwarg: {extra_kwarg}, another_kwarg: {another_kwarg}"
    )
    return f"output_from_func3_with_{param_from_func1}_and_{kwarg2}"


@dag.add_to_registry("error_handler", [], [], {})
def error_handler():
    print("Error occurred, handling failure")
    return "error_handled"


print("\n=== Example 3: Mixed Mapping with Extra Args and Kwargs ===")
dag.execute("func1")

print("\nFinal Outputs:")
print(f"Func1: {dag.output.get('func1', 'Not executed')}")
print(f"Func2: {dag.output.get('func2', 'Not executed')}")
print(f"Func3: {dag.output.get('func3', 'Not executed')}")
print(f"Error Handler: {dag.output.get('error_handler', 'Not executed')}")
