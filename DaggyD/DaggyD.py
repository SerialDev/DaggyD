from collections import deque
import traceback


class DaggyD:
    def __init__(self):
        self.functions = (
            {}
        )  # name: (func, input_deps, failure_deps, input_mapping, extra_args_mapping, extra_kwargs)
        self.executed = {}  # name: 'not started' | 'succeeded' | 'failed'
        self.ready_queue = deque()
        self.output = {}  # name: output

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
        """
        Add a function to the DAG with dependencies, parameter mappings, and extra arguments.

        Args:
            name (str): Name of the function.
            func (callable): The function to be executed.
            input_deps (list): List of dependencies required for success.
            failure_deps (list): List of dependencies triggered on failure.
            input_mapping (dict): Mapping of dependency outputs to function parameters.
                - Key: Dependency name.
                - Value: Integer (positional index) or string (keyword argument name).
            extra_args_mapping (dict, optional): Mapping of extra arguments to specific positions or names.
                - Key: Integer (position) or string (parameter name).
                - Value: Argument value.
            extra_kwargs (dict, optional): Additional keyword arguments to pass to the function.
        """
        if name in self.functions:
            raise ValueError(f"Function {name} already exists")
        extra_args_mapping = extra_args_mapping or {}
        extra_kwargs = extra_kwargs or {}
        print(
            f"Registering function {name} with input dependencies: {input_deps}, "
            f"failure dependencies: {failure_deps}, input mapping: {input_mapping}, "
            f"extra_args_mapping: {extra_args_mapping}, extra_kwargs: {extra_kwargs}"
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
        """
        Decorator to add a function to the DAG.

        Args:
            name (str): Name of the function.
            input_deps (list): List of dependencies required for success.
            failure_deps (list): List of dependencies triggered on failure.
            input_mapping (dict): Mapping of dependency outputs to function parameters.
            extra_args_mapping (dict, optional): Mapping of extra arguments to specific positions or names.
            extra_kwargs (dict, optional): Additional keyword arguments to pass to the function.
        """

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
        """
        Execute the DAG starting from the specified function.

        Args:
            start_name (str): Name of the starting function.
            initial_outputs (dict, optional): Predefined outputs for some functions.
        """
        if start_name not in self.functions:
            raise ValueError(f"Function {start_name} not found")

        print(f"\nStarting DAG execution from {start_name}")
        for name in self.functions:
            self.executed[name] = "not started"
            print(f"Initialized status for {name}: not started")

        if initial_outputs:
            for name, output in initial_outputs.items():
                if name not in self.functions:
                    raise ValueError(f"Initial output for non-existent function {name}")
                self.output[name] = output
                self.executed[name] = "succeeded"
                print(f"Initialized {name} with output: {output}")

        self.ready_queue.append(start_name)
        self.executed[start_name] = "ready"
        print(f"Added {start_name} to ready queue")

        while self.ready_queue:
            current_name = self.ready_queue.popleft()
            print(f"\nProcessing {current_name} from ready queue")
            if self.executed[current_name] != "ready":
                print(
                    f"Skipping {current_name}, status is {self.executed[current_name]}"
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
                    f"Cannot execute {current_name} yet, dependencies not met: {input_deps}"
                )
                continue

            # Prepare inputs: start with dependency mappings
            args = []
            kwargs = extra_kwargs.copy()  # Start with extra kwargs

            # Apply dependency input mappings
            for dep, mapping in input_mapping.items():
                output = self.output[dep]
                if isinstance(mapping, int):  # Positional argument
                    while len(args) <= mapping:
                        args.append(None)
                    args[mapping] = output
                elif isinstance(mapping, str):  # Keyword argument
                    kwargs[mapping] = output
                else:
                    raise ValueError(f"Invalid input mapping for {dep}: {mapping}")

            # Apply extra_args_mapping
            for key, value in extra_args_mapping.items():
                if isinstance(key, int):  # Positional argument
                    while len(args) <= key:
                        args.append(None)
                    args[key] = value
                elif isinstance(key, str):  # Keyword argument
                    kwargs[key] = value
                else:
                    raise ValueError(
                        f"Invalid extra_args_mapping key for {current_name}: {key}"
                    )

            # Remove None placeholders and finalize args
            final_args = [arg for arg in args if arg is not None]

            print(f"Executing {current_name} with args: {final_args}, kwargs: {kwargs}")

            try:
                output = func(*final_args, **kwargs)
                self.output[current_name] = output
                self.executed[current_name] = "succeeded"
                print(f"Successfully executed {current_name}, output: {output}")

                for dep in self.get_success_dependencies(current_name):
                    if self.is_ready(dep):
                        self.ready_queue.append(dep)
                        self.executed[dep] = "ready"
                        print(
                            f"Added {dep} to ready queue due to success of {current_name}"
                        )
            except Exception as e:
                self.executed[current_name] = "failed"
                print(f"Failed to execute {current_name}: {e}")
                traceback.print_exc()

                for dep in failure_deps:
                    if self.is_ready(dep):
                        self.ready_queue.append(dep)
                        self.executed[dep] = "ready"
                        print(
                            f"Added {dep} to ready queue due to failure of {current_name}"
                        )

                for dep in self.get_success_dependencies(current_name):
                    if self.executed[dep] == "not started":
                        self.executed[dep] = "failed"
                        print(
                            f"Marked {dep} as failed due to dependency failure from {current_name}"
                        )

    def get_success_dependencies(self, name):
        success_deps = []
        for func_name, (
            func,
            input_deps,
            failure_deps,
            input_mapping,
            extra_args_mapping,
            extra_kwargs,
        ) in self.functions.items():
            if name in input_deps:
                success_deps.append(func_name)
        print(f"Found success dependencies for {name}: {success_deps}")
        return success_deps

    def is_ready(self, name):
        if self.executed[name] != "not started":
            print(f"{name} is not ready, current status: {self.executed[name]}")
            return False
        (
            func,
            input_deps,
            failure_deps,
            input_mapping,
            extra_args_mapping,
            extra_kwargs,
        ) = self.functions[name]
        for dep in input_deps:
            if self.executed.get(dep, "not started") != "succeeded":
                print(
                    f"{name} is not ready, dependency {dep} status: {self.executed.get(dep, 'not started')}"
                )
                return False
        print(f"{name} is ready to execute")
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
