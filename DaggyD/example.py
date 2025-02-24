from DaggyD import DaggyD, add_f_to_registry


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


import traceback
from collections import deque

dag1 = DaggyD()
dag2 = DaggyD()

dags = [dag1, dag2]


def func1():
    print("Executing func1")
    return "output_from_func1"


def func2(param0, param_from_func1):
    print(
        f"Executing func2 with param0: {param0}, param_from_func1: {param_from_func1}"
    )
    return f"output_from_func2_with_{param0}_and_{param_from_func1}"


def error_handler():
    print("Error occurred, handling failure")
    return "error_handled"


add_f_to_registry(dags, "func1", [], ["error_handler"], {})(func1)
add_f_to_registry(
    dags,
    "func2",
    ["func1"],
    ["error_handler"],
    {"func1": 1},
    extra_args_mapping={0: "extra_at_0"},
)(func2)
add_f_to_registry(dags, "error_handler", [], [], {})(error_handler)

print("\n=== Testing DAG 1 ===")
dag1.execute("func1")

print("\n=== Testing DAG 2 ===")
dag2.execute("func1")
