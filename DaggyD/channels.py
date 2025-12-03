def create_last_value_channel(initial_value=None):
    """
    LastValue channel with BSP-compliant message delivery.
    
    Per Pregel BSP model:
    - Writes during superstep S set has_new_message flag
    - checkpoint() commits writes and KEEPS has_new_message True
    - has_updates() returns True if there was a write (message to deliver)
    - consume() clears the flag after plan has seen it (called at start of execution)
    """
    state = {
        "value": initial_value,           # Current value
        "has_new_message": False          # True if there's a message to deliver
    }
    
    def write(value):
        state["value"] = value
        state["has_new_message"] = True
        return value
    
    def read():
        return state["value"]
    
    def checkpoint():
        # BSP barrier: just return the value, keep has_new_message for next plan()
        return state["value"]
    
    def consume():
        # Called after plan() has scheduled nodes - clear the "new message" flag
        state["has_new_message"] = False
    
    def restore(saved_state):
        state["value"] = saved_state
        state["has_new_message"] = False
    
    def clear():
        state["has_new_message"] = False
    
    def is_empty():
        return state["value"] is None
    
    def has_updates():
        return state["has_new_message"]
    
    def get_state():
        return {
            "value": state["value"],
            "has_new_message": state["has_new_message"]
        }
    
    def set_state(new_state):
        state["value"] = new_state.get("value")
        state["has_new_message"] = new_state.get("has_new_message", False)
    
    return {
        "write": write,
        "read": read,
        "checkpoint": checkpoint,
        "consume": consume,
        "restore": restore,
        "clear": clear,
        "is_empty": is_empty,
        "has_updates": has_updates,
        "get_state": get_state,
        "set_state": set_state,
        "type": "LastValue"
    }


def create_topic_channel():
    """
    Topic channel - accumulates messages, BSP compliant.
    """
    state = {
        "values": [],               # Committed messages
        "pending_values": [],       # Buffered writes
        "has_new_message": False    # Flag for message delivery
    }
    
    def write(value):
        state["pending_values"].append(value)
        state["has_new_message"] = True
        return value
    
    def read():
        return state["values"].copy()
    
    def checkpoint():
        # Commit pending to values
        state["values"].extend(state["pending_values"])
        state["pending_values"] = []
        return state["values"].copy()
    
    def consume():
        state["has_new_message"] = False
    
    def restore(saved_state):
        state["values"] = saved_state
        state["pending_values"] = []
        state["has_new_message"] = False
    
    def clear():
        state["values"] = []
        state["pending_values"] = []
        state["has_new_message"] = False
    
    def is_empty():
        return len(state["values"]) == 0 and len(state["pending_values"]) == 0
    
    def has_updates():
        return state["has_new_message"]
    
    def get_state():
        return {
            "values": state["values"].copy(),
            "pending_values": state["pending_values"].copy(),
            "has_new_message": state["has_new_message"]
        }
    
    def set_state(new_state):
        state["values"] = new_state.get("values", [])
        state["pending_values"] = new_state.get("pending_values", [])
        state["has_new_message"] = new_state.get("has_new_message", False)
    
    return {
        "write": write,
        "read": read,
        "checkpoint": checkpoint,
        "consume": consume,
        "restore": restore,
        "clear": clear,
        "is_empty": is_empty,
        "has_updates": has_updates,
        "get_state": get_state,
        "set_state": set_state,
        "type": "Topic"
    }


def create_binary_operator_channel(operator, initial_value=None):
    """
    BinaryOperator channel - aggregates values with operator, BSP compliant.
    """
    state = {
        "value": initial_value,
        "pending_values": [],
        "operator": operator,
        "has_new_message": False
    }
    
    def write(value):
        state["pending_values"].append(value)
        state["has_new_message"] = True
        return value
    
    def read():
        return state["value"]
    
    def checkpoint():
        for val in state["pending_values"]:
            if state["value"] is None:
                state["value"] = val
            else:
                state["value"] = state["operator"](state["value"], val)
        state["pending_values"] = []
        return state["value"]
    
    def consume():
        state["has_new_message"] = False
    
    def restore(saved_state):
        state["value"] = saved_state
        state["pending_values"] = []
        state["has_new_message"] = False
    
    def clear():
        state["pending_values"] = []
        state["has_new_message"] = False
    
    def is_empty():
        return state["value"] is None and len(state["pending_values"]) == 0
    
    def has_updates():
        return state["has_new_message"]
    
    def get_state():
        return {
            "value": state["value"],
            "pending_values": state["pending_values"].copy(),
            "has_new_message": state["has_new_message"]
        }
    
    def set_state(new_state):
        state["value"] = new_state.get("value")
        state["pending_values"] = new_state.get("pending_values", [])
        state["has_new_message"] = new_state.get("has_new_message", False)
    
    def get_operator():
        return state["operator"]
    
    return {
        "write": write,
        "read": read,
        "checkpoint": checkpoint,
        "consume": consume,
        "restore": restore,
        "clear": clear,
        "is_empty": is_empty,
        "has_updates": has_updates,
        "get_state": get_state,
        "set_state": set_state,
        "get_operator": get_operator,
        "type": "BinaryOperator"
    }


def create_ephemeral_channel():
    """
    Ephemeral channel - value is cleared after each superstep, BSP compliant.
    """
    state = {
        "value": None,
        "has_value": False,
        "has_new_message": False
    }
    
    def write(value):
        state["value"] = value
        state["has_value"] = True
        state["has_new_message"] = True
        return value
    
    def read():
        return state["value"] if state["has_value"] else None
    
    def checkpoint():
        # Ephemeral: value is cleared after checkpoint
        val = state["value"]
        state["value"] = None
        state["has_value"] = False
        return val
    
    def consume():
        state["has_new_message"] = False
    
    def restore(saved_state):
        state["value"] = None
        state["has_value"] = False
        state["has_new_message"] = False
    
    def clear():
        state["value"] = None
        state["has_value"] = False
        state["has_new_message"] = False
    
    def is_empty():
        return not state["has_value"]
    
    def has_updates():
        return state["has_new_message"]
    
    def get_state():
        return {
            "value": state["value"],
            "has_value": state["has_value"],
            "has_new_message": state["has_new_message"]
        }
    
    def set_state(new_state):
        state["value"] = new_state.get("value")
        state["has_value"] = new_state.get("has_value", False)
        state["has_new_message"] = new_state.get("has_new_message", False)
    
    return {
        "write": write,
        "read": read,
        "checkpoint": checkpoint,
        "consume": consume,
        "restore": restore,
        "clear": clear,
        "is_empty": is_empty,
        "has_updates": has_updates,
        "get_state": get_state,
        "set_state": set_state,
        "type": "Ephemeral"
    }


def create_accumulator_channel(operator, initial_value=None):
    """
    Accumulator channel - aggregates values with operator, then RESETS after checkpoint.
    
    This is perfect for Pregel message passing:
    - Multiple senders write contributions during a superstep
    - Values are aggregated via the operator (e.g., sum)
    - After checkpoint (BSP barrier), the value resets to initial_value
    - This way, each superstep only sees NEW messages
    
    Example use case: PageRank inbox that sums contributions each superstep
    """
    state = {
        "value": initial_value,       # Committed value (from last checkpoint)
        "pending_values": [],         # Values written this superstep
        "operator": operator,
        "initial_value": initial_value,
        "has_new_message": False
    }
    
    def write(value):
        state["pending_values"].append(value)
        state["has_new_message"] = True
        return value
    
    def read():
        # Return the committed value (result of last superstep's aggregation)
        return state["value"]
    
    def checkpoint():
        # Aggregate pending values
        result = state["initial_value"]
        for val in state["pending_values"]:
            if result is None:
                result = val
            else:
                result = state["operator"](result, val)
        
        # Store result as the committed value for next superstep's read
        state["value"] = result
        
        # Clear pending for next superstep
        state["pending_values"] = []
        
        return result
    
    def consume():
        state["has_new_message"] = False
    
    def restore(saved_state):
        state["value"] = saved_state
        state["pending_values"] = []
        state["has_new_message"] = False
    
    def clear():
        state["value"] = state["initial_value"]
        state["pending_values"] = []
        state["has_new_message"] = False
    
    def is_empty():
        return state["value"] is None and len(state["pending_values"]) == 0
    
    def has_updates():
        return state["has_new_message"]
    
    def get_state():
        return {
            "value": state["value"],
            "pending_values": state["pending_values"].copy(),
            "initial_value": state["initial_value"],
            "has_new_message": state["has_new_message"]
        }
    
    def set_state(new_state):
        state["value"] = new_state.get("value")
        state["pending_values"] = new_state.get("pending_values", [])
        state["initial_value"] = new_state.get("initial_value", initial_value)
        state["has_new_message"] = new_state.get("has_new_message", False)
    
    def get_operator():
        return state["operator"]
    
    return {
        "write": write,
        "read": read,
        "checkpoint": checkpoint,
        "consume": consume,
        "restore": restore,
        "clear": clear,
        "is_empty": is_empty,
        "has_updates": has_updates,
        "get_state": get_state,
        "set_state": set_state,
        "get_operator": get_operator,
        "type": "Accumulator"
    }


def create_place_channel(capacity=float('inf'), initial_tokens=0):
    """
    Petri Net Place - holds discrete tokens with optional capacity limit.
    
    This implements Petri net semantics:
    - Places hold non-negative integer tokens
    - Capacity limits maximum tokens (default: unlimited)
    - Tokens are consumed by input arcs when transitions fire
    - Tokens are produced by output arcs after transitions fire
    
    Key operations:
    - write(n): Add n tokens (pending until checkpoint)
    - consume(n): Remove n tokens (pending until checkpoint)
    - can_consume(n): Check if n tokens available for firing
    - read(): Returns current committed token count
    
    BSP semantics:
    - Writes and consumes are buffered until checkpoint
    - Checkpoint commits all pending operations atomically
    - This ensures consistent state during parallel execution
    
    Example use cases:
    - Bounded buffer (producer-consumer)
    - Resource pools (database connections)
    - Synchronization barriers (wait for N signals)
    - Mutex/semaphore patterns
    """
    state = {
        "tokens": initial_tokens,      # Committed token count
        "capacity": capacity,          # Maximum tokens allowed
        "pending_add": 0,              # Tokens to add at checkpoint
        "pending_remove": 0,           # Tokens to remove at checkpoint
        "has_new_message": False,      # For BSP message detection
        "initial_tokens": initial_tokens
    }
    
    def write(n=1):
        """Add n tokens to the place (pending until checkpoint)."""
        if not isinstance(n, (int, float)) or n < 0:
            raise ValueError(f"Token count must be non-negative, got {n}")
        state["pending_add"] += n
        state["has_new_message"] = True
        return n
    
    def consume(n=1):
        """
        Mark n tokens for consumption (pending until checkpoint).
        Should only be called after can_consume() returns True.
        """
        if not isinstance(n, (int, float)) or n < 0:
            raise ValueError(f"Token count must be non-negative, got {n}")
        state["pending_remove"] += n
        return n
    
    def can_consume(n=1):
        """Check if n tokens are available for consumption."""
        return state["tokens"] >= n
    
    def can_produce(n=1):
        """Check if n tokens can be added without exceeding capacity."""
        return state["tokens"] + state["pending_add"] - state["pending_remove"] + n <= state["capacity"]
    
    def read():
        """Return the current committed token count."""
        return state["tokens"]
    
    def peek_after_pending():
        """Return what token count would be after pending operations."""
        return state["tokens"] + state["pending_add"] - state["pending_remove"]
    
    def checkpoint():
        """
        Commit all pending operations atomically.
        
        Order: remove first, then add (Petri net firing semantics)
        """
        new_tokens = state["tokens"] - state["pending_remove"] + state["pending_add"]
        
        # Validate
        if new_tokens < 0:
            raise RuntimeError(
                f"Place underflow: tried to remove {state['pending_remove']} "
                f"from {state['tokens']} tokens"
            )
        if new_tokens > state["capacity"]:
            raise RuntimeError(
                f"Place overflow: {new_tokens} tokens exceeds capacity {state['capacity']}"
            )
        
        state["tokens"] = new_tokens
        state["pending_add"] = 0
        state["pending_remove"] = 0
        
        return state["tokens"]
    
    def consume_messages():
        """Clear the has_new_message flag after plan phase."""
        state["has_new_message"] = False
    
    def restore(saved_tokens):
        """Restore place to a specific token count."""
        state["tokens"] = saved_tokens
        state["pending_add"] = 0
        state["pending_remove"] = 0
        state["has_new_message"] = False
    
    def clear():
        """Reset place to initial state."""
        state["tokens"] = state["initial_tokens"]
        state["pending_add"] = 0
        state["pending_remove"] = 0
        state["has_new_message"] = False
    
    def is_empty():
        """Check if place has no tokens."""
        return state["tokens"] == 0 and state["pending_add"] == 0
    
    def has_updates():
        """Check if there are pending changes (for BSP scheduling)."""
        return state["has_new_message"]
    
    def get_state():
        """Get full state for checkpointing."""
        return {
            "tokens": state["tokens"],
            "capacity": state["capacity"],
            "pending_add": state["pending_add"],
            "pending_remove": state["pending_remove"],
            "has_new_message": state["has_new_message"],
            "initial_tokens": state["initial_tokens"]
        }
    
    def set_state(new_state):
        """Restore full state from checkpoint."""
        state["tokens"] = new_state.get("tokens", 0)
        state["capacity"] = new_state.get("capacity", float('inf'))
        state["pending_add"] = new_state.get("pending_add", 0)
        state["pending_remove"] = new_state.get("pending_remove", 0)
        state["has_new_message"] = new_state.get("has_new_message", False)
        state["initial_tokens"] = new_state.get("initial_tokens", 0)
    
    def get_capacity():
        """Return the capacity of this place."""
        return state["capacity"]
    
    return {
        "write": write,
        "read": read,
        "consume": consume,
        "consume_messages": consume_messages,
        "can_consume": can_consume,
        "can_produce": can_produce,
        "peek_after_pending": peek_after_pending,
        "checkpoint": checkpoint,
        "restore": restore,
        "clear": clear,
        "is_empty": is_empty,
        "has_updates": has_updates,
        "get_state": get_state,
        "set_state": set_state,
        "get_capacity": get_capacity,
        "type": "Place"
    }


def create_managed_channel(channel_type, manager, **kwargs):
    shared_state = manager.dict()
    lock = manager.Lock()
    
    if channel_type == "LastValue":
        initial_value = kwargs.get("initial_value")
        shared_state["value"] = initial_value
        shared_state["has_new_message"] = False
        
        def write(value):
            with lock:
                shared_state["value"] = value
                shared_state["has_new_message"] = True
            return value
        
        def read():
            with lock:
                return shared_state["value"]
        
        def checkpoint():
            with lock:
                return shared_state["value"]
        
        def consume():
            with lock:
                shared_state["has_new_message"] = False
        
        def restore(saved_state):
            with lock:
                shared_state["value"] = saved_state
                shared_state["has_new_message"] = False
        
        def clear():
            with lock:
                shared_state["has_new_message"] = False
        
        def is_empty():
            with lock:
                return shared_state["value"] is None
        
        def has_updates():
            with lock:
                return shared_state["has_new_message"]
        
        def get_state():
            with lock:
                return {
                    "value": shared_state["value"],
                    "has_new_message": shared_state["has_new_message"]
                }
        
        def set_state(new_state):
            with lock:
                shared_state["value"] = new_state.get("value")
                shared_state["has_new_message"] = new_state.get("has_new_message", False)
        
        return {
            "write": write,
            "read": read,
            "consume": consume,
            "checkpoint": checkpoint,
            "restore": restore,
            "clear": clear,
            "is_empty": is_empty,
            "has_updates": has_updates,
            "get_state": get_state,
            "set_state": set_state,
            "type": "LastValue",
            "managed": True
        }
    
    elif channel_type == "Topic":
        shared_state["values"] = manager.list()
        shared_state["pending_values"] = manager.list()
        shared_state["has_new_message"] = False
        
        def write(value):
            with lock:
                shared_state["pending_values"].append(value)
                shared_state["has_new_message"] = True
            return value
        
        def read():
            with lock:
                return list(shared_state["values"])
        
        def checkpoint():
            with lock:
                for v in shared_state["pending_values"]:
                    shared_state["values"].append(v)
                del shared_state["pending_values"][:]
                return list(shared_state["values"])
        
        def consume():
            with lock:
                shared_state["has_new_message"] = False
        
        def restore(saved_state):
            with lock:
                del shared_state["values"][:]
                for v in saved_state:
                    shared_state["values"].append(v)
                del shared_state["pending_values"][:]
                shared_state["has_new_message"] = False
        
        def clear():
            with lock:
                del shared_state["values"][:]
                del shared_state["pending_values"][:]
                shared_state["has_new_message"] = False
        
        def is_empty():
            with lock:
                return len(shared_state["values"]) == 0 and len(shared_state["pending_values"]) == 0
        
        def has_updates():
            with lock:
                return shared_state["has_new_message"]
        
        def get_state():
            with lock:
                return {
                    "values": list(shared_state["values"]),
                    "pending_values": list(shared_state["pending_values"]),
                    "has_new_message": shared_state["has_new_message"]
                }
        
        def set_state(new_state):
            with lock:
                del shared_state["values"][:]
                for v in new_state.get("values", []):
                    shared_state["values"].append(v)
                del shared_state["pending_values"][:]
                for v in new_state.get("pending_values", []):
                    shared_state["pending_values"].append(v)
                shared_state["has_new_message"] = new_state.get("has_new_message", False)
        
        return {
            "write": write,
            "read": read,
            "checkpoint": checkpoint,
            "consume": consume,
            "restore": restore,
            "clear": clear,
            "is_empty": is_empty,
            "has_updates": has_updates,
            "get_state": get_state,
            "set_state": set_state,
            "type": "Topic",
            "managed": True
        }
    
    elif channel_type == "BinaryOperator":
        operator = kwargs.get("operator")
        initial_value = kwargs.get("initial_value")
        shared_state["value"] = initial_value
        shared_state["pending_values"] = manager.list()
        shared_state["has_new_message"] = False
        
        def write(value):
            with lock:
                shared_state["pending_values"].append(value)
                shared_state["has_new_message"] = True
            return value
        
        def read():
            with lock:
                return shared_state["value"]
        
        def checkpoint():
            with lock:
                current = shared_state["value"]
                for val in shared_state["pending_values"]:
                    if current is None:
                        current = val
                    else:
                        current = operator(current, val)
                shared_state["value"] = current
                del shared_state["pending_values"][:]
                return current
        
        def consume():
            with lock:
                shared_state["has_new_message"] = False
        
        def restore(saved_state):
            with lock:
                shared_state["value"] = saved_state
                del shared_state["pending_values"][:]
                shared_state["has_new_message"] = False
        
        def clear():
            with lock:
                del shared_state["pending_values"][:]
                shared_state["has_new_message"] = False
        
        def is_empty():
            with lock:
                return shared_state["value"] is None and len(shared_state["pending_values"]) == 0
        
        def has_updates():
            with lock:
                return shared_state["has_new_message"]
        
        def get_state():
            with lock:
                return {
                    "value": shared_state["value"],
                    "pending_values": list(shared_state["pending_values"]),
                    "has_new_message": shared_state["has_new_message"]
                }
        
        def set_state(new_state):
            with lock:
                shared_state["value"] = new_state.get("value")
                del shared_state["pending_values"][:]
                for v in new_state.get("pending_values", []):
                    shared_state["pending_values"].append(v)
                shared_state["has_new_message"] = new_state.get("has_new_message", False)
        
        return {
            "write": write,
            "read": read,
            "checkpoint": checkpoint,
            "consume": consume,
            "restore": restore,
            "clear": clear,
            "is_empty": is_empty,
            "has_updates": has_updates,
            "get_state": get_state,
            "set_state": set_state,
            "type": "BinaryOperator",
            "managed": True
        }
    
    elif channel_type == "Ephemeral":
        shared_state["value"] = None
        shared_state["has_value"] = False
        shared_state["has_new_message"] = False
        
        def write(value):
            with lock:
                shared_state["value"] = value
                shared_state["has_value"] = True
                shared_state["has_new_message"] = True
            return value
        
        def read():
            with lock:
                return shared_state["value"] if shared_state["has_value"] else None
        
        def checkpoint():
            with lock:
                val = shared_state["value"]
                shared_state["value"] = None
                shared_state["has_value"] = False
                return val
        
        def consume():
            with lock:
                shared_state["has_new_message"] = False
        
        def restore(saved_state):
            with lock:
                shared_state["value"] = None
                shared_state["has_value"] = False
                shared_state["has_new_message"] = False
        
        def clear():
            with lock:
                shared_state["value"] = None
                shared_state["has_value"] = False
                shared_state["has_new_message"] = False
        
        def is_empty():
            with lock:
                return not shared_state["has_value"]
        
        def has_updates():
            with lock:
                return shared_state["has_new_message"]
        
        def get_state():
            with lock:
                return {
                    "value": shared_state["value"],
                    "has_value": shared_state["has_value"],
                    "has_new_message": shared_state["has_new_message"]
                }
        
        def set_state(new_state):
            with lock:
                shared_state["value"] = new_state.get("value")
                shared_state["has_value"] = new_state.get("has_value", False)
                shared_state["has_new_message"] = new_state.get("has_new_message", False)
        
        return {
            "write": write,
            "read": read,
            "checkpoint": checkpoint,
            "consume": consume,
            "restore": restore,
            "clear": clear,
            "is_empty": is_empty,
            "has_updates": has_updates,
            "get_state": get_state,
            "set_state": set_state,
            "type": "Ephemeral",
            "managed": True
        }
    
    raise ValueError(f"Unknown channel type: {channel_type}")