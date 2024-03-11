import inspect
import hashlib

def get_class_hash(cls):
    # Get the source code of the class
    source_code = inspect.getsource(cls)

    # Hash the source code
    hash_object = hashlib.sha256(source_code.encode())
    return hash_object.hexdigest()

def has_class_changed(cls, stored_hash):
    # Get the current hash of the class
    current_hash = get_class_hash(cls)

    # Compare the current hash with the stored hash
    return current_hash != stored_hash

# Example usage
class MyClass:
    def __init__(self):
        pass

# Get the initial hash of the class
initial_hash = get_class_hash(MyClass)

# Later, check if the class has changed
if has_class_changed(MyClass, initial_hash):
    print("The class has changed.")
else:
    print("The class has not changed.")