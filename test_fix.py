import os
import shutil
from late_arrivals_graph import generate_lateness_graph

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
INPUT_DIR = os.path.join(BASE_DIR, "temp_input")
OUTPUT_DIR = os.path.join(BASE_DIR, "temp_output")

print(f"Running test with:")
print(f"INPUT: {INPUT_DIR}")
print(f"OUTPUT: {OUTPUT_DIR}")

output = generate_lateness_graph(INPUT_DIR, OUTPUT_DIR)
if output:
    print(f"SUCCESS: Graph generated at {output}")
else:
    print("FAILURE: No graph generated")
