#!/usr/bin/env python3

import ast
import sys

from hadoop.dijkstra_hadoop import reducer

input_stream = sys.stdin.read().strip().replace('\n', '')

input_graph = ast.literal_eval(input_stream)

result = reducer(input_graph)

# Emit
print(result)
