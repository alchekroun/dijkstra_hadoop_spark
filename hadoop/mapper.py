#!/usr/bin/env python3

import ast
import sys

from hadoop.dijkstra_hadoop import shuffle_and_sort, mapper

input_stream = sys.stdin.read().strip().replace('\n', '')

input_graph = ast.literal_eval(input_stream)

result = mapper(input_graph)

# Emit
print(shuffle_and_sort(result))
