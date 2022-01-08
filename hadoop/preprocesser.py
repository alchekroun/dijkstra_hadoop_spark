#!/usr/bin/env python3

import sys
import ast

from hadoop.dijkstra_hadoop import prepare_data

input_stream = sys.stdin.read().strip().replace('\n', '')

input_graph = ast.literal_eval(input_stream)

print(prepare_data(input_graph))
