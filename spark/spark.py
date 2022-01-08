import ast
import re


def prepare_data(node):
    """

    :param node:
    :return:
    """
    line_processed = node.strip().replace(':', ',', 1)
    line_processed = re.sub(r'[{}]', '', line_processed)
    node = ast.literal_eval(line_processed)
    # init first path item
    node[1][2] = [node[0]]
    return node


def process_neighbors(parent_path, parent_distance, neighbor):
    """

    :param parent_distance:
    :param neighbor:
    :param parent_path:
    :return:
    """
    if len(neighbor) > 1:
        distance = parent_distance + neighbor[1]
        path = parent_path[:] + [neighbor[0]]
        return neighbor[0], (distance, None, path)


def min_distance(node1, node2):
    """

    :param node1:
    :param node2:
    :return:
    """
    neighbors = node1[1] if node1[1] is not None else node2[1]
    if node1[0] <= node2[0]:
        distance = node1[0]
        path = node1[2]
    else:
        distance = node2[0]
        path = node2[2]
    return distance, neighbors, path
