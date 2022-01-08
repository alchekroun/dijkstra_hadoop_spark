def get_keyvalue_from_line(node):
    """
    Transform the line in a key value tuple.
    (node_id, (distance, [neighbors], path))
    :param node:
    :return:
    """
    if len(node.split(' ')) < 3:
        node, distance = node.split(' ')
        neighbors = None
    else:
        node, distance, neighbors = node.split(' ')
        neighbors = neighbors.split(':')
        neighbors = neighbors[:len(neighbors) - 1]

    return node, (int(distance), neighbors, node)


def customSplitNodesIterative(node):
    """
    TODO REVIEW
    :param node:
    :return:
    """
    nid = node[0]
    distance = node[1][0]
    neighbors = node[1][1]
    path = node[1][2]
    elements = path.split('->')
    if elements[len(elements) - 1] != nid:
        path = path + '->' + nid
    return nid, (int(distance), neighbors, path)


def customSplitNeighbor(parent_path, parent_distance, neighbor):
    if neighbor is not None:
        node, distance = neighbor.split(',')
        distance = parent_distance + int(distance)
        path = parent_path + '->' + node
        return node, (int(distance), None, path)


def min_distance(node1, node2):
    if node1[1] != 'None':
        neighbors = node1[1]
    else:
        neighbors = node2[1]

    if node1[0] <= node2[0]:
        distance = node1[0]
        path = node1[2]
    else:
        # count.add(1)
        distance = node2[0]
        path = node2[2]

    return distance, neighbors, path


def format_result(node):
    return node[0], node[1][0], node[1][2]
