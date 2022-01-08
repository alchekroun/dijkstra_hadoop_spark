import pytest

import hadoop.dijkstra_hadoop as dh


@pytest.mark.parametrize(
    'input_graph, max_node',
    (
            ([
                 [1, 2, 10],
                 [1, 3, 5],
                 [2, 3, 2],
                 [2, 4, 1],
                 [3, 2, 3],
                 [3, 4, 9],
                 [3, 5, 2],
                 [4, 5, 4],
                 [5, 1, 7],
                 [5, 4, 6],
                 [5, 6, 2]
             ], 6),
            ([
                 [1, 2, 10],
             ], 2),
            ([
                 [1, 2, 10],
                 [1, 3, 5],
                 [2, 3, 2],
                 [2, 4, 1],
                 [3, 2, 3],
                 [3, 4, 9],
                 [3, 5, 2],
                 [4, 5, 4],
                 [5, 1, 7],
                 [5, 4, 6],
             ], 5),
    )
)
def test_prepare_data(input_graph, max_node):
    # WHEN
    result = dh.prepare_data(input_graph)

    # THEN
    assert isinstance(result, dict), 'Wrong return type'
    assert (result[1][0] == 0), 'The first node must have a Zero value distance'
    for node in result.items():
        if node[0] > 1:
            assert (node[1][0] == 9999), 'Every node but the first must have infinite value distance'
    assert (result[max_node]), 'Not all nodes have been created'


@pytest.mark.parametrize(
    'input_graph, max_node, possibility',
    (
            ({
                 1: [0, [[2, 10], [3, 5]], []],
                 2: [float('inf'), [[3, 2], [4, 1]], []],
                 3: [float('inf'), [[2, 3], [4, 9], [5, 2]], []],
                 4: [float('inf'), [[5, 4]], []],
                 5: [float('inf'), [[1, 7], [4, 6], [6, 2]], []],
                 6: [float('inf'), [], []]
             }, 6, 17),
            ({
                 1: [0, [[2, 7], [3, 9], [4, 14]], []],
                 2: [float('inf'), [[3, 10], [5, 15]], []],
                 3: [float('inf'), [[5, 11], [4, 2]], []],
                 4: [float('inf'), [[6, 9]], []],
                 5: [float('inf'), [[6, 6]], []],
                 6: [float('inf'), [], []]
             }, 6, 15),
    )
)
def test_mapper(input_graph, max_node, possibility):
    # WHEN
    result = dh.mapper(input_graph)
    # THEN
    assert (len(result) == possibility), 'Every possibility have not been calculated'
    for node in result:
        if node[2] is None:
            assert (len(node[3]) == 2), 'It returns wrong path for possibility'


@pytest.mark.parametrize(
    'input_graph, expected_result',
    (
            ({
                 1: [[0, [[2, 10], [3, 5]], [1]], [float('inf'), None, [5, 1]]],
                 2: [[10, None, [1, 2]], [float('inf'), [[3, 2], [4, 1]], [2]], [float('inf'), None, [3, 2]]],
                 3: [[5, None, [1, 3]],
                     [float('inf'), None, [2, 3]],
                     [float('inf'), [[2, 3], [4, 9], [5, 2]], [3]]],
                 4: [[float('inf'), None, [2, 4]],
                     [float('inf'), None, [3, 4]],
                     [float('inf'), [[5, 4]], [4]],
                     [float('inf'), None, [5, 4]]],
                 5: [[float('inf'), None, [3, 5]],
                     [float('inf'), None, [4, 5]],
                     [float('inf'), [[1, 7], [4, 6], [6, 2]], [5]]],
                 6: [[float('inf'), None, [5, 6]], [float('inf'), [], [6]]]
             },
             {
                 1: [0, [[2, 10], [3, 5]], [1]],
                 2: [10, [[3, 2], [4, 1]], [1, 2]],
                 3: [5, [[2, 3], [4, 9], [5, 2]], [1, 3]],
                 4: [float('inf'), [[5, 4]], []],
                 5: [float('inf'), [[1, 7], [4, 6], [6, 2]], []],
                 6: [float('inf'), [], []]
             }),
    )
)
def test_reducer(input_graph, expected_result):
    # WHEN
    result = dh.reducer(input_graph)
    # THEN
    assert (result == expected_result)


@pytest.mark.parametrize(
    'input_graph, expected_result',
    (
            ([
                 [1, 0, [[2, 10], [3, 5]], [1]],
                 [2, 10, None, [1, 2]],
                 [3, 5, None, [1, 3]],
                 [2, float('inf'), [[3, 2], [4, 1]], [2]],
                 [3, float('inf'), None, [2, 3]],
                 [4, float('inf'), None, [2, 4]],
                 [3, float('inf'), [[2, 3], [4, 9], [5, 2]], [3]],
                 [2, float('inf'), None, [3, 2]],
                 [4, float('inf'), None, [3, 4]],
                 [5, float('inf'), None, [3, 5]],
                 [4, float('inf'), [[5, 4]], [4]],
                 [5, float('inf'), None, [4, 5]],
                 [5, float('inf'), [[1, 7], [4, 6], [6, 2]], [5]],
                 [1, float('inf'), None, [5, 1]],
                 [4, float('inf'), None, [5, 4]],
                 [6, float('inf'), None, [5, 6]],
                 [6, float('inf'), [], [6]]
             ],
             {
                 1: [[0, [[2, 10], [3, 5]], [1]], [float('inf'), None, [5, 1]]],
                 2: [[10, None, [1, 2]], [float('inf'), [[3, 2], [4, 1]], [2]], [float('inf'), None, [3, 2]]],
                 3: [[5, None, [1, 3]],
                     [float('inf'), None, [2, 3]],
                     [float('inf'), [[2, 3], [4, 9], [5, 2]], [3]]],
                 4: [[float('inf'), None, [2, 4]],
                     [float('inf'), None, [3, 4]],
                     [float('inf'), [[5, 4]], [4]],
                     [float('inf'), None, [5, 4]]],
                 5: [[float('inf'), None, [3, 5]],
                     [float('inf'), None, [4, 5]],
                     [float('inf'), [[1, 7], [4, 6], [6, 2]], [5]]],
                 6: [[float('inf'), None, [5, 6]], [float('inf'), [], [6]]]
             }),
            ([
                 [1, 0, [[2, 7], [3, 9], [4, 14]], [1]],
                 [2, 7, None, [1, 2]],
                 [3, 9, None, [1, 3]],
                 [4, 14, None, [1, 4]],
                 [2, float('inf'), [[3, 10], [5, 15]], [2]],
                 [3, float('inf'), None, [2, 3]],
                 [5, float('inf'), None, [2, 5]],
                 [3, float('inf'), [[5, 11], [4, 2]], [3]],
                 [5, float('inf'), None, [3, 5]],
                 [4, float('inf'), None, [3, 4]],
                 [4, float('inf'), [[6, 9]], [4]],
                 [6, float('inf'), None, [4, 6]],
                 [5, float('inf'), [[6, 6]], [5]],
                 [6, float('inf'), None, [5, 6]],
                 [6, float('inf'), [], [6]]
             ], {
                 1: [[0, [[2, 7], [3, 9], [4, 14]], [1]]],
                 2: [[7, None, [1, 2]],
                     [float('inf'), [[3, 10], [5, 15]], [2]]],
                 3: [[9, None, [1, 3]],
                     [float('inf'), None, [2, 3]],
                     [float('inf'), [[5, 11], [4, 2]], [3]]],
                 4: [[14, None, [1, 4]],
                     [float('inf'), None, [3, 4]],
                     [float('inf'), [[6, 9]], [4]]],
                 5: [[float('inf'), None, [2, 5]],
                     [float('inf'), None, [3, 5]],
                     [float('inf'), [[6, 6]], [5]]],
                 6: [[float('inf'), None, [4, 6]],
                     [float('inf'), None, [5, 6]],
                     [float('inf'), [], [6]]]
             })
    )
)
def test_shuffle_and_sort(input_graph, expected_result):
    # WHEN
    result = dh.shuffle_and_sort(input_graph)
    # THEN
    assert (result == expected_result)


@pytest.mark.parametrize(
    'input_graph, expected_result',
    (
            ([
                 [1, 2, 10],
                 [1, 3, 5],
                 [2, 3, 2],
                 [2, 4, 1],
                 [3, 2, 3],
                 [3, 4, 9],
                 [3, 5, 2],
                 [4, 5, 4],
                 [5, 1, 7],
                 [5, 4, 6],
                 [5, 6, 2]
             ],
             {
                 1: [0, [[2, 10], [3, 5]], [1]],
                 2: [8, [[3, 2], [4, 1]], [1, 3, 2]],
                 3: [5, [[2, 3], [4, 9], [5, 2]], [1, 3]],
                 4: [9, [[5, 4]], [1, 3, 2, 4]],
                 5: [7, [[1, 7], [4, 6], [6, 2]], [1, 3, 5]],
                 6: [9, [], [1, 3, 5, 6]]
             }),
            ([
                 [1, 2, 7],
                 [1, 3, 9],
                 [1, 4, 14],
                 [2, 3, 10],
                 [2, 5, 15],
                 [3, 5, 11],
                 [3, 4, 2],
                 [4, 6, 9],
                 [5, 6, 6],
             ], {
                 1: [0, [[2, 7], [3, 9], [4, 14]], [1]],
                 2: [7, [[3, 10], [5, 15]], [1, 2]],
                 3: [9, [[5, 11], [4, 2]], [1, 3]],
                 4: [11, [[6, 9]], [1, 3, 4]],
                 5: [20, [[6, 6]], [1, 3, 5]],
                 6: [20, [], [1, 3, 4, 6]]
             }),
    )
)
def test_dijkstra(input_graph, expected_result):
    # WHEN
    result = dh.dijkstra(input_graph)
    # THEN
    assert (result == expected_result), 'Wrong answer given by the function'
