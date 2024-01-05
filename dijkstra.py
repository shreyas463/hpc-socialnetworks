import heapq
import math
from mpi4py import MPI

def parse_dataset(file_path):
    """
    Parse the dataset from the given file path and return a graph representation.

    Parameters:
    - file_path: Path to the text file containing the dataset.

    Returns:
    - graph: Dictionary representing the graph in the form {node: [(neighbor1, weight1), (neighbor2, weight2), ...]}
    """
    graph = {}
    with open(file_path, 'r') as file:
        for line in file:
            nodes = line.strip().split()
            if len(nodes) == 2:
                node1, node2 = map(int, nodes)
                if node1 not in graph:
                    graph[node1] = []
                if node2 not in graph:
                    graph[node2] = []
                # Assuming unweighted edges, set weight to 1
                graph[node1].append((node2, 1))
                graph[node2].append((node1, 1))
    return graph

def dijkstra(graph, start):
    """
    Dijkstra's algorithm to find the shortest paths from a starting node to all other nodes in a graph.

    Parameters:
    - graph: Dictionary representing the graph in the form {node: [(neighbor1, weight1), (neighbor2, weight2), ...]}
    - start: Starting node for the shortest paths

    Returns:
    - distances: Dictionary containing the shortest distances from the start node to all other nodes
    """
    distances = {node: math.inf for node in graph}
    distances[start] = 0

    priority_queue = [(0, start)]

    while priority_queue:
        current_distance, current_node = heapq.heappop(priority_queue)

        if current_distance > distances[current_node]:
            continue

        for neighbor, weight in graph[current_node]:
            distance = current_distance + weight

            if distance < distances[neighbor]:
                distances[neighbor] = distance
                heapq.heappush(priority_queue, (distance, neighbor))

    return distances

def closeness_centrality(graph, node):
    """
    Calculate closeness centrality for a node in a graph using Dijkstra's algorithm.

    Parameters:
    - graph: Dictionary representing the graph in the form {node: [(neighbor1, weight1), (neighbor2, weight2), ...]}
    - node: Node for which closeness centrality is calculated

    Returns:
    - Closeness centrality for the specified node
    """
    
    closeness_centrality = 0

    # Avoid division by zero
    if num_reachable_nodes > 0:
        closeness_centrality = num_reachable_nodes / total_distance

    return closeness_centrality

file_path = '/home/shreyas463/miniconda3/envs/hpc_assgr2/facebook_combined.txt' 
graph = parse_dataset(file_path)

# Calculate closeness centrality for each node in the graph
centrality_values = []

with open('/home/shreyas463/miniconda3/envs/hpc_assgr2/output_D_FBd_hpc.txt', 'w') as output_file:
    for node in graph:
        centrality = closeness_centrality(graph, node)
        centrality_values.append((node, centrality))
        output_file.write(f"Closeness Centrality for {node}: {centrality:.6f}\n")

    # Calculate top nodes with highest centrality
    top_nodes = sorted(centrality_values, key=lambda x: x[1], reverse=True)[:5]
    output_file.write("\nTop Nodes with Highest Centrality:\n")
    for node, centrality in top_nodes:
        output_file.write(f"{node}: {centrality:.6f}\n")

    # Calculate average centrality of all nodes
    average_centrality = sum(centrality for _, centrality in centrality_values) / len(centrality_values)
    output_file.write(f"\nAverage Centrality of All Nodes: {average_centrality:.6f}\n")
