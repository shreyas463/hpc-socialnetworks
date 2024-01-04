from mpi4py import MPI
import networkx as nx
from queue import Queue
import time 
from minheap import MinHeap

start_time = time.time()
# BUILDING GRAPH
G = nx.Graph()
# Add nodes
with open("facebook_combined.txt", 'r') as file:
    for line in file:
        vertices = list(map(int, line.strip().split()))
        G.add_edge(vertices[0], vertices[1])

# Q: Queue
# d = degree dictionnary
# s = source
# w = neighboor
# v = vertex 
def compute_closeness(G, s):
    Q = Queue()
    sum_ = 0
    d = {}
    Q.put(s)
    d[s] = 0
    while not Q.empty():
        v = Q.get()
        for w in list(G.neighbors(v)):
            # First time?
            if w not in d:
                d[w] = d[v] + 1
                sum_ = sum_ + d[w]
                Q.put(w)
    return (len(G.nodes) - 1 )/ sum_ , s  

#MPI
comm = MPI.COMM_WORLD
rank = comm.Get_rank()
size = comm.Get_size()

G_list = list(G.nodes)
n = len(G_list)
local_closeness = []
gathered_data = None

for i in range(int(n/size*rank), int(n/size*(rank+1))):
    local_closeness.append(compute_closeness(G, G_list[i]))

gathered_data = comm.gather(local_closeness, root=0)

if rank == 0:
    sum_ = 0
    print("***************** GENERATING OUTPUT.TXT *****************")
    top_5 = MinHeap(5)
    with open('output.txt', 'w') as f:
        f.write("***************************** RESULTS *****************************\n")
        for i in range(size):
            for tuple in gathered_data[i]:
                f.write("NODE: %d\tCLOSENESS-CENTRALITY: %f\n" %(tuple[1], tuple[0]))
                top_5.push(tuple)
                sum_ += tuple[0]
        f.write("***************************** TOP 5  *****************************\n")
        for i in range(5):
            tuple = top_5.pop()
            f.write("%d - NODE: %d\tCLOSENESS-CENTRALITY: %f\n" %(5-i, tuple[1], tuple[0]))

        f.write("***************************** AVERAGE *****************************\n")
        f.write("AVERAGE CENTRALITY: %f\n" %(sum_/n))
    print("DONE!")
    end_time = time.time()
    #print(end_time - start_time)