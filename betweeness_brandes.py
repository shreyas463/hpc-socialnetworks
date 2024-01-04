import networkx as nx
from queue import Queue
from minheap import MinHeap

G = nx.Graph()

with open("facebook_combined.txt", 'r') as file:
    for line in file:
        vertices = list(map(int, line.strip().split()))
        G.add_edge(vertices[0], vertices[1])

# C : Betweeness Centrality
# P : Predecessors
# V: set of Vertices
# s: starting node
# t: end node
V = list(G.nodes)
C = {v:0 for v in V}
for s in V:
    print(s)
    S = []
    P = {w:[] for w in V}
    sigma = dict((t, 0) for t in V); sigma[s] = 1
    d = {t:-1 for t in V}; d[s] = 0
    Q = Queue()
    Q.put(s)
    while not Q.empty():
        v = Q.get()
        S.append(v)
        for w in list(G.neighbors(v)):
            if d[w] < 0:
                Q.put(w)
                d[w] = d[v] + 1
            if d[w] == d[v] + 1:
                sigma[w] = sigma[w] + sigma[v]
                P[w].append(v)
    pair_dependency = {v:0 for v in V}
    while S:
        w = S.pop()
        for v in P[w]:
            pair_dependency[v] = pair_dependency[v] + (sigma[v]/sigma[w]) * (1 + pair_dependency[w])
        if w != s:
            C[w] = C[w] + pair_dependency[w]

sum_ = 0
n = len(V)
normalization = (n-1)*(n-2) / 2.0
with open('output.txt', 'w') as f:
    print("***************** GENERATING OUTPUT.TXT *****************")
    top_5 = MinHeap(5)
    with open('output.txt', 'w') as f:
        f.write("***************************** RESULTS *****************************\n")
        for key in C:
            betweeness = C[key]/normalization
            f.write("NODE: %d\tBETWEENESS-CENTRALITY: %f\n" %(key, betweeness))
            top_5.push((betweeness, key))
            sum_ += betweeness
        f.write("***************************** TOP 5  *****************************\n")
        for i in range(5):
            tuple = top_5.pop()
            f.write("%d - NODE: %d\tBETWEENESS-CENTRALITY: %f\n" %(5-i, tuple[1], tuple[0]))

        f.write("***************************** AVERAGE *****************************\n")
        f.write("AVERAGE CENTRALITY: %f\n" %(sum_/n))
    print("DONE!")
