from textblob import TextBlob
import pickle
import networkx as nx
import numpy as np
import random

def power_method(mat, start, maxit):
    result = start
    for i in xrange(maxit):
        result = mat*result
        result = result/np.linalg.norm(result)
    return result

def compute_score(p, vl, vr):
    vl_dict = {}
    vr_dict = {}
    vl_dict = {p.nodes()[i]:abs(float(vl[i]))*1e50 for i in range(0, len(vl))}
    vr_dict = {p.nodes()[i]:abs(float(vr[i]))*1e50 for i in range(0, len(vr))}
    v_dict = {(start, end): vl_dict[start]*vr_dict[start] for (start, end) in p.edges()}
    return v_dict

# construct network
f = open('iphone_ipad_v1','r')
data = pickle.load(f)
f.close()

pos_net = {}
neg_net = {}
tot_net = {}
n=0
for item in data:
    try:
        item[2] = item[2].decode('unicode_escape')
    except (UnicodeDecodeError, UnicodeEncodeError):
        continue
    n += 1
    if n > 40000:
        break
    polarity = (TextBlob(item[2]).sentiment[0])
    for affecter in item[0]:
        for affectee in item[1]:
            if polarity > 0:
                if (affecter, affectee) in pos_net:
                    pos_net[(affecter, affectee)] += polarity
                else:
                    pos_net[(affecter, affectee)] = polarity
            elif polarity < 0:
                if (affecter, affectee) in neg_net:
                    neg_net[(affecter, affectee)] += polarity
                else:
                    neg_net[(affecter, affectee)] = polarity

tot_net = list(set([(affecter, affectee) for (affecter, affectee) in pos_net] + [(affecter, affectee) for (affecter, affectee) in neg_net]))
pos_net = [(affecter, affectee, pos_net[(affecter, affectee)]) for (affecter, affectee) in pos_net]
neg_net = [(affecter, affectee, neg_net[(affecter, affectee)]) for (affecter, affectee) in neg_net]


del pos_net



# here I treat the entire graph as the positive graph
TG = nx.DiGraph()
TG.add_edges_from(tot_net)

del tot_net
nodes = nx.weakly_connected_component_subgraphs(TG)[0].nodes()
TG_LC_graph = TG.subgraph(nodes)
TG_LC_mat = nx.adjacency_matrix(TG_LC_graph, nodelist=nodes, weight='weight')
start = np.ones((len(TG_LC_mat),1))
right_eigenvector = power_method(TG_LC_mat, start, 100)
left_eigenvector = power_method(TG_LC_mat.T, start, 100)
TG_score = compute_score(TG_LC_graph, left_eigenvector, right_eigenvector)

del TG, TG_LC_graph, TG_LC_mat

NG = nx.DiGraph()
NG.add_weighted_edges_from(neg_net)
ng_nodes = list(set(nodes)&set(NG.nodes()))
NG_LC_graph = NG.subgraph(ng_nodes)
NG_LC_mat = nx.adjacency_matrix(NG_LC_graph, nodelist=ng_nodes, weight='weight')
start = np.ones((len(NG_LC_mat),1))
right_eigenvector = power_method(NG_LC_mat, start, 100)
left_eigenvector = power_method(NG_LC_mat.T, start, 100)
NG_score = compute_score(NG_LC_graph, left_eigenvector, right_eigenvector)

del neg_net, NG, NG_LC_graph, NG_LC_mat

T_score = {}
for edge in TG_score:
    if edge not in T_score:
        T_score[edge] = {}
    T_score[edge]['p'] = TG_score[edge]

for edge in NG_score:
    if edge not in T_score:
        T_score[edge] = {}
    T_score[edge]['n'] = NG_score[edge]

for edge in T_score:
    if 'p' not in T_score[edge]:
        T_score[edge]['t'] = T_score[edge]['n']
        continue
    if 'n' not in T_score[edge]:
        T_score[edge]['t'] = -T_score[edge]['p']
        continue
    T_score[edge]['t'] = T_score[edge]['n']-T_score[edge]['p']

T_score = [(edge, T_score[edge]) for edge in T_score]
T_score.sort(key=lambda item:item[1]['t'], reverse=True)
top20 = T_score[0:20]
for t in top20:
    for item in data:
        if t[0][0] in item[0] and t[0][1] in item[1]:
            if 'text' in t[1]:
                t[1]['text'] += item[2]
            else:
                t[1]['text'] = item[2]
            break
