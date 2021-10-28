import pandas as pd
import networkx as nx 
import matplotlib.pyplot as plt

def plot_and_save_graph(df_edge, source, target, filename, x_size, y_size, title):

    """

    Plot Graph, save to file and return Graph
    
    :param df_edge: Dataframe with data
    :param source: column name for source
    :param target: column name for target
    :param filename: output filename
    :param x_size: x fig size
    :param y_size: y fig size
    :param title: fig title
    :return: Networkx graph object with graph

    """

    # PLOT GRAPH AND SAVE.png
    plt.figure(figsize=(x_size, y_size))

    # 1. Create graph
    G = nx.from_pandas_edgelist(df_edge
                                , source=source
                                , target=target
                                )

    # 2. Create a layout for our nodes 
    layout = nx.spring_layout(G,iterations=50)

    # Lists
    list_source = list(df_edge[source].unique())
    list_target = list(df_edge[target].unique())

    # DRAW Nodes    
    nx.draw_networkx_nodes(G, 
                           layout, 
                           nodelist=list_target, 
                           node_color='lightblue', 
                           node_size=100)

    nx.draw_networkx_nodes(G, 
                           layout, 
                           nodelist=list_source, 
                           node_color='green', 
                           node_size=100)

    # DRAW EDGES
    nx.draw_networkx_edges(G, 
                           layout, 
                           width=1, 
                           edge_color="#cccccc")

    # DRAW NODE LABELS
    node_labels = pd.Series(df_edge.source.values, 
                            index=df_edge.source).to_dict()
    nx.draw_networkx_labels(G, 
                           layout, 
                           labels=node_labels)

    node_labels_disp = pd.Series(df_edge.target.values, 
                                index=df_edge.target).to_dict()
    nx.draw_networkx_labels(G, 
                            layout, 
                            labels=node_labels_disp)

    plt.axis('off')
    plt.title(title)

    if filename != None:
        plt.savefig(filename)

    return G
