import pandas as pd
import numpy as np
import networkx as nx 


def build_graph(df, source_column, target_column):
    
    """

    Function to build graph from dataframe

    :param df: Dataframe with data to build graph
    :param source_column: Source column
    :param target_column: Target column
    :return G, sG: Complete graph and subgraphs objects

    """

    # Graph EDGES
    df_edge = df[[source_column, source_column]]
    df_edge.columns = ['source', 'target']
    df_edge = df_edge[df_edge.source != '']
    df_edge = df_edge[df_edge.target != '']
    df_edge = df_edge[df_edge.source.notnull()]
    df_edge = df_edge[df_edge.target.notnull()]
    df_edge = df_edge[df_edge.source != df_edge.target]

    # Drop duplicates
    df_edge = df_edge.drop_duplicates()
    
    # Build graph
    G = nx.from_pandas_edgelist(
      df_edge,
      source='source',
      target='target'
    )

    # Identify subgraphs
    sG = nx.connected_component_subgraphs(G)
    
    return G, sG
    