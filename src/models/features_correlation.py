import pandas as pd
import numpy as np


def get_corr_feat_to_drop(df, cut=0.7):

    corr_matx = df.astype('float').corr().abs()
    avg_corr = corr_matx.mean(axis=1)
    up = corr_matx.where(np.triu(np.ones(corr_matx.shape), k=1).astype(np.bool))

    drop = list()

    for row in range(len(up)-1):
        col_idx = row + 1
        for col in range(col_idx, len(up)):
            if (avg_corr.iloc[row] > avg_corr.iloc[col]):
                drop.append(row)
            else:
                drop.append(col)

    drop_set = list(set(drop))
    dropcols_names = list(df.columns[[item for item in drop_set]])

    return dropcols_names
