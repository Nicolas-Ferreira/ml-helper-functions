from scipy import stats
from scipy.stats import chi2_contingency
import pandas as pd
import numpy as np
import psycopg2



def median_diff(df1, df2, variable):

    """

    Run median difference test

    :param df1: dataframe to compare
    :param df2: dataframe to compare with
    :param variable: column with variable to test
    :return: String with test result

    """
    
    # Calculate the T-test for the means of two independent samples of scores
    test_t = stats.ttest_ind(
        df1[variable].tolist(),
        df2[variable].tolist(), 
        equal_var=False
    )
    
    if test_t.pvalue < 0.05:
        result = 'Significant difference'
    else:
        result = 'NOT Significant difference'
    
    return result



def prop_diff(df_full, df1, df2, variable):

    """

    Run proportion difference test

    :param df_full: dataframe with all samples
    :param df1: dataframe to compare
    :param df2: dataframe to compare with
    :param variable: column with variable to test
    :return: String with test result

    """
    
    # General dataset
    df_var = pd.DataFrame(columns=[variable])
    df_var[variable] = df_full[df_full['class'] == 1][variable].unique().tolist()
    
    # agrupo los dfs de los medios de pago
    df1_var = df_lp[df_lp['class'] == 1].groupby(variable, as_index=False).agg({'id': 'count'})
    df1_var.columns = [variable, 'frec_df1']

    df2_var = df_np[df_np['class'] == 1].groupby(variable, as_index=False).agg({'id': 'count'})
    df2_var.columns = [variable, 'frec_df2']
    
    # merge all
    df_var = pd.merge(df_var, df1_var, how='left', on=variable)
    df_var = pd.merge(df_var, df2_var, how='left', on=variable)
    df_var = df_var.fillna(0)
    df_var['frec_esp'] = df_var['frec_df1'] + df_var['frec_df2']
    df_var = df_var[df_var['frec_esp'] > 5]
    
    # Prop testing
    test_chi = chi2_contingency(df_var[['frec_df1', 'frec_df2']])
    pvalue = test_chi[1]

    if pvalue < 0.05:
        result = 'Significant difference'
    else:
        result = 'NOT Significant difference'

    return resultado



def diff_test(df_full, df1, df2, variable, variable_type):

    """

    Function that run statistical tests to compare two populations (Statistical significance)

    :param df_full: dataframe with all samples
    :param df1: dataframe to compare
    :param df2: dataframe to compare with
    :param variable: column with variable to test
    :param variable_type: column with variable type (discrete or continuous)
    :return: String with test result

    """

    if variable_type == 'cont':
        test_results = dif_medias(df1, df2, variable)

    elif variable_type == 'disc':
        test_results = dif_prop(df_full, df1, df2, variable)

    return resultado_test
