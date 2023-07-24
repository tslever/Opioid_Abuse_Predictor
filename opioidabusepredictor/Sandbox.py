from opioidabusepredictor import generate_slice_of_feature_matrix as g
import os

query_that_results_in_table_criteria = """
    SELECT *
    FROM `""" + os.environ["WORKSPACE_CDR"] + """.cb_criteria` cr
    LIMIT
"""

data_frame = g.get_data_frame(query_that_results_in_table_criteria)
