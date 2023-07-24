from opioidabusepredictor import generate_feature_matrix_revisited as g
import os
import pandas as pd

data_frame = g.get_data_frame(g.query_that_results_in_feature_matrix)
print(pd.unique(data_frame["person_id"]))
print(data_frame)
deduplicated_data_frame = data_frame.drop_duplicates(subset = ["visit_occurrence_id"])
print(data_frame.shape[0] - len(deduplicated_data_frame))
