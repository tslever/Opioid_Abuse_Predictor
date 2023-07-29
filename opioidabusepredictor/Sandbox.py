from opioidabusepredictor import generate_slice_of_feature_matrix as GSFM
import pandas as pd

data_frame = GSFM.get_data_frame(
GSFM.query_that_results_in_table_of_person_IDs_of_cohort
)
print(data_frame)
