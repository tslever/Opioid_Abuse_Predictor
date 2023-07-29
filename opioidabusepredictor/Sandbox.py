from opioidabusepredictor import generate_slice_of_feature_matrix as GSFM
import pandas as pd

data_frame = GSFM.get_data_frame(
GSFM.query_that_results_in_feature_matrix
)
print(data_frame)
