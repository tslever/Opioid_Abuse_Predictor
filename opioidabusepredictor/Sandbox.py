import pandas as pd

data_frame = pd.read_csv("data/Slice_Of_Feature_Matrix.csv")

# TODO: Tom wants to use (person_id, visit_occurrence_id) as unique primary key in feature matrix.
# Write code that ensures that ascending order by visit_occurrence_id implies
# ascending order by visit_start_date and vice versa.

data_frame_sorted_by_visit_start_date = data_frame.sort_values(by = "visit_start_date")
#data_frame_sorted_by_visit_occurrence_id = data_frame.sort_values(by = "visit_occurrence_id")
#print(data_frame_sorted_by_visit_start_date["visit_occurrence_id"].to_list() == data_frame_sorted_by_visit_occurrence_id["visit_occurrence_id"].to_list())

print(data_frame_sorted_by_visit_start_date[["person_id", "visit_occurrence_id", "visit_start_date"]])
