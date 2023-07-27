import pandas as pd

data_frame = pd.read_csv("data/Slice_Of_Feature_Matrix.csv")
data_frame = data_frame.sort_values(by = ["person_id", "visit_start_datetime"])
data_frame_of_events_where_patient_is_exposed_to_Opioids = data_frame[data_frame["is_exposed_to_Opioids"] == 1]
data_frame_of_reference_events = data_frame_of_events_where_patient_is_exposed_to_Opioids.drop_duplicates(subset = ["person_id", "is_exposed_to_Opioids"])

