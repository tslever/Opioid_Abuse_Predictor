import pandas as pd
import torch


def get_input_tensor_and_ground_truth(person_id, df, reference_event): # Where reference visit can either visit occurrence id or start datetime
    data_frame_corresponding_to_person_id = df[df["person_id"] == person_id]
    data_frame_corresponding_to_prior_to_reference_event = df[df["visit_startdate"] <= reference_event]
    data_frame_corresponding_to_after_reference_event = df[df[visit_startdate] > reference_event]

    will_abuse_opioids_indicator = df["has_Opioid_abuse"].max()

    # Generate input tensor
    data_frame_corresponding_to_prior_to_reference_event.drop(columns = ["person_id", "visit_occurrence_id", "visit_startdate"], inplace = True)
    numpy_array_corresponding_to_prior_to_reference_event = df.values
    num_visits_for_person = len(numpy_array_corresponding_to_prior_to_reference_event)
    num_features_for_visit = len(numpy_array_corresponding_to_prior_to_reference_event[0])
    
    input_tensor = torch.tensor(df.values) # Input tensor is now a numpy array of 2D with values from df but without columns
    input_tensor = input_tensor.resize_(1, num_visits_for_person, num_features_for_visit)

    return will_abuse_opioids_indicator, input_tensor