import pandas as pd
import random
import time
import torch


class RNN(torch.nn.Module):

    def __init__(self, input_size, number_of_elements_in_hidden_state, output_size):
        super(RNN, self).__init__()
        self.number_of_elements_in_hidden_state = number_of_elements_in_hidden_state
        self.linear_layer_between_input_and_hidden_state = torch.nn.Linear(input_size + number_of_elements_in_hidden_state, number_of_elements_in_hidden_state)
        self.linear_layer_between_hidden_state_and_output = torch.nn.Linear(number_of_elements_in_hidden_state, output_size)
        self.log_softmax_layer = torch.nn.LogSoftmax(dim = 1)

    def forward(self, input, hidden):
        combined = torch.cat((input, hidden), 1)
        hidden = self.linear_layer_between_input_and_hidden_state(combined)
        output = self.linear_layer_between_hidden_state_and_output(hidden)
        output = self.log_softmax_layer(output)
        return output, hidden

    def initialize_hidden_state(self):
        return torch.zeros(1, self.number_of_elements_in_hidden_state)

def calculate_time_interval_between_now_and_start_time(start_time):
    now = time.time()
    time_interval_in_seconds = now - start_time
    time_interval_in_whole_minutes = math.floor(time_interval_in_seconds / 60)
    time_interval_during_this_minute = time_interval_in_seconds - time_interval_in_whole_minutes * 60
    return '%dtime_interval_in_whole_minutes %dtime_interval_during_this_minute' % (time_interval_in_whole_minutes, time_interval_during_this_minute)

def create_indicator_of_whether_patient_will_abuse_opioids_to_IntegerArray_of_person_IDs(slice_of_feature_matrix):
    IntegerArray_of_distinct_person_IDs_in_slice_of_feature_matrix = pd.unique(slice_of_feature_matrix["person_id"])
    number_of_distinct_person_IDs_in_slice_of_feature_matrix = len(IntegerArray_of_distinct_person_IDs_in_slice_of_feature_matrix)
    slice_of_feature_matrix_where_has_Opioid_abuse_is_1 = slice_of_feature_matrix[slice_of_feature_matrix["has_Opioid_abuse"] == 1]
    IntegerArray_of_distinct_person_IDs_of_patients_who_have_Opioid_abuse = pd.unique(slice_of_feature_matrix_where_has_Opioid_abuse_is_1["person_id"])
    number_of_distinct_person_IDs_of_patients_who_have_Opioid_abuse = len(IntegerArray_of_distinct_person_IDs_of_patients_who_have_Opioid_abuse)
    slice_of_feature_matrix_of_patients_who_do_not_have_Opioid_abuse = slice_of_feature_matrix[~slice_of_feature_matrix["person_id"].isin(IntegerArray_of_distinct_person_IDs_of_patients_who_have_Opioid_abuse)]
    IntegerArray_of_distinct_person_IDs_of_patients_who_do_not_have_Opioid_abuse = pd.unique(slice_of_feature_matrix_of_patients_who_do_not_have_Opioid_abuse["person_id"])
    number_of_distinct_person_IDs_of_patients_who_do_not_have_Opioid_abuse = len(IntegerArray_of_distinct_person_IDs_of_patients_who_do_not_have_Opioid_abuse)
    indicator_of_whether_patient_will_abuse_opioids_to_IntegerArray_of_person_IDs = {}
    indicator_of_whether_patient_will_abuse_opioids_to_IntegerArray_of_person_IDs[0] = IntegerArray_of_distinct_person_IDs_of_patients_who_do_not_have_Opioid_abuse
    indicator_of_whether_patient_will_abuse_opioids_to_IntegerArray_of_person_IDs[1] = IntegerArray_of_distinct_person_IDs_of_patients_who_have_Opioid_abuse
    return indicator_of_whether_patient_will_abuse_opioids_to_IntegerArray_of_person_IDs

def get_random_element(the_list):
    length_of_list = len(the_list)
    random_index = random.randint(0, length_of_list - 1)
    return the_list

def get_tensor_for_person_ID(person_ID, slice_of_feature_matrix):
    slice_of_feature_matrix_corresponding_to_person_ID = slice_of_feature_matrix[slice_of_feature_matrix["person_id"] == person_ID]

    """
    data_frame_corresponding_to_prior_to_reference_event = df[df["visit_start_datetime"] <= reference_event]
    data_frame_corresponding_to_after_reference_event = df[df[visit_startdate] > reference_event]

    will_abuse_opioids_indicator = data_frame_corresponding_to_after_reference_event["has_Opioid_abuse"].max()

    # Generate input tensor
    data_frame_corresponding_to_prior_to_reference_event.drop(columns = ["person_id", "visit_occurrence_id", "visit_startdate"], inplace = True)
    numpy_array_corresponding_to_prior_to_reference_event = df.values
    num_visits_for_person = len(numpy_array_corresponding_to_prior_to_reference_event)
    num_features_for_visit = len(numpy_array_corresponding_to_prior_to_reference_event[0])

    input_tensor = torch.tensor(df.values) # Input tensor is now a numpy array of 2D with values from df but without columns
    input_tensor = input_tensor.resize_(num_visits_for_person, 1, num_features_for_visit) # TODO: consider swapping num_visits_for_person and 1

    return will_abuse_opioids_indicator, input_tensor
    """

def get_tuple_of_random_indicator_random_patient_tensor_of_index_of_random_indicator_and_tensor_of_random_patient():
    slice_of_feature_matrix = pd.read_csv("data/Slice_Of_Feature_Matrix.csv")
    indicator_of_whether_patient_will_abuse_opioids_to_IntegerArray_of_person_IDs = create_indicator_of_whether_patient_will_abuse_opioids_to_IntegerArray_of_person_IDs()
    list_of_indicators = list(indicator_of_whether_patient_will_abuse_opioids_to_IntegerArray_of_person_IDs.keys())
    random_indicator = get_random_element(list_of_indicators)
    IntegerArray_of_person_IDs = indicator_of_whether_patient_will_abuse_opioids_to_IntegerArray_of_person_IDs[random_indicator]
    random_person_ID = get_random_element(IntegerArray_of_person_IDs)
    index_of_random_indicator = list_of_indicators.index(random_indicator)
    list_of_index_of_random_indicator = [index_of_random_indicator]
    tensor_index_of_random_indicator = torch.tensor(list_of_index_of_random_indicator, dtype = torch.long)
    tensor_of_random_patient = get_tensor_for_person_ID(person_ID, slice_of_feature_matrix)

if __name__ == "__main__":
    """
    slice_of_feature_matrix = pd.read_csv("data/Slice_Of_Feature_Matrix.csv")
    print(slice_of_feature_matrix)

    indicator_of_whether_patient_will_abuse_opioids_to_IntegerArray_of_distinct_person_IDs = create_indicator_of_whether_patient_will_abuse_opioids_to_IntegerArray_of_person_IDs(slice_of_feature_matrix)
    print(indicator_of_whether_patient_will_abuse_opioids_to_IntegerArray_of_distinct_person_IDs)

    number_of_features_in_slice_of_feature_matrix = slice_of_feature_matrix.shape[1]
    number_of_elements_in_hidden_state = 128
    list_of_indicators = list(indicator_of_whether_patient_will_abuse_opioids_to_IntegerArray_of_distinct_person_IDs.keys())
    number_of_indicators = len(list_of_indicators)
    rnn = RNN(number_of_features_in_slice_of_feature_matrix, number_of_elements_in_hidden_state, number_of_indicators)
    print(rnn)
    """

    should_train = True
    if (should_train):
        start_time = time.time()
        number_of_iterations = 1
        for iteration in range(1, number_of_iterations + 1):
            random_indicator, random_patient, tensor_of_index_of_random_indicator, tensor_of_random_patient = get_tuple_of_random_indicator_random_patient_tensor_of_index_of_random_indicator_and_tensor_of_random_patient()
