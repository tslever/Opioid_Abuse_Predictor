import math
import matplotlib.pyplot as plt
import pandas as pd
import random
import time
import torch

# -------
# Level 3
# -------

def get_table_of_distinct_person_IDs_and_visit_start_datetimes_of_reference_events():
    slice_of_feature_matrix_where_is_exposed_to_Opioids_is_1 = feature_matrix[feature_matrix["is_exposed_to_Opioids"] == 1]
    table_of_distinct_person_IDs_and_visit_start_datetimes_of_reference_events = slice_of_feature_matrix_where_is_exposed_to_Opioids_is_1.drop_duplicates(subset = ["person_id"])
    table_of_distinct_person_IDs_and_visit_start_datetimes_of_reference_events = table_of_distinct_person_IDs_and_visit_start_datetimes_of_reference_events[["person_id", "visit_start_datetime"]]
    table_of_distinct_person_IDs_and_visit_start_datetimes_of_reference_events = table_of_distinct_person_IDs_and_visit_start_datetimes_of_reference_events.rename(columns = {'person_id': 'person_id', 'visit_start_datetime': 'visit_start_datetime_of_reference_event'})
    return table_of_distinct_person_IDs_and_visit_start_datetimes_of_reference_events

# -------
# Level 2
# -------

# For example, this dictionary looks like {0: IntegerArray(0, 1000, 2000), 1: IntegerArray(3000, 4000, 5000)}
def create_dictionary_of_indicators_of_whether_patient_will_abuse_opioids_and_IntegerArrays_of_distinct_person_IDs():
    table_of_distinct_person_IDs_and_visit_start_datetimes_of_reference_events = get_table_of_distinct_person_IDs_and_visit_start_datetimes_of_reference_events()
    feature_matrix_with_column_of_visit_start_datetimes_of_reference_event = pd.merge(feature_matrix, table_of_distinct_person_IDs_and_visit_start_datetimes_of_reference_events, on = "person_id", how = "left")
    feature_matrix_of_events_after_reference_events = feature_matrix_with_column_of_visit_start_datetimes_of_reference_event[feature_matrix_with_column_of_visit_start_datetimes_of_reference_event["visit_start_datetime"] > feature_matrix_with_column_of_visit_start_datetimes_of_reference_event["visit_start_datetime_of_reference_event"]]
    feature_matrix_of_events_after_reference_events = feature_matrix_of_events_after_reference_events.drop(columns = ["visit_start_datetime_of_reference_event"])
    slice_of_feature_matrix_where_has_Opioid_abuse_is_1 = feature_matrix_of_events_after_reference_events[feature_matrix_of_events_after_reference_events["has_Opioid_abuse"] == 1]
    IntegerArray_of_distinct_person_IDs_of_patients_who_will_have_Opioid_abuse = pd.unique(slice_of_feature_matrix_where_has_Opioid_abuse_is_1["person_id"])    
    slice_of_feature_matrix_of_patients_who_will_not_have_Opioid_abuse = feature_matrix[~feature_matrix["person_id"].isin(IntegerArray_of_distinct_person_IDs_of_patients_who_will_have_Opioid_abuse)]
    IntegerArray_of_distinct_person_IDs_of_patients_who_will_not_have_Opioid_abuse = pd.unique(slice_of_feature_matrix_of_patients_who_will_not_have_Opioid_abuse["person_id"])
    dictionary_of_indicators_of_whether_patient_will_abuse_opioids_and_IntegerArrays_of_distinct_person_IDs = {}
    dictionary_of_indicators_of_whether_patient_will_abuse_opioids_and_IntegerArrays_of_distinct_person_IDs[0.0] = IntegerArray_of_distinct_person_IDs_of_patients_who_will_not_have_Opioid_abuse
    dictionary_of_indicators_of_whether_patient_will_abuse_opioids_and_IntegerArrays_of_distinct_person_IDs[1.0] = IntegerArray_of_distinct_person_IDs_of_patients_who_will_have_Opioid_abuse
    return dictionary_of_indicators_of_whether_patient_will_abuse_opioids_and_IntegerArrays_of_distinct_person_IDs

def get_random_element(iterable):
    length_of_list = len(iterable)
    random_index = random.randint(0, length_of_list - 1)
    return iterable[random_index]

def get_tensor_corresponding_to_person_ID(person_ID, feature_matrix):
    slice_of_feature_matrix_corresponding_to_person_ID = feature_matrix[feature_matrix["person_id"] == person_ID]
    slice_of_feature_matrix_corresponding_to_person_ID = slice_of_feature_matrix_corresponding_to_person_ID.drop(columns = ["person_id"])
    visit_start_datetime_of_reference_event = slice_of_feature_matrix_corresponding_to_person_ID[slice_of_feature_matrix_corresponding_to_person_ID["is_exposed_to_Opioids"] == 1].reset_index(drop = True).at[0, "visit_start_datetime"]
    slice_of_feature_matrix_corresponding_to_person_ID = slice_of_feature_matrix_corresponding_to_person_ID.drop(columns = ["is_exposed_to_Opioids"])
    slice_of_feature_matrix_corresponding_to_person_ID_prior_to_reference_event = slice_of_feature_matrix_corresponding_to_person_ID[slice_of_feature_matrix_corresponding_to_person_ID["visit_start_datetime"] < visit_start_datetime_of_reference_event]
    slice_of_feature_matrix_corresponding_to_person_ID_prior_to_reference_event_with_only_columns_of_positive_indicators = slice_of_feature_matrix_corresponding_to_person_ID_prior_to_reference_event.drop(columns = ["visit_start_datetime"])
    tensor_corresponding_to_person_ID_prior_to_reference_event = torch.tensor(slice_of_feature_matrix_corresponding_to_person_ID_prior_to_reference_event_with_only_columns_of_positive_indicators.values, dtype = torch.float32)
    number_of_visits_for_patient = slice_of_feature_matrix_corresponding_to_person_ID_prior_to_reference_event_with_only_columns_of_positive_indicators.shape[0]
    number_of_features = slice_of_feature_matrix_corresponding_to_person_ID_prior_to_reference_event_with_only_columns_of_positive_indicators.shape[1]
    tensor_corresponding_to_person_ID_prior_to_reference_event = tensor_corresponding_to_person_ID_prior_to_reference_event.resize_(number_of_visits_for_patient, 1, number_of_features) # TODO: consider swapping num_visits_for_person and 1tensor_corresponding_to_person_ID_prior_to_reference_event
    return tensor_corresponding_to_person_ID_prior_to_reference_event

class RNN(torch.nn.Module):

    def __init__(self, number_of_features, number_of_elements_in_hidden_state, number_of_all_indicators):
        super(RNN, self).__init__()
        self.number_of_elements_in_hidden_state = number_of_elements_in_hidden_state
        self.linear_layer_between_input_and_hidden_state = torch.nn.Linear(number_of_features + number_of_elements_in_hidden_state, number_of_elements_in_hidden_state)
        # Why torch.nn.Linear have two parameters?
        # torch.nn.Linear is informed of the number of elements of the input of this layer
        # and the number of elements of the output of this layer. The output of torch.nn.Linear is the hidden state.
        # TODO: Why are provided the number of elements of the input of this layer and the number of elements of the output of this layer?
        self.linear_layer_between_hidden_state_and_output = torch.nn.Linear(number_of_elements_in_hidden_state, number_of_all_indicators)
        self.log_softmax_layer = torch.nn.LogSoftmax(dim = 1)

    def forward(self, input_event, hidden_state):
        tensor_representing_combination_of_input_event_and_hidden_state = torch.cat((input_event, hidden_state), 1)
        hidden_state = self.linear_layer_between_input_and_hidden_state(tensor_representing_combination_of_input_event_and_hidden_state)
        output = self.linear_layer_between_hidden_state_and_output(hidden_state)
        output = self.log_softmax_layer(output)
        return output, hidden_state

    def initialize_hidden_state(self):
        return torch.zeros(1, self.number_of_elements_in_hidden_state)

# -------
# Level 1
# -------

def get_tuple_of_random_indicator_tensor_of_index_of_random_indicator_and_tensor_of_random_patient():
    random_indicator = get_random_element(list_of_all_indicators)
    index_of_random_indicator = list_of_all_indicators.index(random_indicator)
    list_of_index_of_random_indicator = [index_of_random_indicator]
    tensor_of_index_of_random_indicator = torch.tensor(list_of_index_of_random_indicator, dtype = torch.long)
    # TODO: Why do we have a tensor of index of random indicator and not a tensor of random indicator?
    IntegerArray_of_distinct_person_IDs = dictionary_of_indicators_of_whether_patient_will_abuse_opioids_and_IntegerArrays_of_person_IDs[random_indicator]
    random_person_ID = get_random_element(IntegerArray_of_distinct_person_IDs)
    tensor_of_random_patient = get_tensor_corresponding_to_person_ID(random_person_ID, feature_matrix)
    return (random_indicator, tensor_of_index_of_random_indicator, tensor_of_random_patient)

def get_value_of_dimension(tensor, dimension):
    size_of_tensor = tensor.size()
    value_of_dimension = size_of_tensor[dimension]
    return value_of_dimension

def train(tensor_of_index_of_random_indicator, tensor_of_random_patient):
    hidden_state = rnn.initialize_hidden_state()
    # A hidden state is a repository of information from previous events.
    # A hidden state is a vector that is computed with input information.
    # TODO: Why does a hidden state have 128 elements?
    rnn.zero_grad()
    number_of_events_for_random_patient = get_value_of_dimension(tensor_of_random_patient, 0)
    for i in range(0, number_of_events_for_random_patient):
        event = tensor_of_random_patient[i]
        tensor_of_probabilities_that_patient_corresponds_to_indicator, hidden_state = rnn(event, hidden_state)
    loss = criterion(tensor_of_probabilities_that_patient_corresponds_to_indicator, tensor_of_index_of_random_indicator)
    loss.backward()
    for parameter in rnn.parameters():
        parameter.data.add_(parameter.grad.data, alpha = -learning_rate)
    return tensor_of_probabilities_that_patient_corresponds_to_indicator, loss.item()

def get_tuple_of_most_likely_indicator_and_its_index_in_list_of_all_indicators(tensor_of_probabilities_that_name_corresponds_to_indicator):
    the_topk = tensor_of_probabilities_that_name_corresponds_to_indicator.topk(k = 1)
    # TODO: Consider renaming the_topk.
    tensor_of_indices_of_most_likely_indicators = the_topk[1]
    tensor_of_index_of_most_likely_indicator = tensor_of_indices_of_most_likely_indicators[0]
    index_of_most_likely_indicator = tensor_of_index_of_most_likely_indicator.item()
    most_likely_indicator = list_of_all_indicators[index_of_most_likely_indicator]
    tuple_of_most_likely_indicator_and_its_index_in_list_of_all_indicators = (most_likely_indicator, index_of_most_likely_indicator)
    return tuple_of_most_likely_indicator_and_its_index_in_list_of_all_indicators

def calculate_time_interval_between_now_and_start_time(start_time):
    now = time.time()
    time_interval_in_seconds = now - start_time
    time_interval_in_whole_minutes = math.floor(time_interval_in_seconds / 60)
    time_interval_during_this_minute = time_interval_in_seconds - time_interval_in_whole_minutes * 60
    return '%dtime_interval_in_whole_minutes %dtime_interval_during_this_minute' % (time_interval_in_whole_minutes, time_interval_during_this_minute)

def train_RNN():
    if should_train:
        sum_of_losses = 0
        start_time = time.time()
        for iteration in range(1, number_of_iterations + 1):
            random_indicator, tensor_of_index_of_random_indicator, tensor_of_random_patient = get_tuple_of_random_indicator_tensor_of_index_of_random_indicator_and_tensor_of_random_patient()
            if get_value_of_dimension(tensor_of_random_patient, 0) == 0:
                print("FOUND TENSOR WITH ZERO INFO")
                break
                continue
            tensor_of_probabilities_that_name_corresponds_to_indicator, loss = train(tensor_of_index_of_random_indicator, tensor_of_random_patient)
            sum_of_losses += loss
            if iteration % number_of_iterations_after_which_to_print == 0:
                predicted_indicator, _ = get_tuple_of_most_likely_indicator_and_its_index_in_list_of_all_indicators(tensor_of_probabilities_that_name_corresponds_to_indicator)
                indicator_of_whether_prediction_is_correct = '✓' if predicted_indicator == random_indicator else '✗ (%s)' % random_indicator
                progress = iteration / number_of_iterations * 100
                elapsed_time = calculate_time_interval_between_now_and_start_time(start_time)
                print('%d %d%% (%s) %.4f / %s %s' % (iteration, progress, elapsed_time, loss, predicted_indicator, indicator_of_whether_prediction_is_correct))
            if iteration % number_of_iterations_after_which_to_plot == 0:
                average_loss = sum_of_losses / number_of_iterations_after_which_to_plot
                list_of_average_losses.append(average_loss)
                sum_of_losses = 0
        torch.save(rnn.state_dict(), 'data/Opioid_Abuse_Predictor.pt')
        plt.figure()
        plt.plot(list_of_average_losses)
        plt.show()
    else:
        rnn.load_state_dict(torch.load('data/Opioid_Abuse_Predictor.pt'))

# -------
# Level 0
# -------

feature_matrix = pd.read_csv("data/Feature_Matrix.csv", index_col = 0)
feature_matrix = feature_matrix.fillna(0)
feature_matrix = feature_matrix.sort_values(by = ["person_id", "visit_start_datetime"])
dictionary_of_indicators_of_whether_patient_will_abuse_opioids_and_IntegerArrays_of_person_IDs = create_dictionary_of_indicators_of_whether_patient_will_abuse_opioids_and_IntegerArrays_of_distinct_person_IDs()
feature_matrix = feature_matrix.drop(columns = ["has_Opioid_abuse"])
list_of_all_indicators = list(dictionary_of_indicators_of_whether_patient_will_abuse_opioids_and_IntegerArrays_of_person_IDs.keys())
number_of_features = feature_matrix.drop(columns = ["person_id", "visit_start_datetime", "is_exposed_to_Opioids"]).shape[1]
number_of_elements_in_hidden_state = 128
number_of_all_indicators = len(list_of_all_indicators)
rnn = RNN(number_of_features, number_of_elements_in_hidden_state, number_of_all_indicators)

should_train = True
number_of_iterations = 10000
criterion = torch.nn.NLLLoss()
learning_rate = 0.005
number_of_iterations_after_which_to_print = 100 #5000
number_of_iterations_after_which_to_plot = 100 #1000
list_of_average_losses = []

if __name__ == "__main__":
    train_RNN()
