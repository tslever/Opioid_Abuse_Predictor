import torch
import torch.nn as nn 
import matplotlib.pyplot as plt 
import random
import pandas as pd

class RNN(nn.Module):
    # implement RNN from scratch rather than using nn.RNN
    def __init__(self, input_size, hidden_size, output_size): # Scalars
        super(RNN, self).__init__()
        
        self.hidden_size = hidden_size
        self.i2h = nn.Linear(input_size + hidden_size, hidden_size)
        self.i2o = nn.Linear(input_size + hidden_size, output_size)
        self.softmax = nn.LogSoftmax(dim=1)
        
    def forward(self, input_tensor, hidden_tensor):
        combined = torch.cat((input_tensor, hidden_tensor), 1)
        
        hidden = self.i2h(combined)
        output = self.i2o(combined)
        output = self.softmax(output)
        return output, hidden
    
    def init_hidden(self):
        return torch.zeros(1, self.hidden_size) # 2D vector, THUS INPUT SIZE IS A SCALAR CORRESPONDING TO 1ST dimension, not the 0th one


def category_from_output(output):
    category_idx = torch.argmax(output).item()
    return all_categories[category_idx]


def train(person_tensor, category_tensor):     # LINE_TENSOR represents a patient's 3D feature matrix of size: (num_visits, 1, num_features), I belive category_tensor is ground truth
    hidden = rnn.init_hidden()
    
    for i in range(person_tensor.size()[0]): # 0th dimension represents num events
        output, hidden = rnn(person_tensor[i], hidden) # line_tensor[i] should be 2D since being computed with hidden state
        
    loss = criterion(output, category_tensor)
    
    optimizer.zero_grad()
    loss.backward()
    optimizer.step()
    
    return output, loss.item()

def get_dictionary_of_person_IDs_and_reference_events(df):
    df_of_opioid_exposure_events = df[df["is_exposed_to_Opioids"] == 1]
    df_of_first_opioid_exposure_events = df_of_opioid_exposure_events.drop_duplicates(subset = ["person_id"])
    list_of_person_IDs = list(df_of_first_opioid_exposure_events["person_id"])
    list_of_reference_events = list(df_of_first_opioid_exposure_events["visit_start_datetime"])

    dictionary_of_person_IDs_and_reference_events = dict(zip(list_of_person_IDs, list_of_reference_events))
    return dictionary_of_person_IDs_and_reference_events

def get_dictionary_of_labels_and_person_IDs(df, dict_of_person_IDs_and_reference_events):
    unique_person_IDs = list(pd.unique(df["person_id"]))
    dictionary_of_labels_and_person_IDs = {0: [], 1: []}
    for person_ID in unique_person_IDs:
        df_corresponding_to_person = df[df["person_id"] == person_ID]
        reference_event_datetime = dict_of_person_IDs_and_reference_events[person_ID]
        df_corresponding_to_person_after_reference_event = df_corresponding_to_person[df_corresponding_to_person["visit_start_datetime"] > reference_event_datetime]
        if (df_corresponding_to_person_after_reference_event["has_Opioid_abuse"].max() == 1):
            dictionary_of_labels_and_person_IDs[1].append(person_ID)
        else:
            dictionary_of_labels_and_person_IDs[0].append(person_ID)
    return dictionary_of_labels_and_person_IDs


def random_training_example(all_categories, dict_of_labels_and_person_IDs, dict_of_person_IDs_and_reference_events, df):
    def random_choice(a):
        random_idx = random.randint(0, len(a) - 1)
        return a[random_idx]
    
    random_category = random_choice(all_categories) # Gets category ground truth / label
    random_person = random_choice(dict_of_labels_and_person_IDs[random_category]) # Gets name / input

    category_tensor = torch.tensor([all_categories.index(category)], dtype=torch.long) # Category gets placed into a tensor

    data_frame_corresponding_to_person = df[df["person_id"] == random_person]
    reference_event_datetime = dict_of_person_IDs_and_reference_events[random_person]
    data_frame_corresponding_to_person_before_reference_event = data_frame_corresponding_to_person[data_frame_corresponding_to_person["visit_start_datetime"] < reference_event_datetime]
    data_frame_corresponding_to_person_before_reference_event = data_frame_corresponding_to_person_before_reference_event.drop(columns = ["person_id", "visit_start_datetime", "has_Opioid_abuse", "is_exposed_to_Opioids"])

    #### CONVERT THIS DATA FRAME INTO TENSOR NOW
    person_tensor = torch.tensor(data_frame_corresponding_to_person_before_reference_event.values, dtype = torch.float32)
    number_of_visits_for_patient = data_frame_corresponding_to_person_before_reference_event.shape[0]
    number_of_features = data_frame_corresponding_to_person_before_reference_event.shape[1]
    person_tensor = person_tensor.resize_(number_of_visits_for_patient, 1, number_of_features)

    return random_category, category_tensor, person_tensor

def train_RNN():
    current_loss = 0
    all_losses = []
    plot_steps, print_steps = 1000, 5000
    n_iters = 100000

    for i in range(n_iters):
        category, category_tensor, person_tensor = random_training_example(all_categories, dictionary_of_labels_and_person_IDs, dictionary_of_person_IDs_and_reference_events, feature_matrix) # Ground truth, pts raw history not necessary, ground truth index, input as tensor
        if person_tensor.size()[0] == 0:
            continue
        output, loss = train(person_tensor, category_tensor) # PASS patients 3D tensor
        current_loss += loss 
        
        if (i+1) % plot_steps == 0:
            all_losses.append(current_loss / plot_steps)
            current_loss = 0
            
        if (i+1) % print_steps == 0:
            guess = category_from_output(output)
            correct = "CORRECT" if guess == category else f"WRONG ({category})"
            print(f"{i+1} {(i+1)/n_iters*100} {loss:.4f} / {guess} {correct}") # REMOVE LINE AS IT REPRESENTS RAW PT HISTORY
            
        
    plt.figure()
    plt.plot(all_losses)
    plt.show()


all_categories = [0, 1]
n_categories = len(all_categories)                  # CAN MAKE 2 OR 1, 2 since that seems to be standard, SUSPICIOUS ABOUT HOW THIS CHECKNG WORKS
n_hidden = 128
n_features = 8
rnn = RNN(n_features, n_hidden, n_categories)        # NUMBER OF FEATURES is N_LETTERS, in our case 8 with the aggregated
criterion = nn.NLLLoss()
learning_rate = 0.005
optimizer = torch.optim.SGD(rnn.parameters(), lr=learning_rate)

feature_matrix = pd.read_csv("~/workspaces/opioidabusepredictor/opioidabusepredictor/Feature_Matrix.csv", index_col = 0)
feature_matrix = feature_matrix.fillna(0)
feature_matrix = feature_matrix.sort_values(by = ["person_id", "visit_start_datetime"])
feature_matrix = feature_matrix.reset_index(drop = True)
dictionary_of_person_IDs_and_reference_events = get_dictionary_of_person_IDs_and_reference_events(feature_matrix)
dictionary_of_labels_and_person_IDs = get_dictionary_of_labels_and_person_IDs(feature_matrix, dictionary_of_person_IDs_and_reference_events)
