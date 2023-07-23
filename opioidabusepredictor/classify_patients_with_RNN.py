"""
Classifying Patients With A Visit-Level RNN

Created: 07/23/2023 by Tom Lever
Updated: 07/23/2023 by Tom Lever

See https://pytorch.org/tutorials/intermediate/char_rnn_classification_tutorial.html .

We train a basic visit-level Recurrent Neural Network (RNN) to classify patients.

A visit-level RNN interprets patients as a series of visits -
outputting a prediction and "hidden state" at each step,
feeding its previous hidden state into each next step.
We take the final prediction to be the output,
i.e. which class the patient belongs to.

Specifically, we'll train on a few thousand patients from 2 classes;
namely, will abuse opioids and will not abuse opioids.
We'll predict which class a patient is from based on the patient's visits.

Preparing The Data



Included in the `data/names` directory are 18 text files named as `[Language].txt`.
Each file contains a bunch of names, one name per line, mostly romanized
(but we still need to convert from Unicode to ASCII).

We'll end up with a dictionary of lists of names per language, `{language: [names ...]}`.
"""

# Generate list of paths of file of names
import glob
path_for_each_file_of_names = 'opioidabusepredictor/data/names/*.txt'
list_of_paths_of_file_of_names = glob.glob(path_for_each_file_of_names)
list_of_paths_of_file_of_names = [path_of_file_of_names.replace('\\', '/') for path_of_file_of_names in list_of_paths_of_file_of_names]
#print(list_of_paths_of_file_of_names)
#print()

# Define a string of all ASCII letters and the number of ASCII letters.
import string
string_of_all_ASCII_letters = string.ascii_letters + " .,;'"
number_of_ASCII_letters = len(string_of_all_ASCII_letters)

# Turn a Unicode string to plain ASCII.
# See https://stackoverflow.com/a/518232/2809427
import unicodedata
def convert_from_unicode_to_ASCII(the_string):
    normalized_string = unicodedata.normalize('NFD', the_string)
    return ''.join(character for character in normalized_string if unicodedata.category(character) != 'Mn' and character in string_of_all_ASCII_letters)
#print(convert_from_unicode_to_ASCII('Ślusàrski'))
#print()

# Generate list of lines of file
def generate_list_of_lines_of_file(file_name):
    the_TextIOWrapper = open(file_name, encoding = 'utf-8')
    contents_of_file = the_TextIOWrapper.read()
    stripped_contents_of_file = contents_of_file.strip()
    list_of_lines_of_file = stripped_contents_of_file.split('\n')
    list_of_lines_of_file_where_lines_are_converted_to_ASCII = [convert_from_unicode_to_ASCII(line) for line in list_of_lines_of_file]
    return(list_of_lines_of_file_where_lines_are_converted_to_ASCII)
#print(generate_list_of_lines_of_file('opioidabusepredictor/data/names/English.txt'))
#print()

# Generate dictionary of languages and lists of names
dictionary_of_languages_and_lists_of_names = {}
import os
for path_of_file_of_names in list_of_paths_of_file_of_names:
    base_name = os.path.basename(path_of_file_of_names)
    language = os.path.splitext(base_name)[0]
    list_of_names = generate_list_of_lines_of_file(path_of_file_of_names)
    dictionary_of_languages_and_lists_of_names[language] = list_of_names
#print(dictionary_of_languages_and_lists_of_names)
#print()

# Turning Names Into Tensors
#
# Now we have `dictionary_of_languages_and_lists_of_names`,
# a dictionary mapping each language to a list of names.
#
# Now that we have all the names organized,
# we need to turn them into Tensors to make any use of them.
#
# To represent a single letter, we use a "one-hot vector" of size <1 x n_letters>.
# A one-hot vector is filled with 0's except for a 1 at index of the current letter,
# e.g. "b" = <0 1 0 0 0 ...>.
#
# To make a word we join a bunch of those into a 2D matrix <line_length x 1 x n_letters>.
#
# That extra 1 dimension is because PyTorch assumes everything is in batches -
# we're just using a batch size of 1 here.

def get_index_of_character_in_string_of_all_ASCII_characters(character):
    index_of_character_in_string_of_all_ASCII_characters = string_of_all_ASCII_letters.find(character)
    return(index_of_character_in_string_of_all_ASCII_characters)
#print(get_index_of_character_in_string_of_all_ASCII_characters('c'))
#print()

import torch
def convert_character_to_tensor(character):
    tensor = torch.zeros(1, number_of_ASCII_letters)
    index_of_character_in_string_of_all_ASCII_characters = get_index_of_character_in_string_of_all_ASCII_characters(character)
    tensor[0][index_of_character_in_string_of_all_ASCII_characters] = 1
    return tensor
tensor_representing_character_in_name = convert_character_to_tensor("c")
#print(tensor_representing_character_in_name.size())
#print(tensor_representing_character_in_name)

def convert_name_to_tensor(name):
    number_of_characters_in_name = len(name)
    tensor = torch.zeros(number_of_characters_in_name, 1, number_of_ASCII_letters)
    for index_of_character_in_name, character in enumerate(name):
        index_of_character_in_string_of_all_ASCII_characters = get_index_of_character_in_string_of_all_ASCII_characters(character)
        tensor[index_of_character_in_name][0][index_of_character_in_string_of_all_ASCII_characters] = 1
    return tensor
tensor_representing_name = convert_name_to_tensor('Jones')
#print(tensor_representing_name.size())
#print(tensor_representing_name)

# Creating The Network
#
# torch.autograd is PyTorch's automatic differentiation engine
# that powers neural network training.
#
# Before autograd, creating a recurrent neural network in Torch involved
# cloning the parameters of a layer over several timesteps.
# The layers held hidden state and gradients
# which are now entirely handled by the graph itself.
# This means you can implement a RNN in a very "pure" way, as regular feed-forward layers.
#
# This RNN module
# (mostly copied from the PyTorch for Torch users tutorial at
# https://pytorch.org/tutorials/beginner/former_torchies/nnft_tutorial.html)
# is just 2 linear layers which operate an an input and hidden state,
# with a LogSoftmax layer after the output.

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
    
number_of_elements_in_hidden_state = 128
list_of_all_languages = list(dictionary_of_languages_and_lists_of_names.keys())
number_of_languages = len(list_of_all_languages)
rnn = RNN(number_of_ASCII_letters, number_of_elements_in_hidden_state, number_of_languages)

# To run a step of this network we need to pass an input
# (in our case, the Tensor for the current letter)
# and a previous hidden state (which we initialize as zeros at first).
# We'll get back to the output (probability of each language)
# and a next hidden state (which we keep for the next step).

tensor_representing_character_in_name = convert_character_to_tensor('A')
hidden_state = torch.zeros(1, number_of_elements_in_hidden_state)
tensor_of_likelihoods_that_name_corresponds_to_language, next_hidden_state = rnn(tensor_representing_character_in_name, hidden_state)
#print(tensor_of_likelihoods_that_name_corresponds_to_language)
#print(next_hidden_state)

# For the sake of efficiency we don't want to be creating a new Tensor for every step,
# so we will use `convert_name_to_tensor` instead of `convert_character_to_tensor`
# and use slices.
# This could be further optimized by precomputing batches of Tensors.

tensor_representing_name = convert_name_to_tensor('Albert')
hidden_state = torch.zeros(1, number_of_elements_in_hidden_state)
tensor_representing_first_character_in_name = tensor_representing_name[0]
tensor_of_likelihoods_that_name_corresponds_to_language, next_hidden_state = rnn(tensor_representing_first_character_in_name, hidden_state)
#print(tensor_of_likelihoods_that_name_corresponds_to_language)

# Training
#
# Preparing For Training
#
# Before going into training we should make a few helper functions.
# The first is to interpret the output of the network,
# which we known to be a likelihood of each category.
# We can use `Tensor.topk` to get the index of the greatest value.

def get_tuple_of_most_likely_language_and_its_index_in_list_of_all_languages(tensor_of_likelihoods_that_name_corresponds_to_language):
    the_topk = tensor_of_likelihoods_that_name_corresponds_to_language.topk(k = 1)
    tensor_of_indices_of_most_likely_languages = the_topk[1]
    tensor_of_index_of_most_likely_language = tensor_of_indices_of_most_likely_languages[0]
    index_of_most_likely_language = tensor_of_index_of_most_likely_language.item()
    most_likely_language = list_of_all_languages[index_of_most_likely_language]
    tuple_of_most_likely_language_and_its_index_in_list_of_all_languages = (most_likely_language, index_of_most_likely_language)
    return tuple_of_most_likely_language_and_its_index_in_list_of_all_languages
#print(get_tuple_of_most_likely_language_and_its_index_in_list_of_all_languages(tensor_of_likelihoods_that_name_corresponds_to_language))

# We will also want a quick way to get a training example (a name and its language).

import random
def get_random_element_of_list(the_list):
    length_of_list = len(the_list)
    random_index = random.randint(0, length_of_list - 1)
    return the_list[random_index]

def get_tuple_of_random_language_random_name_tensor_of_index_of_random_language_and_tensor_of_index_of_random_name():
    random_language = get_random_element_of_list(list_of_all_languages)
    list_of_names = dictionary_of_languages_and_lists_of_names[random_language]
    random_name = get_random_element_of_list(list_of_names)
    index_of_random_language = list_of_all_languages.index(random_language)
    list_of_index_of_random_language = [index_of_random_language]
    tensor_of_index_of_random_language = torch.tensor(list_of_index_of_random_language, dtype = torch.long)
    tensor_of_random_name = convert_name_to_tensor(random_name)
    tuple_of_random_language_random_name_tensor_of_index_of_random_language_and_tensor_of_index_of_random_name = (random_language, random_name, tensor_of_index_of_random_language, tensor_of_random_name)
    return tuple_of_random_language_random_name_tensor_of_index_of_random_language_and_tensor_of_index_of_random_name
for i in range(0, 10):
    random_language, random_name, tensor_of_index_of_random_language, tensor_of_random_name = get_tuple_of_random_language_random_name_tensor_of_index_of_random_language_and_tensor_of_index_of_random_name()
    #print('random language = ', random_language, '/ random name = ', random_name)

# Training The Network
#
# Now all it takes to train this network is
# to show it a bunch of examples, have it make guesses, and tell it if it's wrong.
#
# For the loss function `torch.nn.NLLLoss` is appropriate,
# since the last layer of the RNN is `torch.nn.LogSoftmax`.

criterion = torch.nn.NLLLoss()
learning_rate = 0.005

def train(tensor_of_index_of_random_language, tensor_of_random_name):
    hidden_state = rnn.initialize_hidden_state()
    rnn.zero_grad()
    for i in range(tensor_of_random_name.size()[0]):
        tensor_of_likelihoods_that_name_corresponds_to_language, hidden_state = rnn(tensor_of_random_name[i], hidden_state)
    loss = criterion(tensor_of_likelihoods_that_name_corresponds_to_language, tensor_of_index_of_random_language)
    loss.backward()
    for p in rnn.parameters():
        p.data.add_(p.grad.data, alpha = -learning_rate)
    return tensor_of_likelihoods_that_name_corresponds_to_language, loss.item()

# Now we just have to run that with a bunch of examples.
# Since the `train` function returns both the output and loss we can print its guesses and
# also keep track of loss for plotting.
# Since there are 1000's of example we print only every `print_every` examples, and
# take an average of the loss.

sum_of_losses = 0
list_of_losses = []
number_of_iterations = 100000
number_of_iterations_after_which_to_plot = 1000
number_of_iterations_after_which_to_print = 5000

import math
import time

# Keep track of losses for plotting
sum_of_losses = 0
list_of_average_losses = []

def calculate_time_interval_between_now_and_start_time(start_time):
    now = time.time()
    time_interval_in_seconds = now - start_time
    time_interval_in_whole_minutes = math.floor(time_interval_in_seconds / 60)
    time_interval_during_this_minute = time_interval_in_seconds - time_interval_in_whole_minutes * 60
    return '%dtime_interval_in_whole_minutes %dtime_interval_during_this_minute' % (time_interval_in_whole_minutes, time_interval_during_this_minute)

import matplotlib.pyplot as plt
import matplotlib.ticker as ticker
should_train = False
if (should_train):
    start_time = time.time()
    for iteration in range(1, number_of_iterations + 1):
        random_language, random_name, tensor_of_index_of_random_language, tensor_of_random_name = get_tuple_of_random_language_random_name_tensor_of_index_of_random_language_and_tensor_of_index_of_random_name()
        tuple_of_most_likely_language_and_its_index_in_list_of_all_languages, loss = train(tensor_of_index_of_random_language, tensor_of_random_name)
        sum_of_losses += loss
        if iteration % number_of_iterations_after_which_to_print == 0:
            predicted_language, index_of_predicted_language = get_tuple_of_most_likely_language_and_its_index_in_list_of_all_languages(tuple_of_most_likely_language_and_its_index_in_list_of_all_languages)
            indicator_of_whether_prediction_is_correct = '✓' if predicted_language == random_language else '✗ (%s)' % random_language
            print('%d %d%% (%s) %.4f %s / %s %s' % (iteration, iteration / number_of_iterations * 100, calculate_time_interval_between_now_and_start_time(start_time), loss, random_name, predicted_language, indicator_of_whether_prediction_is_correct))
        if iteration % number_of_iterations_after_which_to_plot == 0:
            list_of_average_losses.append(sum_of_losses / number_of_iterations_after_which_to_plot)
            sum_of_losses = 0
    torch.save(rnn.state_dict(), 'Opioid_Abuse_Predictor.pt')
    plt.figure()
    plt.plot(list_of_average_losses)
    plt.show()
else:
    rnn.load_state_dict(torch.load('Opioid_Abuse_Predictor.pt'))

# Evaluating The Results
#
# To see how well the network performs on different languages,
# we will create a confusion matrix, indicating for every actual language (rows)
# which language the network guesses (columns). To calculate the confusion matrix
# a bunch of samples are run through the network with `evaluate()`,
# which is the same as `train()` minus the backprop.

# Keep track of correct guesses in a confusion matrix
confusion_matrix = torch.zeros(number_of_languages, number_of_languages)
number_of_iterations = 10000

# Just return an output given a line
def evaluate(tensor_of_random_name):
    hidden_state = rnn.initialize_hidden_state()
    for i in range(tensor_of_random_name.size()[0]):
        tensor_of_likelihoods_that_name_corresponds_to_language, hidden_state = rnn(tensor_of_random_name[i], hidden_state)
    return tensor_of_likelihoods_that_name_corresponds_to_language

# Go through a bunch of examples and record which are correctly guessed
for i in range(number_of_iterations):
    random_language, random_name, tensor_of_index_of_random_language, tensor_of_random_name = get_tuple_of_random_language_random_name_tensor_of_index_of_random_language_and_tensor_of_index_of_random_name()
    tensor_of_likelihoods_that_name_corresponds_to_language = evaluate(tensor_of_random_name)
    predicted_language, index_of_predicted_language = get_tuple_of_most_likely_language_and_its_index_in_list_of_all_languages(tensor_of_likelihoods_that_name_corresponds_to_language)
    index_of_random_language = list_of_all_languages.index(random_language)
    confusion_matrix[index_of_random_language][index_of_predicted_language] += 1

# Normalize by dividing every row by its sum
for i in range(number_of_languages):
    confusion_matrix[i] = confusion_matrix[i] / confusion_matrix[i].sum()

# Set up plot
fig = plt.figure()
ax = fig.add_subplot(111)
cax = ax.matshow(confusion_matrix.numpy())
fig.colorbar(cax)

# Set up axes
ax.set_xticklabels([''] + list_of_all_languages, rotation=90)
ax.set_yticklabels([''] + list_of_all_languages)

# Force label at every tick
ax.xaxis.set_major_locator(ticker.MultipleLocator(1))
ax.yaxis.set_major_locator(ticker.MultipleLocator(1))

# sphinx_gallery_thumbnail_number = 2
plt.show()

def predict(input_line, n_predictions=3):
    print('\n> %s' % input_line)
    with torch.no_grad():
        output = evaluate(convert_name_to_tensor(input_line))

        # Get top N categories
        topv, topi = output.topk(n_predictions, 1, True)
        predictions = []

        for i in range(n_predictions):
            value = topv[0][i].item()
            category_index = topi[0][i].item()
            print('(%.2f) %s' % (value, list_of_all_languages[category_index]))
            predictions.append([value, list_of_all_languages[category_index]])

predict('Dovesky')
predict('Jackson')
predict('Satoshi')
