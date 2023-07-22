# NLP From Scratch: Classifying Names With A Character-Level RNN
#
# Created: 07/22/2023 by Tom Lever
# Updated: 07/22/2023 by Tom Lever
#
# See https://pytorch.org/tutorials/intermediate/char_rnn_classification_tutorial.html .
#
# We train a basic character-level Recurrent Neural Network (RNN) to classify words.
# This tutorial informs training a basic visit-level RNN to classify patients.
#
# A character-level RNN reads words as a series of characters -
# outputting a prediction and "hidden state" at each step,
# feeding its previous hidden state into each next step.
# We take the final prediction to be the output, i.e. which class the word belongs to.
#
# Specifically, we'll train on a few thousand surnames from 18 languages of origin,
# and predict which language a name is from based on the spelling.
#
# Download data from https://download.pytorch.org/tutorial/data.zip and
# extract it to `Opioid_Abuse_Predictor/opioidabusepredictor`.
#
# Included in the `data/names` directory are 18 text files named as `[Language].txt`.
# Each file contains a bunch of names, one name per line, mostly romanized
# (but we still need to convert from Unicode to ASCII).
#
# We'll end up with a dictionary of lists of names per language, `{language: [names ...]}`.

# Generate list of paths of file of names
import glob
path_for_each_file_of_names = 'opioidabusepredictor/data/names/*.txt'
list_of_paths_of_file_of_names = glob.glob(path_for_each_file_of_names)
list_of_paths_of_file_of_names = [path_of_file_of_names.replace('\\', '/') for path_of_file_of_names in list_of_paths_of_file_of_names]
#print(list_of_paths_of_file_of_names)
#print()

# Turn a Unicode string to plain ASCII.
# See https://stackoverflow.com/a/518232/2809427
import string
import unicodedata
def convert_from_unicode_to_ASCII(the_string):
    normalized_string = unicodedata.normalize('NFD', the_string)
    string_of_ASCII_letters = string.ascii_letters + " .,;'"
    return ''.join(character for character in normalized_string if unicodedata.category(character) != 'Mn' and character in string_of_ASCII_letters)
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

