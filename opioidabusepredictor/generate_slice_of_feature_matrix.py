import os
import pandas as pd

def get_data_frame(query):
    data_frame = pd.read_gbq(
        query = query,
        dialect = "standard",
        use_bqstorage_api = ("BIGQUERY_STORAGE_API_ENABLED" in os.environ),
        progress_bar_type = "tqdm_notebook"
    )
    return data_frame

# 1
query_that_results_in_table_of_IDs_of_criteria_relating_to_cancer = """
SELECT CAST(cr.id as string) as id
FROM `""" + os.environ["WORKSPACE_CDR"] + """.cb_criteria` cr
WHERE
    concept_id IN (
        40493428, 252840, 4092524, 4178964, 76349, 4177243, 4313083, 4241905, 46273652, 196925,
        4180791, 436045, 200962, 4091466, 4162860, 4294416, 4092212, 444411, 4154630, 442139,
        4309225, 4298032, 40650479, 197807, 4091465, 40490994, 4216273, 133424, 4247726, 4247822,
        4181483, 4162994, 4309851, 436671, 4180780, 4300555, 4160780, 4180910, 4247238, 4155297,
        4178971, 443568, 75210, 442183, 40492021, 78987, 443388, 195483, 4155285, 4181488,
        25189, 4180902, 36676291, 376918, 4297666, 4157332, 40488919, 380661, 256633, 133711,
        200348, 4174593, 433143, 4178977, 4178959, 443387, 40480128, 432263, 4312698, 4315805,
        4112853, 4297185, 4181333, 442181, 46270738, 4178974, 4246450, 442131, 443391, 444410,
        137809, 4181330, 4313916, 40487047, 192581, 4314047, 4092217, 4181351, 37397344, 4112745,
        257503, 438693, 139750, 4181338, 198091, 436357, 196359, 432257, 4246451, 258375,
        200052, 40488964, 256646, 436353, 438094, 4312023, 31509, 201238, 37310458, 4095312,
        138996, 4162253, 378087, 435484, 374874, 36684472, 4314071, 4312288, 261514, 4181332,
        44806773, 196360, 4188544, 4298026, 443392, 4295624, 432845, 45769720, 197507, 4233629,
        4241917, 199754, 4160276, 440649, 443588, 4089655, 4092511, 439751, 432837, 4095740,
        197808, 4162251, 200051, 4310566, 4187850, 26052, 4187849, 4311637, 4246017, 440658,
        4247719, 4312685, 4241904, 4181480, 4311342, 4116084, 4111921, 36683531, 441805, 4298028,
        437498, 4054503, 4298033, 192568, 4246127, 4312022, 4081044, 197500, 373425, 4338758,
        443398, 376647, 4308621, 198988, 40491001, 40492037, 40490993, 4180793, 4181484, 4247338,
        4297665, 441515, 74582, 442132, 261236, 4188545, 4180790, 4247331, 133726, 433716,
        258369, 136917, 440345, 200959, 320342, 434298, 4334322, 4181350, 40492932, 4178968,
        198700, 380055, 440956, 4111776, 4311619, 4247836, 4155171, 432833, 4111805, 433435,
        201519, 438386, 4178976, 192855, 4157457, 75512, 4311617, 4157456, 24897, 45770892,
        4187848, 80045, 4116241, 45768522, 4311499, 72266, 255507, 193144, 30346, 4315806,
        443561, 28083, 4001666, 4089665, 4089756, 439404, 435755, 42536893, 4312944, 443390,
        40493020, 4187851, 441233, 4248061, 4247357, 4157454, 4313482, 438699, 318096, 4112974,
        432254, 4158563, 4091464, 436635, 198985, 40482750, 254591, 4181343, 197804, 4151250,
        40487143, 443386, 46270513, 4157449, 4181354, 4181339, 133147, 443389, 4092382, 45757101,
        4313634, 4307263, 4092512, 194589, 253717, 136354, 764225, 195513, 4153882, 4157331,
        4214901, 4181477
    )
    AND full_text LIKE '%_rank1]%'
"""

# 2
query_that_results_in_table_of_distinct_concept_IDs_from_table_cb_criteria_and_table_of_IDs_of_criteria_relating_to_cancer = """
SELECT distinct concept_id
FROM `""" + os.environ["WORKSPACE_CDR"] + """.cb_criteria` c
JOIN (""" + query_that_results_in_table_of_IDs_of_criteria_relating_to_cancer + """) a
ON (
       c.path LIKE CONCAT('%.', a.id, '.%')
       OR c.path LIKE CONCAT('%.', a.id)
       OR c.path LIKE CONCAT(a.id, '.%')
       OR c.path = a.id
   )
WHERE
    is_standard = 1
    AND is_selectable = 1
"""

# 3
query_that_results_in_table_of_distinct_combinations_of_person_ID_entry_date_and_concept_ID_from_table_of_events_where_concept_IDs_correspond_to_cancer = """
SELECT DISTINCT person_id, entry_date, concept_id
FROM `""" + os.environ["WORKSPACE_CDR"] + """.cb_search_all_events`
WHERE
    concept_id IN (""" + query_that_results_in_table_of_distinct_concept_IDs_from_table_cb_criteria_and_table_of_IDs_of_criteria_relating_to_cancer + """)
    AND is_standard = 1
"""

# 4
query_that_results_in_table_of_person_IDs_from_table_of_distinct_combinations_of_person_ID_entry_date_and_concept_ID_from_table_of_events_where_concept_IDs_correspond_to_cancer = """
SELECT person_id
FROM (""" + query_that_results_in_table_of_distinct_combinations_of_person_ID_entry_date_and_concept_ID_from_table_of_events_where_concept_IDs_correspond_to_cancer + """)
"""

# 5
query_that_results_in_table_of_ID_of_concept_Opioids = """
SELECT CAST(cr.id as string) as id
FROM `""" + os.environ["WORKSPACE_CDR"] + """.cb_criteria` cr
WHERE
    concept_id IN (21604254)
    AND full_text LIKE '%_rank1]%'
"""

# 6
query_that_results_in_table_of_distinct_concept_IDs_of_opioids = """
SELECT DISTINCT c.concept_id
FROM `""" + os.environ["WORKSPACE_CDR"] + """.cb_criteria` c
JOIN (""" + query_that_results_in_table_of_ID_of_concept_Opioids + """) a
ON (
    c.path LIKE CONCAT('%.', a.id, '.%')
    OR c.path LIKE CONCAT('%.', a.id)
    OR c.path LIKE CONCAT(a.id, '.%')
    OR c.path = a.id
)
WHERE
    is_standard = 1
    AND is_selectable = 1
"""

# 6.25
query_that_results_in_table_of_criteria_ancestor = """
SELECT *
FROM `""" + os.environ["WORKSPACE_CDR"] + """.cb_criteria_ancestor` ca
"""

# 6.50
query_that_results_in_table_of_criteria_ancestor_and_codes = """
SELECT *
FROM `""" + os.environ["WORKSPACE_CDR"] + """.cb_criteria_ancestor` ca
LEFT JOIN (""" + query_that_results_in_table_of_concept_IDs_and_codes + """) ba
ON ca.ancestor_id = b.concept_id
"""

# 7
query_that_results_in_table_of_distinct_descendant_IDs_of_opioids = """
SELECT DISTINCT ca.descendant_id
FROM `""" + os.environ["WORKSPACE_CDR"] + """.cb_criteria_ancestor` ca
JOIN (""" + query_that_results_in_table_of_distinct_concept_IDs_of_opioids + """) b
ON ca.ancestor_id = b.concept_id
"""

# 8
query_that_results_in_table_of_distinct_combinations_of_person_ID_entry_date_and_concept_ID_from_table_of_events_where_concept_IDs_correspond_to_opioids = """
SELECT DISTINCT person_id, entry_date, concept_id
FROM `""" + os.environ["WORKSPACE_CDR"] + """.cb_search_all_events`
WHERE
    concept_id IN (""" + query_that_results_in_table_of_distinct_descendant_IDs_of_opioids + """)
    AND is_standard = 1
"""

# 9
query_that_results_in_table_of_person_IDs_from_table_of_distinct_combinations_of_person_IDs_entry_date_and_concept_ID_from_table_of_events_where_concept_IDs_correspond_to_opioids = """
SELECT person_id
FROM (""" + query_that_results_in_table_of_distinct_combinations_of_person_ID_entry_date_and_concept_ID_from_table_of_events_where_concept_IDs_correspond_to_opioids + """)
"""

# 10
query_that_results_in_table_of_distinct_person_IDs_of_cohort = """
SELECT DISTINCT person_id
FROM `""" + os.environ["WORKSPACE_CDR"] + """.cb_search_person` cb_search_person
WHERE
    person_id IN (""" + query_that_results_in_table_of_person_IDs_from_table_of_distinct_combinations_of_person_IDs_entry_date_and_concept_ID_from_table_of_events_where_concept_IDs_correspond_to_opioids + """)
    AND person_id NOT IN (""" + query_that_results_in_table_of_person_IDs_from_table_of_distinct_combinations_of_person_ID_entry_date_and_concept_ID_from_table_of_events_where_concept_IDs_correspond_to_cancer + """)
"""

# 11
query_that_results_in_table_of_condition_occurrences_relating_to_cohort = """
SELECT *
FROM `""" + os.environ["WORKSPACE_CDR"] + """.condition_occurrence` c_occurrence
WHERE PERSON_ID IN (""" + query_that_results_in_table_of_distinct_person_IDs_of_cohort + """)
"""

# 12
query_that_results_in_table_of_concept_IDs_and_codes = """
SELECT
    concept_id,
    code
FROM `""" + os.environ["WORKSPACE_CDR"] + """.cb_criteria` cr
WHERE full_text LIKE '%_rank1]%'
"""

# 13
query_that_results_in_table_of_person_IDs_codes_standard_concept_names_and_visit_occurrence_IDs_from_table_of_condition_occurrences_relating_to_cohort_table_concept_and_table_of_concept_IDs_and_codes = """
SELECT
    person_id,
    code,
    concept_name,
    visit_occurrence_id
FROM (""" + query_that_results_in_table_of_condition_occurrences_relating_to_cohort + """) c_occurrence
LEFT JOIN `""" + os.environ["WORKSPACE_CDR"] + """.concept` c_standard_concept
ON c_occurrence.condition_concept_id = c_standard_concept.concept_id
LEFT JOIN (""" + query_that_results_in_table_of_concept_IDs_and_codes + """) table_of_concept_IDs_and_codes
ON c_occurrence.condition_concept_id = table_of_concept_IDs_and_codes.concept_id
"""
query_13 = query_that_results_in_table_of_person_IDs_codes_standard_concept_names_and_visit_occurrence_IDs_from_table_of_condition_occurrences_relating_to_cohort_table_concept_and_table_of_concept_IDs_and_codes

# 14
query_that_results_in_table_of_distinct_person_IDs_of_opioid_abusers_in_cohort = """
SELECT DISTINCT person_id
FROM (""" + query_that_results_in_table_of_person_IDs_codes_standard_concept_names_and_visit_occurrence_IDs_from_table_of_condition_occurrences_relating_to_cohort_table_concept_and_table_of_concept_IDs_and_codes + """)
WHERE concept_name = "Opioid abuse"
"""

# 15
query_that_results_in_table_of_3790_distinct_random_person_IDs_of_non_opioid_abusers_in_cohort = """
SELECT DISTINCT person_id
FROM (""" + query_that_results_in_table_of_distinct_person_IDs_of_cohort + """)
WHERE person_id NOT IN (""" + query_that_results_in_table_of_distinct_person_IDs_of_opioid_abusers_in_cohort + """)
ORDER BY RAND()
LIMIT 3790
"""

# 16
query_that_results_in_table_of_distinct_person_IDs_of_undersample = (
"""(""" + query_that_results_in_table_of_3790_distinct_random_person_IDs_of_non_opioid_abusers_in_cohort + """)
UNION ALL
(""" + query_that_results_in_table_of_distinct_person_IDs_of_opioid_abusers_in_cohort + """)"""
)

# 17
query_that_results_in_table_of_distint_person_IDs_visit_occurrence_IDs_and_visit_start_dates_for_undersample = """
SELECT
    visit_occurrence.person_id,
    visit_occurrence_id,
    visit_start_date
FROM `""" + os.environ["WORKSPACE_CDR"] + """.visit_occurrence` visit_occurrence
INNER JOIN (""" + query_that_results_in_table_of_distinct_person_IDs_of_undersample + """) table_of_IDs_of_undersample
ON visit_occurrence.person_id = table_of_IDs_of_undersample.person_id
"""

# 18
query_that_results_in_table_of_person_IDs_codes_standard_concept_names_and_visit_occurrence_IDs_as_in_query_13_but_for_undersample = """
SELECT
    table_of_distinct_person_IDs_of_undersample.person_id,
    code,
    concept_name,
    visit_occurrence_id
FROM (""" + query_13 + """) table_from_query_13
JOIN (""" + query_that_results_in_table_of_distinct_person_IDs_of_undersample + """) table_of_distinct_person_IDs_of_undersample
ON table_from_query_13.person_id = table_of_distinct_person_IDs_of_undersample.person_id
"""

dictionary_of_codes_of_condition_and_names_of_column = {
    "48694002": "has_Anxiety",
    "13746004": "has_Bipolar_disorder",
    "35489007": "has_Depressive_disorder",
    "38341003": "has_Hypertensive_disorder",
    "5602001": "has_Opioid_abuse",
    "75544000": "has_Opioid_dependence",
    "22253000": "has_Pain",
    "70076002": "has_Rhinitis",
    "66214007": "has_Non_Opioid_Substance_abuse"
}

def generate_query_that_results_in_table_of_codes_of_feature_that_is_child_of_provided_feature(code_of_provided_feature):

    # 19
    query_that_results_in_table_with_ID_of_provided_feature = """
SELECT CAST(cr.id as string) as id
FROM `""" + os.environ["WORKSPACE_CDR"] + """.cb_criteria` cr
WHERE
    code = '""" + code_of_provided_feature + """'
    AND full_text LIKE '%_rank1]%'
    """

    # 20
    query_that_results_in_table_of_concept_IDs_of_feature_that_is_child_of_provided_feature = """
SELECT DISTINCT c.concept_id
FROM `""" + os.environ["WORKSPACE_CDR"] + """.cb_criteria` c
JOIN (""" + query_that_results_in_table_with_ID_of_provided_feature + """) a
ON (
    c.path LIKE CONCAT('%.', a.id, '.%')
    OR c.path LIKE CONCAT('%.', a.id)
    OR c.path LIKE CONCAT(a.id, '.%')
    OR c.path = a.id
)
WHERE
    is_standard = 1
    AND is_selectable = 1
    """

    # 21
    query_that_results_in_table_of_codes_of_feature_that_is_child_of_provided_feature = """
SELECT code
FROM (""" + query_that_results_in_table_of_concept_IDs_of_feature_that_is_child_of_provided_feature + """) table_of_concept_IDs
JOIN (""" + query_that_results_in_table_of_concept_IDs_and_codes + """) table_of_concept_IDs_and_codes
ON table_of_concept_IDs.concept_id = table_of_concept_IDs_and_codes.concept_id
    """

    return query_that_results_in_table_of_codes_of_feature_that_is_child_of_provided_feature

# 22
query_that_results_in_ungrouped_conditions_feature_matrix = """
SELECT
    person_id,
    visit_occurrence_id,
"""
for code_of_condition, name_of_column in dictionary_of_codes_of_condition_and_names_of_column.items():
    case_block = """
CASE WHEN code IN (""" + generate_query_that_results_in_table_of_codes_of_feature_that_is_child_of_provided_feature(code_of_condition) + """)
THEN 1
ELSE 0 END AS """ + name_of_column + """,
    """
    query_that_results_in_ungrouped_conditions_feature_matrix += case_block
query_that_results_in_ungrouped_conditions_feature_matrix += """
FROM (""" + query_that_results_in_table_of_person_IDs_codes_standard_concept_names_and_visit_occurrence_IDs_as_in_query_13_but_for_undersample + """)
"""

# 23
query_that_results_in_conditions_feature_matrix = """
SELECT
    visit_occurrence_id,
"""
for name_of_column in dictionary_of_codes_of_condition_and_names_of_column.values():
    max_block = """
    MAX(""" + name_of_column + """) as """ + name_of_column + """,
    """
    query_that_results_in_conditions_feature_matrix += max_block
query_that_results_in_conditions_feature_matrix += """
FROM (""" + query_that_results_in_ungrouped_conditions_feature_matrix + """)
GROUP BY visit_occurrence_id
ORDER BY visit_occurrence_id
"""

# 24
query_that_results_in_table_of_person_IDs_concept_codes_concept_names_and_visit_occurrence_IDs_from_table_d_exposure_and_table_d_standard_concept_for_undersample = """
SELECT
    person_id,
    concept_code,
    concept_name,
    visit_occurrence_id
FROM `""" + os.environ["WORKSPACE_CDR"] + """.drug_exposure` d_exposure
LEFT JOIN `""" + os.environ["WORKSPACE_CDR"] + """.concept` d_standard_concept
ON d_exposure.drug_concept_id = d_standard_concept.concept_id
WHERE d_exposure.person_id IN (""" + query_that_results_in_table_of_distinct_person_IDs_of_undersample + """)
"""

dictionary_of_codes_of_drug_and_names_of_column = {
    "5640": "is_exposed_to_ibuprofen",
    "1819": "is_exposed_to_buprenorphine",
    "2193": "is_exposed_to_nelaxone",
    "4337": "is_exposed_to_fentanyl",
    "7052": "is_exposed_to_morphine",
    "7804": "is_exposed_to_oxycodone",
    "3423": "is_exposed_to_hydromorphone",
    "1191": "is_exposed_to_aspirin",
    "2670": "is_exposed_to_codeine",
    "10689": "is_exposed_to_tramadol",
    "7238": "is_exposed_to_nalbuphine",
    "6754": "is_exposed_to_meperidine",
    "7243": "is_exposed_to_naltrexone",
    "161": "is_exposed_to_acetaminophen",
    "N02A": "is_exposed_to_Opioids"
}

# 25
query_that_results_in_ungrouped_drugs_feature_matrix = """
SELECT
    visit_occurrence_id,
"""
for code_of_drug, name_of_column in dictionary_of_codes_of_drug_and_names_of_column.items():
    case_block = """
CASE WHEN concept_code IN (""" + generate_query_that_results_in_table_of_codes_of_feature_that_is_child_of_provided_feature(code_of_drug) + """)
THEN 1
ELSE 0 END AS """ + name_of_column + """,
    """
    query_that_results_in_ungrouped_drugs_feature_matrix += case_block
query_that_results_in_ungrouped_drugs_feature_matrix += """
FROM (""" + query_that_results_in_table_of_person_IDs_concept_codes_concept_names_and_visit_occurrence_IDs_from_table_d_exposure_and_table_d_standard_concept_for_undersample + """)
"""

# 26
query_that_results_in_drugs_feature_matrix = """
SELECT
    visit_occurrence_id,
"""
for name_of_column in dictionary_of_codes_of_drug_and_names_of_column.values():
    max_block = """
    MAX(""" + name_of_column + """) as """ + name_of_column + """,
    """
    query_that_results_in_drugs_feature_matrix += max_block
query_that_results_in_drugs_feature_matrix += """
FROM (""" + query_that_results_in_ungrouped_drugs_feature_matrix + """)
GROUP BY visit_occurrence_id
ORDER BY visit_occurrence_id
"""

query_that_results_in_table_of_person_IDs_concept_codes_concept_names_and_visit_occurrence_IDs_from_table_procedure_and_table_p_standard_concept_for_undersample = """
SELECT
    person_id,
    concept_code,
    concept_name,
    visit_occurrence_id
FROM `""" + os.environ["WORKSPACE_CDR"] + """.procedure_occurrence` procedure
LEFT JOIN `""" + os.environ["WORKSPACE_CDR"] + """.concept` p_standard_concept
ON procedure.procedure_concept_id = p_standard_concept.concept_id
WHERE procedure.person_id IN (""" + query_that_results_in_table_of_distinct_person_IDs_of_undersample + """)
"""

dictionary_of_codes_of_procedure_and_names_of_column = {
    "71651007": "had_Mammography"
}

query_that_results_in_ungrouped_procedures_feature_matrix = """
SELECT
    visit_occurrence_id,
"""
for code_of_procedure, name_of_column in dictionary_of_codes_of_procedure_and_names_of_column.items():
    case_block = """
CASE WHEN concept_code IN (""" + generate_query_that_results_in_table_of_codes_of_feature_that_is_child_of_provided_feature(code_of_procedure) + """)
THEN 1
ELSE 0 END AS """ + name_of_column + """,
    """
    query_that_results_in_ungrouped_procedures_feature_matrix += case_block
query_that_results_in_ungrouped_procedures_feature_matrix += """
FROM (""" + query_that_results_in_table_of_person_IDs_concept_codes_concept_names_and_visit_occurrence_IDs_from_table_procedure_and_table_p_standard_concept_for_undersample + """)
"""

query_that_results_in_procedures_feature_matrix = """
SELECT
    visit_occurrence_id,
"""
for name_in_column in dictionary_of_codes_of_procedure_and_names_of_column.values():
    max_block = """
    MAX(""" + name_of_column + """) as """ + name_of_column + """,
    """
    query_that_results_in_procedures_feature_matrix += max_block
query_that_results_in_procedures_feature_matrix += """
FROM (""" + query_that_results_in_ungrouped_procedures_feature_matrix + """)
GROUP BY visit_occurrence_id
ORDER BY visit_occurrence_id
"""

query_that_results_in_table_of_person_IDs_concept_codes_concept_names_and_visit_occurrence_IDs_from_table_measurement_and_table_m_standard_concept_for_undersample = """
SELECT
    person_id,
    concept_code,
    concept_name,
    visit_occurrence_id
FROM `""" + os.environ["WORKSPACE_CDR"] + """.measurement` measurement
LEFT JOIN `""" + os.environ["WORKSPACE_CDR"] + """.concept` m_standard_concept
ON measurement.measurement_concept_id = m_standard_concept.concept_id
WHERE measurement.person_id IN (""" + query_that_results_in_table_of_distinct_person_IDs_of_undersample + """)
"""

dictionary_of_codes_of_measurement_and_names_of_column = {
    "LP14355-9": "had_Creatinine"
}

query_that_results_in_ungrouped_measurements_feature_matrix = """
SELECT
    visit_occurrence_id,
"""
for code_of_measurement, name_of_column in dictionary_of_codes_of_measurement_and_names_of_column.items():
    case_block = """
CASE WHEN concept_code IN (""" + generate_query_that_results_in_table_of_codes_of_feature_that_is_child_of_provided_feature(code_of_measurement) + """)
THEN 1
ELSE 0 END AS """ + name_of_column + """,
    """
    query_that_results_in_ungrouped_measurements_feature_matrix += case_block
query_that_results_in_ungrouped_measurements_feature_matrix += """
FROM (""" + query_that_results_in_table_of_person_IDs_concept_codes_concept_names_and_visit_occurrence_IDs_from_table_measurement_and_table_m_standard_concept_for_undersample + """)
"""

query_that_results_in_measurements_feature_matrix = """
SELECT
    visit_occurrence_id,
"""
for name_in_column in dictionary_of_codes_of_measurement_and_names_of_column.values():
    max_block = """
    MAX(""" + name_of_column + """) as """ + name_of_column + """,
    """
    query_that_results_in_measurements_feature_matrix += max_block
query_that_results_in_measurements_feature_matrix += """
FROM(""" + query_that_results_in_ungrouped_measurements_feature_matrix + """)
GROUP BY visit_occurrence_id
ORDER BY visit_occurrence_id
"""

column_names = (
    list(dictionary_of_codes_of_condition_and_names_of_column.values()) +
    list(dictionary_of_codes_of_drug_and_names_of_column.values()) +
    list(dictionary_of_codes_of_procedure_and_names_of_column.values())
)

query_that_results_in_feature_matrix = """
SELECT
   person_id,
   table_of_visit_occurrences_for_undersample.visit_occurrence_id,
   visit_start_date,
"""
for name_of_column in column_names:
    query_that_results_in_feature_matrix += """
    """ + name_of_column + ""","""
query_that_results_in_feature_matrix += """
FROM (""" + query_that_results_in_table_of_distint_person_IDs_visit_occurrence_IDs_and_visit_start_dates_for_undersample + """) table_of_visit_occurrences_for_undersample
LEFT JOIN (""" + query_that_results_in_conditions_feature_matrix + """) conditions_feature_matrix
ON table_of_visit_occurrences_for_undersample.visit_occurrence_id = conditions_feature_matrix.visit_occurrence_id
LEFT JOIN (""" + query_that_results_in_drugs_feature_matrix + """) medications_feature_matrix
ON table_of_visit_occurrences_for_undersample.visit_occurrence_id = medications_feature_matrix.visit_occurrence_id
LEFT JOIN (""" + query_that_results_in_procedures_feature_matrix + """) procedures_feature_matrix
ON table_of_visit_occurrences_for_undersample.visit_occurrence_id = procedures_feature_matrix.visit_occurrence_id
LEFT JOIN (""" + query_that_results_in_measurements_feature_matrix + """) measurements_feature_matrix
ON table_of_visit_occurrences_for_undersample.visit_occurrence_id = measurements_feature_matrix.visit_occurrence_id
ORDER BY person_id, visit_occurrence_id
"""

query_that_results_in_feature_matrix_with_rows_where_every_indicator_is_0_removed = """
SELECT *
FROM (""" + query_that_results_in_feature_matrix + """)
WHERE
    """ + column_names[0] + """ > 0"""
for i in range(1, len(column_names) - 1):
    query_that_results_in_feature_matrix_with_rows_where_every_indicator_is_0_removed += """
    OR """ + name_of_column + """ > 0"""
query_that_results_in_feature_matrix_with_rows_where_every_indicator_is_0_removed += """
ORDER BY person_id, visit_occurrence_id
"""

def interact_with_user():
    print("Started generating slice of feature matrix")
    answer = None
    while answer != "Y" and answer != "N":
        print("Would you like to remove rows where every indicator is 0 from slice of feature matrix (Y/N)?")
        answer = input()
    print("Answer: " + answer)
    query = None
    if answer == "N":
        query = query_that_results_in_feature_matrix
    elif answer == "Y":
        query = query_that_results_in_feature_matrix_with_rows_where_every_indicator_is_0_removed
    else:
        raise Exception("Answer is not 'Y' or 'N'")
    data_frame = get_data_frame(query)
    answer = None
    while not isinstance(answer, int) or answer < 0:
        print("How many distinct person ID's would you like there to be in slice of feature matrix (0 to 7,580)?")
        answer = input()
        try:
            answer = int(answer)
        except:
            continue
    print("Answer: " + str(answer))
    IntegerArray_of_distinct_person_IDs = pd.unique(data_frame["person_id"])
    IntegerArray_with_number_of_distinct_person_IDs_equal_to_answer = IntegerArray_of_distinct_person_IDs[:answer]
    data_frame = data_frame[data_frame["person_id"].isin(IntegerArray_with_number_of_distinct_person_IDs_equal_to_answer)]
    print("There are " + str(len(pd.unique(data_frame["person_id"]))) + " distinct patients in slice of feature matrix.")
    print("There are " + str(data_frame.shape[0]) + " visit occurrences and rows corresponding to those patients.")
    print(data_frame)
    print(data_frame[data_frame["is_exposed_to_Opioids"].isin([1])])
    path_of_Feature_Matrix = "Slice_Of_Feature_Matrix.csv"
    data_frame.to_csv(path_of_Feature_Matrix)
    print("Saved slice of feature matrix to " + path_of_Feature_Matrix)
    print("Finished generating slice of feature matrix")

if __name__ == "__main__":
    interact_with_user()
