from opioidabusepredictor import queries
query_that_results_in_feature_matrix = """
SELECT
    person_id,
    table_of_visit_occurrences_for_cohort.visit_occurrence_id,
    table_of_visit_occurrences_for_cohort.visit_start_date,
    has_Anxiety,
    has_Bipolar_disorder,
    has_Depressive_disorder,
    has_Hypertensive_disorder,
    has_Opioid_abuse,
    has_Opioid_dependence,
    has_Pain,
    has_Rhinitis,
    has_Non_Opioid_Substance_abuse,
    is_exposed_to_ibuprofen,
    is_exposed_to_buprenorphine,
    is_exposed_to_nelaxone,
    is_exposed_to_fentanyl,
    is_exposed_to_morphine,
    is_exposed_to_oxycodone,
    is_exposed_to_hydromorphone,
    is_exposed_to_aspirin,
    is_exposed_to_codeine,
    is_exposed_to_tramadol,
    is_exposed_to_nalbuphine,
    is_exposed_to_meperidine,
    is_exposed_to_naltrexone,
    is_exposed_to_acetaminophen
FROM (""" + queries.query_that_results_in_table_of_visit_occurrences_for_cohort + """) table_of_visit_occurrences_for_cohort
LEFT JOIN (""" + queries.query_that_results_in_conditions_feature_matrix + """) conditions_feature_matrix
ON table_of_visit_occurrences_for_cohort.visit_occurrence_id = conditions_feature_matrix.visit_occurrence_id
LEFT JOIN (""" + queries.query_that_results_in_medications_feature_matrix + """) medications_feature_matrix
ON table_of_visit_occurrences_for_cohort.visit_occurrence_id = medications_feature_matrix.visit_occurrence_id
"""
data_frame = queries.get_data_frame(query_that_results_in_feature_matrix)
print(data_frame)
data_frame.to_csv("Feature_Matrix.csv")
