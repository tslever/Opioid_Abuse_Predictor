import os


def generate_domain_feature_matrix(condition_code, name_of_column):

    # BELOW QUERIES ARE NOT NEEDED BECAUSE ITS GLOBAL VARIABLES AND CAN BE ACCESSED OUTSIDE OF FUNCTION
    # query_that_results_in_table_of_concept_IDs_and_codes
    # query_that_results_in_table_of_half_opioid_abusers_conditions_and_visit_occurrence_ids


    # QUERY 21 or #1
    query_that_results_in_table_with_ID_of_parent_condition= """
        SELECT cast(cr.id as string) as id
        FROM `""" + os.environ["WORKSPACE_CDR"] + """.cb_criteria` cr
        WHERE
            code IN (""" + condition_code + """)
            AND full_text LIKE '%_rank1]%'
    """

    # QUERY 22 or #2
    query_that_results_in_table_of_concept_IDs_of_conditions_that_are_children_of_parent_condition = """
        SELECT DISTINCT c.concept_id
        FROM `""" + os.environ["WORKSPACE_CDR"] + """.cb_criteria` c
        JOIN (""" + query_that_results_in_table_with_ID_of_parent_condition + """) a
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

    # QUERY 23 or #3
    query_that_results_in_table_of_codes_of_conditions_that_are_children_of_parent_condition = """
    SELECT code
    FROM (""" + query_that_results_in_table_of_concept_IDs_of_conditions_that_are_children_of_parent_condition + """) table_of_concept_IDs
    JOIN (""" + query_that_results_in_table_of_concept_IDs_and_codes + """) table_of_concept_ids_and_codes
    ON table_of_concept_IDs.concept_id = table_of_concept_ids_and_codes.concept_id
    """

    # QUERY 24 or #4
    query_that_results_in_table_of_person_IDs_visit_occurrence_ids_and_indicators_of_whether_patient_has_condition = """
    SELECT
        person_id,
        visit_occurrence_id,

        CASE WHEN code IN (""" + query_that_results_in_table_of_codes_of_conditions_that_are_children_of_parent_condition + """)
        THEN 1
        ELSE 0 END AS """ + name_of_column + """"

    FROM (""" + query_that_results_in_table_of_half_opioid_abusers_conditions_and_visit_occurrence_ids + """)
    """

    return query_that_results_in_table_of_person_IDs_visit_occurrence_ids_and_indicators_of_whether_patient_has_condition
