import os
import pandas

def get_data_frame(query):
    data_frame = pandas.read_gbq(
        query = query,
        dialect = "standard",
        use_bqstorage_api = ("BIGQUERY_STORAGE_API_ENABLED" in os.environ),
        progress_bar_type="tqdm_notebook"
    )
    return data_frame

# 1
query_that_results_in_table_of_IDs_from_table_of_cancerous_conditions = """
SELECT cast(cr.id as string) as id
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
query_that_results_in_table_of_distinct_concept_IDs_from_joined_tables_of_conditions_and_cancerous_conditions = """
SELECT DISTINCT c.concept_id
FROM `""" + os.environ["WORKSPACE_CDR"] + """.cb_criteria` c
JOIN (""" + query_that_results_in_table_of_IDs_from_table_of_cancerous_conditions + """) a
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
query_that_results_in_table_of_distinct_combinations_of_person_IDs_entry_dates_and_concept_IDs_from_table_of_events_where_concept_IDs_correspond_to_cancer = """
    SELECT
        DISTINCT person_id,
        entry_date,
        concept_id 
    FROM `""" + os.environ["WORKSPACE_CDR"] + """.cb_search_all_events`
    WHERE
        concept_id IN (""" + query_that_results_in_table_of_distinct_concept_IDs_from_joined_tables_of_conditions_and_cancerous_conditions + """)
        AND is_standard = 1
"""

# 4
query_that_results_in_table_of_person_IDs_from_table_of_events_where_concept_IDs_correspond_to_cancer = """
    SELECT criteria.person_id 
    FROM (""" + query_that_results_in_table_of_distinct_combinations_of_person_IDs_entry_dates_and_concept_IDs_from_table_of_events_where_concept_IDs_correspond_to_cancer + """) criteria
"""

# 5
query_that_results_in_table_of_ID_of_concept_Opioids = """
    SELECT cast(cr.id as string) as id 
    FROM `""" + os.environ["WORKSPACE_CDR"] + """.cb_criteria` cr 
    WHERE
        concept_id IN (21604254) 
        AND full_text LIKE '%_rank1]%'
"""

# 6
query_that_results_in_table_of_distinct_IDs_of_opioids = """
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

# 7
query_that_results_in_table_of_distinct_IDs_of_descendants_of_opioids = """
    SELECT DISTINCT ca.descendant_id 
    FROM `""" + os.environ["WORKSPACE_CDR"] + """.cb_criteria_ancestor` ca 
    JOIN (""" + query_that_results_in_table_of_distinct_IDs_of_opioids + """) b 
    ON (ca.ancestor_id = b.concept_id)
"""

# 8
query_that_results_in_table_of_distinct_combinations_of_person_IDs_entry_dates_and_concept_IDs_from_table_of_events_where_concept_IDs_correspond_to_opioids = """
    SELECT
        DISTINCT person_id,
        entry_date,
        concept_id 
    FROM
        `""" + os.environ["WORKSPACE_CDR"] + """.cb_search_all_events` 
    WHERE
        concept_id IN (""" + query_that_results_in_table_of_distinct_IDs_of_descendants_of_opioids + """) 
        AND is_standard = 1
"""

# 9
query_that_results_in_table_of_person_IDs_from_table_of_events_where_concept_IDs_correspond_to_opioids = """
    SELECT criteria.person_id
    FROM (""" + query_that_results_in_table_of_distinct_combinations_of_person_IDs_entry_dates_and_concept_IDs_from_table_of_events_where_concept_IDs_correspond_to_opioids + """) criteria
"""

# 10
query_that_results_in_distinct_IDs_of_patients_with_at_least_one_prescription_of_opioids_and_without_cancer = """
    SELECT DISTINCT person_id
    FROM `""" + os.environ["WORKSPACE_CDR"] + """.cb_search_person` cb_search_person  
    WHERE
        cb_search_person.person_id IN (""" + query_that_results_in_table_of_person_IDs_from_table_of_events_where_concept_IDs_correspond_to_opioids + """) 
        AND cb_search_person.person_id NOT IN (""" + query_that_results_in_table_of_person_IDs_from_table_of_events_where_concept_IDs_correspond_to_cancer + """)
"""

# 11
query_that_results_in_table_of_occurrences_relating_to_patients_in_cohort = """
    SELECT *
    FROM `""" + os.environ["WORKSPACE_CDR"] + """.condition_occurrence` c_occurrence 
    WHERE c_occurrence.PERSON_ID IN (""" + query_that_results_in_distinct_IDs_of_patients_with_at_least_one_prescription_of_opioids_and_without_cancer + """)
"""

# 12
query_that_results_in_table_of_concept_IDs_and_codes = """
    SELECT concept_id, code
    FROM `""" + os.environ["WORKSPACE_CDR"] + """.cb_criteria` cr
    WHERE full_text LIKE '%_rank1]%'
"""

# 13
query_that_results_in_table_of_patients_conditions_and_start_datetimes = """
    SELECT
        c_occurrence.person_id,
        code,
        c_standard_concept.concept_name as standard_concept_name,
        c_occurrence.condition_start_datetime
    FROM (""" + query_that_results_in_table_of_occurrences_relating_to_patients_in_cohort + """) c_occurrence
    LEFT JOIN `""" + os.environ["WORKSPACE_CDR"] + """.concept` c_standard_concept
        ON c_occurrence.condition_concept_id = c_standard_concept.concept_id
    LEFT JOIN (""" + query_that_results_in_table_of_concept_IDs_and_codes + """) table_of_concept_IDs_and_codes
        ON c_occurrence.condition_concept_id = table_of_concept_IDs_and_codes.concept_id
"""

# 14
query_that_results_in_table_of_IDs_of_patients_in_cohort = """
    SELECT DISTINCT person_id
    FROM (""" + query_that_results_in_table_of_patients_conditions_and_start_datetimes + """)
"""

# 15
query_that_results_in_table_of_random_IDs_of_patients_in_cohort = """
    SELECT *
    FROM (""" + query_that_results_in_table_of_IDs_of_patients_in_cohort + """)
    ORDER BY RAND()
"""

# 16
query_that_results_in_table_of_3790_random_IDs_of_patients_in_cohort = """
    SELECT person_id
    FROM (""" + query_that_results_in_table_of_random_IDs_of_patients_in_cohort + """)
    LIMIT 3790
"""

# 17
query_that_results_in_table_of_opioid_abusers_conditions_and_start_datetimes = """
    SELECT *
    FROM (""" + query_that_results_in_table_of_patients_conditions_and_start_datetimes + """)
    WHERE standard_concept_name = "Opioid abuse"
"""

# 18
query_that_results_in_table_of_IDs_of_opioid_abusers_in_cohort = """
    SELECT person_id
    FROM (""" + query_that_results_in_table_of_opioid_abusers_conditions_and_start_datetimes + """)
"""

# 19
query_that_results_in_table_of_IDs_of_3790_random_patients_in_cohort_and_all_opioid_abusers_in_cohort = """(""" + query_that_results_in_table_of_3790_random_IDs_of_patients_in_cohort + """)
UNION ALL
(""" + query_that_results_in_table_of_IDs_of_opioid_abusers_in_cohort + """)"""

# 20
query_that_results_in_table_of_half_opioid_abusers_conditions_and_start_datetimes = """
    SELECT
        table_of_IDs_of_3790_random_patients_in_cohort_and_all_opioid_abusers_in_cohort.person_id,
        code,
        standard_concept_name,
        condition_start_datetime
    FROM (""" + query_that_results_in_table_of_patients_conditions_and_start_datetimes + """) table_of_patients_conditions_and_start_and_end_datetimes
    JOIN (""" + query_that_results_in_table_of_IDs_of_3790_random_patients_in_cohort_and_all_opioid_abusers_in_cohort + """) table_of_IDs_of_3790_random_patients_in_cohort_and_all_opioid_abusers_in_cohort
    ON table_of_patients_conditions_and_start_and_end_datetimes.person_id = table_of_IDs_of_3790_random_patients_in_cohort_and_all_opioid_abusers_in_cohort.person_id
"""
