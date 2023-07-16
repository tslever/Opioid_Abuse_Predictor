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

# 11 for conditions
query_that_results_in_table_of_occurrences_relating_to_patients_in_cohort = """
    SELECT *
    FROM `""" + os.environ["WORKSPACE_CDR"] + """.condition_occurrence` c_occurrence 
    WHERE c_occurrence.PERSON_ID IN (""" + query_that_results_in_distinct_IDs_of_patients_with_at_least_one_prescription_of_opioids_and_without_cancer + """)
"""

# 12 for conditions
query_that_results_in_table_of_concept_IDs_and_codes = """
    SELECT concept_id, code
    FROM `""" + os.environ["WORKSPACE_CDR"] + """.cb_criteria` cr
    WHERE full_text LIKE '%_rank1]%'
"""

# 13 for conditions
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

# 14 for conditions
query_that_results_in_table_of_IDs_of_patients_in_cohort = """
    SELECT DISTINCT person_id
    FROM (""" + query_that_results_in_table_of_patients_conditions_and_start_datetimes + """)
"""

# 15 for conditions
query_that_results_in_table_of_random_IDs_of_patients_in_cohort = """
    SELECT *
    FROM (""" + query_that_results_in_table_of_IDs_of_patients_in_cohort + """)
    ORDER BY RAND()
"""

# 16 for conditions
query_that_results_in_table_of_3790_random_IDs_of_patients_in_cohort = """
    SELECT person_id
    FROM (""" + query_that_results_in_table_of_random_IDs_of_patients_in_cohort + """)
    LIMIT 3790
"""

# 17 for conditions
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

# 21
query_that_results_in_table_of_person_IDs_condition_start_datetimes_and_indicators_of_whether_patient_has_Anxiety = """
SELECT
    person_id,
    CAST(condition_start_datetime AS DATE) AS condition_start_datetime,

    CASE WHEN cast(code as INT64) IN (48694002, 35429005, 247808006, 431432003, 225635005, 300895004, 197480006, 67195008, 192038005, 192041001, 192044009, 22621000119103, 50026000, 34938008, 762331007, 51493001, 1686006, 52910006, 109006, 37868008, 53467004, 11806006, 69479009, 426174008, 111487009, 428687006, 21897009, 231504006, 191736004, 17496003, 50026000, 34938008, 762331007, 51493001, 1686006, 371631005, 35607004, 8185002, 5509004, 61212007, 56576003, 72861004, 65064003, 82494000, 43150009, 386810004, 70691001, 191722009, 61569007, 82415003, 35607004, 8185002, 5509004, 61212007, 54587008, 25501002, 62351001, 191725006, 58535001, 47505003, 16265301000119106, 16264901000119109, 16264821000119108, 126943008, 198288003, 70997004, 61387006, 79823003, 279622009)
    THEN 1
    ELSE 0 END AS has_Anxiety

    

FROM (""" + query_that_results_in_table_of_half_opioid_abusers_conditions_and_start_datetimes + """)
LIMIT 3
"""

# 22
query_that_results_in_conditions_feature_matrix = """
SELECT
    person_id,
    condition_start_datetime,
    MAX(has_Anxiety) as has_Anxiety
FROM (""" + query_that_results_in_table_of_person_IDs_condition_start_datetimes_and_indicators_of_whether_patient_has_Anxiety + """)
GROUP BY person_id, condition_start_datetime
ORDER BY person_id, condition_start_datetime
"""

# 11 for drugs
query_that_results_in_table_of_IDs_of_drugs = """
    SELECT cast(cr.id as string) as id 
    FROM `""" + os.environ["WORKSPACE_CDR"] + """.cb_criteria` cr 
    WHERE
        concept_id IN (1000560, 1112807, 1123698, 1123896, 1124957, 1125315, 1126658, 1129625, 1136980, 1154029, 1154343, 1174888, 1177480, 1518254, 1713332, 19011035, 19036781, 19049105, 21600002, 21600003, 21600011, 21600033, 21600038, 21600046, 21600047, 21600080, 21600081, 21600095, 21600101, 21600127, 21600248, 21600259, 21600279, 21600280, 21600360, 21600363, 21600490, 21600491, 21600492, 21600531, 21600532, 21600533, 21600565, 21600583, 21600600, 21600601, 21600651, 21600652, 21600712, 21600800, 21600857, 21600858, 21600859, 21600875, 21600876, 21600884, 21600885, 21600960, 21600961, 21600985, 21601136, 21601167, 21601179, 21601194, 21601195, 21601238, 21601278, 21601461, 21601462, 21601474, 21601489, 21601503, 21601516, 21601522, 21601605, 21601606, 21601607, 21601617, 21601634, 21601664, 21601665, 21601701, 21601738, 21601741, 21601782, 21601801, 21601815, 21601832, 21601853, 21601854, 21601855, 21601898, 21601899, 21601903, 21601908, 21601909, 21602002, 21602003, 21602004, 21602019, 21602054, 21602098, 21602099, 21602100, 21602104, 21602119, 21602143, 21602147, 21602158, 21602159, 21602163, 21602178, 21602182, 21602281, 21602282, 21602283, 21602323, 21602324, 21602341, 21602360, 21602361, 21602384, 21602391, 21602429, 21602452, 21602464, 21602627, 21602628, 21602629, 21602722, 21602723, 21602728, 21602796, 21602818, 21602819, 21602861, 21602868, 21602869, 21602968, 21602969, 21603006, 21603007, 21603035, 21603036, 21603041, 21603215, 21603216, 21603241, 21603246, 21603248, 21603249, 21603255, 21603274, 21603282, 21603283, 21603302, 21603311, 21603365, 21603395, 21603396, 21603444, 21603445, 21603446, 21603551, 21603552, 21603553, 21603616, 21603617, 21603638, 21603650, 21603651, 21603663, 21603671, 21603932, 21603933, 21603940, 21603966, 21604015, 21604016, 21604034, 21604035, 21604036, 21604069, 21604091, 21604181, 21604182, 21604200, 21604208, 21604220, 21604228, 21604253, 21604254, 21604255, 21604269, 21604291, 21604296, 21604303, 21604304, 21604343, 21604389, 21604390, 21604428, 21604443, 21604444, 21604456, 21604489, 21604564, 21604606, 21604635, 21604685, 21604686, 21605008, 21605009, 21605010, 21605042, 21605058, 21605067, 21605071, 21605096, 21605097, 21605125, 21605126, 21605144, 21605145, 21605146, 21605164, 21605165, 21605171, 21605172, 21605180, 21605181, 21605187, 21605188, 21605189, 21605199, 21605200, 21605204, 21605205, 21605227, 21605228, 21605229, 21605295, 21605297, 43534842, 43534855, 708298, 753626, 941258, 967823, 989878) 
        AND full_text LIKE '%_rank1]%'
"""

# 12 for drugs
query_that_results_in_table_of_IDs_of_drugs_whose_paths_include_the_IDs_of_drugs = """
    SELECT DISTINCT c.concept_id
    FROM `""" + os.environ["WORKSPACE_CDR"] + """.cb_criteria` c 
    JOIN (""" + query_that_results_in_table_of_IDs_of_drugs + """) a 
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

# 13 for drugs
query_that_results_in_table_of_IDs_of_descendants_of_drugs = """
    SELECT DISTINCT ca.descendant_id
    FROM `""" + os.environ["WORKSPACE_CDR"] + """.cb_criteria_ancestor` ca
    JOIN (""" + query_that_results_in_table_of_IDs_of_drugs_whose_paths_include_the_IDs_of_drugs + """) b
    ON (ca.ancestor_id = b.concept_id)
"""

# 14 for drugs
query_that_results_in_table_of_drugs_for_cohort = """
    SELECT *
    FROM `""" + os.environ["WORKSPACE_CDR"] + """.drug_exposure` d_exposure 
    WHERE
        drug_concept_id IN (""" + query_that_results_in_table_of_IDs_of_descendants_of_drugs + """)
        AND d_exposure.PERSON_ID IN (""" + query_that_results_in_distinct_IDs_of_patients_with_at_least_one_prescription_of_opioids_and_without_cancer + """)
"""

# 15 for drugs
query_that_results_in_table_of_patient_IDs_concept_codes_concept_names_and_drug_exposure_start_datetimes = """
    SELECT
        d_exposure.person_id,
        d_standard_concept.concept_code as standard_concept_code,
        d_standard_concept.concept_name as standard_concept_name,
        d_exposure.drug_exposure_start_datetime
    FROM (""" + query_that_results_in_table_of_drugs_for_cohort + """) d_exposure 
    LEFT JOIN
        `""" + os.environ["WORKSPACE_CDR"] + """.concept` d_standard_concept 
            ON d_exposure.drug_concept_id = d_standard_concept.concept_id 
"""

# 16 for drugs
query_that_results_in_table_of_person_IDs_drug_exposure_start_datetimes_and_indicators_of_whether_patient_is_exposed_to_ibuprofen = """
SELECT
    person_id,
    CAST(drug_exposure_start_datetime AS DATE) AS drug_exposure_start_datetime,

    CASE WHEN standard_concept_code IN ("5640")
    THEN 1
    ELSE 0 END AS is_exposed_to_ibuprofen

    

FROM (""" + query_that_results_in_table_of_patient_IDs_concept_codes_concept_names_and_drug_exposure_start_datetimes + """)
"""

# 17 for drugs
query_that_results_in_medications_feature_matrix = """
SELECT
    person_id,
    drug_exposure_start_datetime,
    MAX(is_exposed_to_ibuprofen) as is_exposed_to_ibuprofen
FROM (""" + query_that_results_in_table_of_person_IDs_drug_exposure_start_datetimes_and_indicators_of_whether_patient_is_exposed_to_ibuprofen + """)
GROUP BY person_id, drug_exposure_start_datetime
ORDER BY person_id, drug_exposure_start_datetime
"""
