import math
import numpy as np
import os
import pandas as pd
import time

def get_data_frame(query):
    data_frame = pd.read_gbq(
        query = query,
        dialect = "standard",
        use_bqstorage_api = ("BIGQUERY_STORAGE_API_ENABLED" in os.environ),
        progress_bar_type = "tqdm_notebook"
    )
    return data_frame

query_that_results_in_table_of_distinct_person_IDs_for_cohort = """
SELECT distinct person_id  
FROM `""" + os.environ["WORKSPACE_CDR"] + """.cb_search_person` cb_search_person  
WHERE
    cb_search_person.person_id IN (
        SELECT criteria.person_id 
        FROM (
            SELECT DISTINCT person_id, entry_date, concept_id 
            FROM `""" + os.environ["WORKSPACE_CDR"] + """.cb_search_all_events` 
            WHERE (
                concept_id IN (
                    SELECT DISTINCT ca.descendant_id 
                    FROM `""" + os.environ["WORKSPACE_CDR"] + """.cb_criteria_ancestor` ca 
                    JOIN (
                        select distinct c.concept_id 
                        FROM `""" + os.environ["WORKSPACE_CDR"] + """.cb_criteria` c 
                        JOIN (
                            select cast(cr.id as string) as id 
                            FROM `""" + os.environ["WORKSPACE_CDR"] + """.cb_criteria` cr 
                            WHERE
                                concept_id IN (21604254) 
                                AND full_text LIKE '%_rank1]%'
                        ) a 
                        ON (
                            c.path LIKE CONCAT('%.', a.id, '.%') 
                            OR c.path LIKE CONCAT('%.', a.id) 
                            OR c.path LIKE CONCAT(a.id, '.%') 
                            OR c.path = a.id
                        ) 
                        WHERE
                            is_standard = 1 
                            AND is_selectable = 1
                    ) b 
                    ON (ca.ancestor_id = b.concept_id)
                )
                AND is_standard = 1
            )
        ) criteria 
    ) 
    AND cb_search_person.person_id NOT IN (
        SELECT criteria.person_id 
        FROM (
            SELECT DISTINCT person_id, entry_date, concept_id 
            FROM `""" + os.environ["WORKSPACE_CDR"] + """.cb_search_all_events` 
            WHERE (
                concept_id IN (
                    SELECT DISTINCT c.concept_id 
                    FROM `""" + os.environ["WORKSPACE_CDR"] + """.cb_criteria` c 
                    JOIN (
                        select cast(cr.id as string) as id 
                        FROM `""" + os.environ["WORKSPACE_CDR"] + """.cb_criteria` cr 
                        WHERE
                            concept_id IN (40493428, 252840, 4092524, 4178964, 76349, 4177243, 4313083, 4241905, 46273652, 196925, 4180791, 436045, 200962, 4091466, 4162860, 4294416, 4092212, 444411, 4154630, 442139, 4309225, 4298032, 40650479, 197807, 4091465, 40490994, 4216273, 133424, 4247726, 4247822, 4181483, 4162994, 4309851, 436671, 4180780, 4300555, 4160780, 4180910, 4247238, 4155297, 4178971, 443568, 75210, 442183, 40492021, 78987, 443388, 195483, 4155285, 4181488, 25189, 4180902, 36676291, 376918, 4297666, 4157332, 40488919, 380661, 256633, 133711, 200348, 4174593, 433143, 4178977, 4178959, 443387, 40480128, 432263, 4312698, 4315805, 4112853, 4297185, 4181333, 442181, 46270738, 4178974, 4246450, 442131, 443391, 444410, 137809, 4181330, 4313916, 40487047, 192581, 4314047, 4092217, 4181351, 37397344, 4112745, 257503, 438693, 139750, 4181338, 198091, 436357, 196359, 432257, 4246451, 258375, 200052, 40488964, 256646, 436353, 438094, 4312023, 31509, 201238, 37310458, 4095312, 138996, 4162253, 378087, 435484, 374874, 36684472, 4314071, 4312288, 261514, 4181332, 44806773, 196360, 4188544, 4298026, 443392, 4295624, 432845, 45769720, 197507, 4233629, 4241917, 199754, 4160276, 440649, 443588, 4089655, 4092511, 439751, 432837, 4095740, 197808, 4162251, 200051, 4310566, 4187850, 26052, 4187849, 4311637, 4246017, 440658, 4247719, 4312685, 4241904, 4181480, 4311342, 4116084, 4111921, 36683531, 441805, 4298028, 437498, 4054503, 4298033, 192568, 4246127, 4312022, 4081044, 197500, 373425, 4338758, 443398, 376647, 4308621, 198988, 40491001, 40492037, 40490993, 4180793, 4181484, 4247338, 4297665, 441515, 74582, 442132, 261236, 4188545, 4180790, 4247331, 133726, 433716, 258369, 136917, 440345, 200959, 320342, 434298, 4334322, 4181350, 40492932, 4178968, 198700, 380055, 440956, 4111776, 4311619, 4247836, 4155171, 432833, 4111805, 433435, 201519, 438386, 4178976, 192855, 4157457, 75512, 4311617, 4157456, 24897, 45770892, 4187848, 80045, 4116241, 45768522, 4311499, 72266, 255507, 193144, 30346, 4315806, 443561, 28083, 4001666, 4089665, 4089756, 439404, 435755, 42536893, 4312944, 443390, 40493020, 4187851, 441233, 4248061, 4247357, 4157454, 4313482, 438699, 318096, 4112974, 432254, 4158563, 4091464, 436635, 198985, 40482750, 254591, 4181343, 197804, 4151250, 40487143, 443386, 46270513, 4157449, 4181354, 4181339, 133147, 443389, 4092382, 45757101, 4313634, 4307263, 4092512, 194589, 253717, 136354, 764225, 195513, 4153882, 4157331, 4214901, 4181477) 
                            AND full_text LIKE '%_rank1]%'
                    ) a 
                    ON (
                        c.path LIKE CONCAT('%.', a.id, '.%') 
                        OR c.path LIKE CONCAT('%.', a.id) 
                        OR c.path LIKE CONCAT(a.id, '.%') 
                        OR c.path = a.id
                    ) 
                    WHERE
                        is_standard = 1 
                        AND is_selectable = 1
                ) 
                AND is_standard = 1 
            )
        ) criteria 
    )
"""

query_that_results_in_table_of_visit_occurrences_for_cohort = """
SELECT person_id, visit_occurrence_id, visit_start_datetime
FROM `""" + os.environ["WORKSPACE_CDR"] + """.visit_occurrence` visit_occurrence 
WHERE visit_occurrence.PERSON_ID IN (""" + query_that_results_in_table_of_distinct_person_IDs_for_cohort + """)
"""

def generate_query_that_results_in_table_of_positive_indicators_for_condition(name_of_column, tuple_of_concept_IDs, query_that_results_in_table_of_distinct_person_IDs):
    query_that_results_in_table_of_positive_indicators = """
SELECT
    c_occurrence.visit_occurrence_id,
    1 AS """ + name_of_column + """
FROM (
    SELECT *
    FROM `""" + os.environ["WORKSPACE_CDR"] + """.condition_occurrence` c_occurrence
    WHERE (
        condition_concept_id IN (
            SELECT DISTINCT c.concept_id
            FROM `""" + os.environ["WORKSPACE_CDR"] + """.cb_criteria` c
            JOIN (
                select cast(cr.id as string) as id
                FROM `""" + os.environ["WORKSPACE_CDR"] + """.cb_criteria` cr
                WHERE
                    concept_id IN (""" + ", ".join([str(concept_ID) for concept_ID in tuple_of_concept_IDs]) + """)
                    AND full_text LIKE '%_rank1]%'
            ) a
            ON (
                c.path LIKE CONCAT('%.', a.id, '.%')
                OR c.path LIKE CONCAT('%.', a.id)
                OR c.path LIKE CONCAT(a.id, '.%')
                OR c.path = a.id
            )
            WHERE
                is_standard = 1
                AND is_selectable = 1
            )
        )
        AND c_occurrence.PERSON_ID IN (""" + query_that_results_in_table_of_distinct_person_IDs + """)
    ) c_occurrence
GROUP BY c_occurrence.visit_occurrence_id
    """
    return query_that_results_in_table_of_positive_indicators

# A0
query_that_results_in_table_of_positive_indicators_of_Opioid_abuse = generate_query_that_results_in_table_of_positive_indicators_for_condition(
    name_of_column = "has_Opioid_abuse",
    tuple_of_concept_IDs = (37016268, 4099935, 438130),
    query_that_results_in_table_of_distinct_person_IDs = query_that_results_in_table_of_distinct_person_IDs_for_cohort
)

def generate_query_that_results_in_table_of_positive_indicators_for_drug(name_of_column, tuple_of_concept_IDs, query_that_results_in_table_of_distinct_person_IDs):
    query_that_results_in_table_of_positive_indicators = """
SELECT
    d_exposure.visit_occurrence_id,
    1 AS """ + name_of_column + """
FROM (
    SELECT * 
    FROM `""" + os.environ["WORKSPACE_CDR"] + """.drug_exposure` d_exposure 
    WHERE
        drug_concept_id IN (
            SELECT DISTINCT ca.descendant_id 
            FROM `""" + os.environ["WORKSPACE_CDR"] + """.cb_criteria_ancestor` ca 
            JOIN (
                select distinct c.concept_id 
                FROM `""" + os.environ["WORKSPACE_CDR"] + """.cb_criteria` c 
                JOIN (
                    select cast(cr.id as string) as id 
                    FROM `""" + os.environ["WORKSPACE_CDR"] + """.cb_criteria` cr 
                    WHERE
                        concept_id IN (""" + ", ".join([str(concept_ID) for concept_ID in tuple_of_concept_IDs]) + """)
                        AND full_text LIKE '%_rank1]%'
                ) a 
                ON (
                    c.path LIKE CONCAT('%.', a.id, '.%') 
                    OR c.path LIKE CONCAT('%.', a.id) 
                    OR c.path LIKE CONCAT(a.id, '.%') 
                    OR c.path = a.id
                ) 
                WHERE
                    is_standard = 1 
                    AND is_selectable = 1
            ) b 
            ON (
                ca.ancestor_id = b.concept_id
            )
        )  
        AND d_exposure.PERSON_ID IN (""" + query_that_results_in_table_of_distinct_person_IDs + """)
) d_exposure
GROUP BY d_exposure.visit_occurrence_id
    """
    return(query_that_results_in_table_of_positive_indicators)


# A1
query_that_results_in_table_of_positive_indicators_of_Opioids = generate_query_that_results_in_table_of_positive_indicators_for_drug(
    name_of_column = "is_exposed_to_Opioids",
    tuple_of_concept_IDs = (1123896, 21600593, 21604200, 21604254, 21604291, 21604296, 21604825),
    query_that_results_in_table_of_distinct_person_IDs = query_that_results_in_table_of_distinct_person_IDs_for_cohort
)

query_that_results_in_table_of_visit_occurrences_has_Opioid_abuse_and_is_exposed_to_Opioids = """
SELECT
    person_id,
    table_of_visit_occurrences.visit_occurrence_id,
    visit_start_datetime,
    has_Opioid_abuse,
    is_exposed_to_Opioids
FROM (""" + query_that_results_in_table_of_visit_occurrences_for_cohort + """) table_of_visit_occurrences
LEFT JOIN (""" + query_that_results_in_table_of_positive_indicators_of_Opioid_abuse + """) table_of_Opioid_abuse
ON table_of_visit_occurrences.visit_occurrence_id = table_of_Opioid_abuse.visit_occurrence_id
LEFT JOIN (""" + query_that_results_in_table_of_positive_indicators_of_Opioids + """) table_of_Opioids
ON table_of_visit_occurrences.visit_occurrence_id = table_of_Opioids.visit_occurrence_id
"""

query_that_results_in_table_of_distinct_person_IDs_of_cohort_who_have_Opioid_abuse = """
SELECT DISTINCT person_id
FROM (""" + query_that_results_in_table_of_visit_occurrences_has_Opioid_abuse_and_is_exposed_to_Opioids + """)
WHERE has_Opioid_abuse = 1
"""

query_that_results_in_table_of_distinct_person_IDs_of_cohort_who_have_Opioid_abuse_and_are_exposed_to_Opioids = """
SELECT DISTINCT person_id
FROM (""" + query_that_results_in_table_of_visit_occurrences_has_Opioid_abuse_and_is_exposed_to_Opioids + """)
WHERE
    person_id IN (""" + query_that_results_in_table_of_distinct_person_IDs_of_cohort_who_have_Opioid_abuse + """)
    AND is_exposed_to_Opioids = 1
"""

query_that_results_in_number_of_distinct_person_IDs_of_cohort_who_have_Opioid_abuse_and_are_exposed_to_Opioids = """
SELECT COUNT(person_id) as number_of_distinct_person_IDs_of_patients_who_have_Opioid_abuse_and_are_exposed_to_Opioids
FROM (""" + query_that_results_in_table_of_distinct_person_IDs_of_cohort_who_have_Opioid_abuse_and_are_exposed_to_Opioids + """)
"""

query_that_results_in_table_of_distinct_person_IDs_of_cohort_who_do_not_have_Opioid_abuse_and_are_exposed_to_Opioids = """
SELECT DISTINCT person_id
FROM (""" + query_that_results_in_table_of_visit_occurrences_has_Opioid_abuse_and_is_exposed_to_Opioids + """)
WHERE
    person_id NOT IN (""" + query_that_results_in_table_of_distinct_person_IDs_of_cohort_who_have_Opioid_abuse + """)
    AND is_exposed_to_Opioids = 1
"""

query_that_results_in_table_of_person_IDs_of_cohort_who_do_not_have_Opioid_abuse_and_are_exposed_to_Opioids_with_equal_number = """
SELECT person_id
FROM (""" + query_that_results_in_table_of_distinct_person_IDs_of_cohort_who_do_not_have_Opioid_abuse_and_are_exposed_to_Opioids + """)
ORDER BY RAND()
LIMIT 2448
"""

query_that_results_in_table_of_distinct_person_IDs_for_undersample = """(""" + query_that_results_in_table_of_distinct_person_IDs_of_cohort_who_have_Opioid_abuse_and_are_exposed_to_Opioids + """)
UNION DISTINCT
(""" + query_that_results_in_table_of_person_IDs_of_cohort_who_do_not_have_Opioid_abuse_and_are_exposed_to_Opioids_with_equal_number + """)
"""

query_that_results_in_table_of_visit_occurrences_for_undersample = """
SELECT *
FROM (""" + query_that_results_in_table_of_visit_occurrences_for_cohort + """)
WHERE person_id IN (""" + query_that_results_in_table_of_distinct_person_IDs_for_undersample + """)
"""

def generate_query_that_results_in_table_of_positive_indicators_for_condition_and_undersample(name_of_column, tuple_of_concept_IDs):
    return generate_query_that_results_in_table_of_positive_indicators_for_condition(name_of_column, tuple_of_concept_IDs, query_that_results_in_table_of_distinct_person_IDs_for_undersample)

# C0
query_that_results_in_table_of_positive_indicators_of_Anxiety = generate_query_that_results_in_table_of_positive_indicators_for_condition_and_undersample(
    name_of_column = "has_Anxiety",
    tuple_of_concept_IDs = (35615152, 35615153, 35615155, 36684319, 37109206, 37117155, 381537, 4009184, 4021501, 4035299, 4039212, 4056690, 4084699, 4085058, 4087190, 4098314, 4113821, 4115221, 4117364, 4146660, 4178114, 4193634, 4198826, 4199892, 4203449, 4214746, 4216670, 4221077, 4242095, 42538592, 42538968, 4261239, 4263429, 4288011, 4304010, 4322025, 4328276, 433178, 4338031, 434613, 434628, 436074, 436075, 440690, 441542, 442077, 44784526, 763092)
)

# C1
query_that_results_in_table_of_positive_indicators_of_Bipolar_disorder = generate_query_that_results_in_table_of_positive_indicators_for_condition_and_undersample(
    name_of_column = "has_Bipolar_disorder",
    tuple_of_concept_IDs = (35622934, 35624743, 35624744, 35624745, 35624747, 35624748, 37109940, 37117177, 372599, 37312578, 4001733, 4009648, 4028027, 4030856, 4037669, 4071442, 4102603, 4144519, 4148842, 4148934, 4150985, 4154283, 4155798, 4161200, 4166701, 4172156, 4177651, 4185096, 4194222, 4195158, 4200385, 4201739, 4215917, 4220617, 4220618, 4244078, 4262111, 4280361, 42872412, 42872413, 4287544, 43020451, 4307804, 4307956, 4310821, 432290, 4324945, 4327669, 432866, 432876, 433743, 433992, 435225, 435226, 436072, 436086, 436386, 436665, 437250, 437528, 437529, 439001, 439245, 439246, 439248, 439249, 439250, 439251, 439253, 439254, 439255, 439256, 439785, 440067, 440078, 440079, 441834, 441836, 442570, 442600, 443797, 443906)
)

# C2
query_that_results_in_table_of_positive_indicators_of_Depression = generate_query_that_results_in_table_of_positive_indicators_for_condition_and_undersample(
    name_of_column = "has_Depression",
    tuple_of_concept_IDs = (35615152, 35615153, 35615155, 35622934, 35624743, 35624748, 3656234, 36713698, 36714389, 36714998, 36715000, 36717092, 37016718, 37018656, 37110429, 37111697, 377527, 379784, 4001733, 4025677, 4031328, 4037669, 40481798, 4049623, 4077577, 4094358, 4095285, 4098302, 4101137, 4103126, 4103574, 4114950, 4124706, 4129184, 4141292, 4141454, 4144233, 4144519, 4148630, 4148934, 4149320, 4149321, 4151170, 4152280, 4154309, 4154391, 4161569, 4174987, 4176002, 4185096, 4191716, 4195572, 4205471, 4214898, 4223090, 4224940, 4228802, 4239471, 4250023, 4262111, 4263748, 4269493, 4282096, 4282316, 42872411, 42872722, 4298317, 43021839, 4304140, 4307111, 4314692, 432285, 4323418, 4324945, 4324959, 4327337, 4328217, 432883, 433440, 4336957, 433751, 4338031, 433991, 434911, 435220, 43531624, 435520, 438406, 438727, 438998, 439254, 439259, 440078, 440383, 440698, 441534, 443864, 44782943, 762503, 762504)
)

# C3
query_that_results_in_table_of_positive_indicators_of_Hypertension = generate_query_that_results_in_table_of_positive_indicators_for_condition_and_undersample(
    name_of_column = "has_Hypertension",
    tuple_of_concept_IDs = (193493, 195556, 201313, 312938, 313502, 314369, 314378, 316866, 316994, 319034, 37208172, 376965, 377551, 4013643, 40481896, 4057978, 4062552, 4116347, 4173820, 4183981, 4207534, 4263504, 42709887, 4298200, 43020424, 43020455, 43020457, 43021835, 43021852, 4322893, 4342636, 439694, 439695, 439696, 439698, 442603, 442604, 442626, 442766, 443919, 444101, 44782728, 44784439, 44784621, 44784638, 44784639, 45757137, 45757139, 45757140, 45757356, 45768449)
)

# C4
query_that_results_in_table_of_positive_indicators_of_Opioid_dependence = generate_query_that_results_in_table_of_positive_indicators_for_condition_and_undersample(
    name_of_column = "has_Opioid_dependence",
    tuple_of_concept_IDs = (37018689, 37209507, 4099809, 4100520, 4102817, 4103413, 42872387, 432301, 4336384, 438120, 440379, 440693)
)

# C5
query_that_results_in_table_of_positive_indicators_of_Pain = generate_query_that_results_in_table_of_positive_indicators_for_condition_and_undersample(
    name_of_column = "has_Pain",
    tuple_of_concept_IDs = (134159, 134736, 137856, 138525, 138845, 192239, 193322, 194133, 194175, 194696, 195083, 197381, 197684, 197988, 198263, 200219, 201347, 201418, 201626, 24134, 259153, 312998, 315832, 35611566, 36674979, 36684444, 36684573, 36684575, 36686912, 36686913, 36712805, 36712807, 37017182, 37108940, 37108941, 37118025, 37204166, 37209603, 37209605, 373852, 375825, 378253, 379031, 380733, 4002014, 4002956, 4008102, 4009391, 4009890, 4010016, 4010017, 4010025, 4010361, 4012075, 4012198, 4012199, 4012222, 4012223, 4012234, 4012690, 4021670, 4024243, 4024561, 4046660, 40481180, 40481920, 40485490, 4048710, 4058670, 4071874, 4077895, 4079724, 4082798, 4083297, 4083769, 4083770, 4083779, 4090433, 4090553, 4090564, 4092930, 4099171, 4099176, 4103476, 4109083, 4109084, 4109085, 4111231, 4115169, 4115170, 4115171, 4115367, 4115368, 4115406, 4115408, 4115409, 4115410, 4115411, 4116166, 4116809, 4116810, 4116811, 4116987, 4116988, 4117695, 4127066, 4128083, 4129418, 4132891, 4132892, 4132926, 4132929, 4132931, 4133037, 4133039, 4133040, 4133638, 4133643, 4134577, 4137674, 4139512, 4144753, 4145372, 4147441, 4147829, 4149024, 4150062, 4150125, 4150129, 4150759, 4160062, 4160900, 4166666, 4167250, 4168213, 4168216, 4168686, 4169580, 4169905, 4170554, 4170962, 4176923, 4182327, 4182562, 4200298, 4201930, 4204199, 4208857, 4218101, 4218793, 4237198, 4237315, 4237595, 4241033, 4244072, 4253797, 42538688, 42539051, 42539474, 4256912, 4260916, 4263576, 4264107, 4264144, 4270932, 4279301, 4297894, 4302739, 4306292, 4308696, 4317968, 4322528, 4322871, 4329041, 4330445, 4331953, 4333227, 433456, 43530621, 43530622, 43530661, 43531612, 436096, 438867, 439080, 439502, 440704, 441334, 442287, 442555, 442752, 443464, 444391, 44782778, 44784631, 45757565, 45763561, 45768450, 45769207, 45771676, 45773181, 46273207, 73819, 75863, 759905, 759906, 759907, 759908, 759909, 759911, 759912, 760837, 760912, 760919, 761157, 761158, 761159, 761703, 761704, 762287, 762288, 762289, 762290, 762291, 762292, 762293, 762294, 762296, 762297, 762298, 762299, 762361, 762377, 762941, 76388, 76458, 765060, 765061, 765131, 765268, 765384, 765422, 765423, 765933, 77074, 77670, 78232, 78234, 78508, 78517)
)

# C6
query_that_results_in_table_of_positive_indicators_of_Rhinitis = generate_query_that_results_in_table_of_positive_indicators_for_condition_and_undersample(
    name_of_column = "has_Rhinitis",
    tuple_of_concept_IDs = (252936, 256439, 257007, 259848, 260427, 40481374, 4048171, 40486433, 4049223, 4051476, 4101701, 4110489, 4177553, 4270705, 42709857, 4280726, 42872416, 42873159, 4305500, 4309214, 4316066, 4320791, 4327870, 443558, 45757082, 45766684, 45766713, 46269743, 46269744, 46269789, 46270028, 46270030, 46270156, 46273452, 46273454)
)

# C7
query_that_results_in_table_of_positive_indicators_of_Non_Opioid_Substance_Abuse = generate_query_that_results_in_table_of_positive_indicators_for_condition_and_undersample(
    name_of_column = "has_Non_Opioid_Substance_Abuse",
    tuple_of_concept_IDs = (193256, 195300, 196463, 201343, 201612, 318773, 35623150, 35624505, 3654548, 3654785, 36676296, 36676297, 36713086, 36714559, 36715277, 36715922, 36716473, 37016173, 37016176, 37016267, 37017563, 37018356, 37108712, 37108954, 37109022, 37109023, 37110409, 37110429, 37110436, 37110437, 37110441, 37110444, 37110445, 37110468, 37110472, 37110474, 37110490, 37110493, 37116661, 37117049, 37119151, 37203948, 37209507, 372607, 37309681, 37309774, 37311993, 37312034, 37312450, 374317, 374623, 375504, 375519, 375794, 376383, 377830, 378421, 378726, 4002572, 4004672, 4009647, 4010023, 4012869, 4020146, 4022082, 4029464, 4035299, 4035966, 4041191, 4042889, 4042893, 4044237, 40479573, 40482898, 40483111, 40483172, 40483827, 40484946, 4052690, 4053640, 4056690, 4063372, 4078688, 4080762, 4088373, 4089921, 4091714, 4091715, 4092159, 4094638, 4094639, 4096598, 4097389, 4097390, 4099334, 4099809, 4099811, 4100520, 4100526, 4101256, 4102817, 4102821, 4103413, 4103418, 4103419, 4103424, 4103426, 4103853, 4104431, 4104707, 4109691, 4132097, 4136662, 4137236, 4137955, 4139421, 4141524, 4143110, 4143732, 4145220, 4146660, 4146716, 4146763, 4148093, 4148140, 4150794, 4152165, 4155336, 4157200, 4159804, 4159806, 4159810, 4160957, 4161894, 4164432, 4166129, 4168706, 4171175, 4171789, 4171795, 4173746, 4174619, 4174805, 4175635, 4176120, 4176286, 4176464, 4176983, 4178114, 4180736, 4181940, 4183441, 4184438, 4191592, 4192127, 4193868, 4197130, 4197434, 4198826, 4199244, 4202330, 4203152, 4203257, 4205002, 4206341, 4209423, 4214950, 4216493, 4217840, 4218081, 4218106, 4220072, 4220197, 4221077, 4224276, 4224791, 4228331, 4232492, 4233811, 4234597, 4236877, 4237906, 4239381, 4239812, 4245794, 4245840, 42536419, 42537692, 42538589, 42538592, 42539146, 42539355, 4262566, 4264766, 4264889, 4267413, 4272033, 4272313, 4275756, 4279309, 4287251, 4288013, 4290062, 4290538, 4300092, 43020446, 43020473, 43021844, 4302744, 4307098, 4308292, 4310679, 4313135, 4319165, 4319166, 4322698, 432302, 432303, 432304, 4323272, 4323639, 4324044, 432609, 4326515, 4327117, 432878, 432884, 4331287, 433180, 4332880, 4332991, 433452, 433458, 433473, 433735, 433745, 433746, 433753, 4338023, 4338026, 433935, 433994, 434015, 434016, 434019, 4340383, 4340385, 4340386, 4340493, 4340964, 434327, 434328, 434627, 434916, 434917, 434921, 435140, 435231, 435243, 43530680, 43530681, 435532, 435533, 435534, 435718, 435809, 436089, 436097, 436098, 436296, 436389, 436585, 436953, 436954, 437245, 437257, 437264, 437533, 437838, 438126, 438306, 438393, 438648, 438732, 439005, 439277, 439312, 439313, 439554, 439796, 440002, 440069, 440270, 440380, 440381, 440387, 440612, 440685, 440692, 440694, 440891, 440892, 440992, 440996, 441198, 441260, 441261, 441262, 441272, 441276, 441465, 441833, 442582, 442601, 442914, 443236, 443274, 443534, 443930, 444038, 444363, 44782445, 44782714, 44782987, 44783367, 44784619, 44784627, 45757093, 45757783, 45766641, 45766642, 45769462, 45773120, 46269816, 46269817, 46269818, 46269835, 46273635, 761844, 765451)
)

def generate_query_that_results_in_table_of_positive_indicators_for_drug_and_undersample(name_of_column, tuple_of_concept_IDs):
    return generate_query_that_results_in_table_of_positive_indicators_for_drug(name_of_column, tuple_of_concept_IDs, query_that_results_in_table_of_distinct_person_IDs_for_undersample)

# D0
query_that_results_in_table_of_positive_indicators_of_ibuprofen = generate_query_that_results_in_table_of_positive_indicators_for_drug_and_undersample(
    name_of_column = "is_exposed_to_ibuprofen",
    tuple_of_concept_IDs = (1177480,)
)

#D1
query_that_results_in_table_of_positive_indicators_of_buprenorphine = generate_query_that_results_in_table_of_positive_indicators_for_drug_and_undersample(
    name_of_column = "is_exposed_to_buprenorphine",
    tuple_of_concept_IDs = (1133201,)
)

#D2
query_that_results_in_table_of_positive_indicators_of_fentanyl = generate_query_that_results_in_table_of_positive_indicators_for_drug_and_undersample(
    name_of_column = "is_exposed_to_fentanyl",
    tuple_of_concept_IDs = (1154029,)
)

#D3
query_that_results_in_table_of_positive_indicators_of_morphine = generate_query_that_results_in_table_of_positive_indicators_for_drug_and_undersample(
    name_of_column = "is_exposed_to_morphine",
    tuple_of_concept_IDs = (1110410,)
)

#D4
query_that_results_in_table_of_positive_indicators_of_oxycodone = generate_query_that_results_in_table_of_positive_indicators_for_drug_and_undersample(
    name_of_column = "is_exposed_to_oxycodone",
    tuple_of_concept_IDs = (1124957,)
)

#D5
query_that_results_in_table_of_positive_indicators_of_hydromorphone = generate_query_that_results_in_table_of_positive_indicators_for_drug_and_undersample(
    name_of_column = "is_exposed_to_hydromorphone",
    tuple_of_concept_IDs = (1126658,)
)

#D6
query_that_results_in_table_of_positive_indicators_of_aspirin = generate_query_that_results_in_table_of_positive_indicators_for_drug_and_undersample(
    name_of_column = "is_exposed_to_aspirin",
    tuple_of_concept_IDs = (1112807,)
)

#D7
query_that_results_in_table_of_positive_indicators_of_codeine = generate_query_that_results_in_table_of_positive_indicators_for_drug_and_undersample(
    name_of_column = "is_exposed_to_codeine",
    tuple_of_concept_IDs = (1201620,)
)

#D8
query_that_results_in_table_of_positive_indicators_of_tramadol = generate_query_that_results_in_table_of_positive_indicators_for_drug_and_undersample(
    name_of_column = "is_exposed_to_tramadol",
    tuple_of_concept_IDs = (1103314,)
)

#D9
query_that_results_in_table_of_positive_indicators_of_nalbuphine = generate_query_that_results_in_table_of_positive_indicators_for_drug_and_undersample(
    name_of_column = "is_exposed_to_nalbuphine",
    tuple_of_concept_IDs = (1114122,)
)

#D10
query_that_results_in_table_of_positive_indicators_of_meperidine = generate_query_that_results_in_table_of_positive_indicators_for_drug_and_undersample(
    name_of_column = "is_exposed_to_meperidine",
    tuple_of_concept_IDs = (1102527,)
)

#D11
query_that_results_in_table_of_positive_indicators_of_naltrexone = generate_query_that_results_in_table_of_positive_indicators_for_drug_and_undersample(
    name_of_column = "is_exposed_to_naltrexone",
    tuple_of_concept_IDs = (1714319,)
)

#D12
query_that_results_in_table_of_positive_indicators_of_acetaminophen = generate_query_that_results_in_table_of_positive_indicators_for_drug_and_undersample(
    name_of_column = "is_exposed_to_acetaminophen",
    tuple_of_concept_IDs = (1125315,)
)

def generate_query_that_results_in_table_of_positive_indicators_for_procedure_and_undersample(name_of_column, tuple_of_concept_IDs):
    query_that_results_in_table_of_positive_indicators_for_procedure_and_undersample = """
SELECT
    procedure.visit_occurrence_id,
    1 AS """ + name_of_column + """
FROM (
    SELECT * 
    FROM `""" + os.environ["WORKSPACE_CDR"] + """.procedure_occurrence` procedure 
    WHERE
        procedure_concept_id IN (
            SELECT DISTINCT c.concept_id 
            FROM `""" + os.environ["WORKSPACE_CDR"] + """.cb_criteria` c 
            JOIN (
                select cast(cr.id as string) as id 
                FROM `""" + os.environ["WORKSPACE_CDR"] + """.cb_criteria` cr 
                WHERE concept_id IN (""" + ", ".join([str(concept_ID) for concept_ID in tuple_of_concept_IDs]) + """)
                AND full_text LIKE '%_rank1]%'
            ) a 
            ON (
                c.path LIKE CONCAT('%.', a.id, '.%') 
                OR c.path LIKE CONCAT('%.', a.id)
                OR c.path LIKE CONCAT(a.id, '.%')
                OR c.path = a.id
            )
            WHERE
                is_standard = 1 
                AND is_selectable = 1
        )
        AND procedure.PERSON_ID IN (""" + query_that_results_in_table_of_distinct_person_IDs_for_undersample + """)
) procedure
GROUP BY procedure.visit_occurrence_id
    """
    return query_that_results_in_table_of_positive_indicators_for_procedure_and_undersample

# P0
query_that_results_in_table_of_positive_indicators_of_Mammography = generate_query_that_results_in_table_of_positive_indicators_for_procedure_and_undersample(
    name_of_column = "had_Mammography",
    tuple_of_concept_IDs = (4324693,)
)

# P1
query_that_results_in_table_of_positive_indicators_of_Knee_procedure = generate_query_that_results_in_table_of_positive_indicators_for_procedure_and_undersample(
    name_of_column = "had_Knee_procedure",
    tuple_of_concept_IDs = (2617368, 4030239, 4042660, 4043028, 40482787, 40482788, 4078547, 4079526, 4106050, 4106397, 4147773, 4195136, 4197548, 4205229, 4241716, 4263550, 4268018, 4268896, 4298098, 4311039, 4343454, 4343455, 4343907, 43531648)
)

# P2
query_that_results_in_table_of_positive_indicators_of_Tooth_procedure = generate_query_that_results_in_table_of_positive_indicators_for_procedure_and_undersample(
    name_of_column = "had_Tooth_procedure",
    tuple_of_concept_IDs = (40217364, 4040556, 40487004, 4050720, 4101094, 4120794, 4120795, 4123251, 4142228, 4208393, 4276519, 4287086)
)

# P3
query_that_results_in_table_of_positive_indicators_of_Hip_procedure = generate_query_that_results_in_table_of_positive_indicators_for_procedure_and_undersample(
    name_of_column = "had_Hip_procedure",
    tuple_of_concept_IDs = (4010119, 4041270, 4042331, 4102292, 4134857, 4162099, 4165513, 4203771, 4266062, 4297365, 4327115)
)

# P4
query_that_results_in_table_of_positive_indicators_of_Vascular_procedure = generate_query_that_results_in_table_of_positive_indicators_for_procedure_and_undersample(
    name_of_column = "had_Vascular_procedure",
    tuple_of_concept_IDs = (4020466, 4045839, 4050410, 4148948, 4159959, 4160912, 4181966, 4214091, 4284104, 4295278)
)

# P5
query_that_results_in_table_of_positive_indicators_of_Brain_procedure = generate_query_that_results_in_table_of_positive_indicators_for_procedure_and_undersample(
    name_of_column = "had_Brain_procedure",
    tuple_of_concept_IDs = (4043201, 4045859, 4046832, 4120973, 4146487, 4175191, 4213313, 4214763, 42537289, 4323283, 44784260)
)

# P6
query_that_results_in_table_of_positive_indicators_of_Heart_procedure = generate_query_that_results_in_table_of_positive_indicators_for_procedure_and_undersample(
    name_of_column = "had_Heart_procedure",
    tuple_of_concept_IDs = (4000889, 4000891, 4002405, 4012932, 4042673, 4042674, 4044369, 4046698, 4057804, 4094240, 4105593, 4137127, 4144921, 4145119, 4146733, 4148131, 4195852, 4197660, 4203779, 4223020, 4223626, 4225473, 4238716, 4249161, 4251776, 4275142, 4275564, 4284104, 42872715, 4312194, 4315396)
)

# P7
query_that_results_in_table_of_positive_indicators_of_procedural_ED_visits = generate_query_that_results_in_table_of_positive_indicators_for_procedure_and_undersample(
    name_of_column = "had_procedural_ED_visit",
    tuple_of_concept_IDs = (2514433, 2514434, 2514435, 2514436, 2514437)
)

# P8
query_that_results_in_table_of_positive_indicators_of_Head_Or_Neck_procedure = generate_query_that_results_in_table_of_positive_indicators_for_procedure_and_undersample(
    name_of_column = "had_Head_Or_Neck_procedure",
    tuple_of_concept_IDs = (4003728, 4027414, 4040721, 4043175, 4119836, 4160266, 4161058, 4233388)
)

query_that_results_in_feature_matrix = """
SELECT
    table_of_visit_occurrences.person_id,
    table_of_visit_occurrences.visit_occurrence_id,
    visit_start_datetime,
    has_Opioid_abuse,
    is_exposed_to_Opioids
FROM (""" + query_that_results_in_table_of_visit_occurrences_for_undersample + """) table_of_visit_occurrences
LEFT JOIN (""" + query_that_results_in_table_of_positive_indicators_of_Opioid_abuse + """) table_of_positive_indicators_of_Opioid_abuse
ON table_of_visit_occurrences.visit_occurrence_id = table_of_positive_indicators_of_Opioid_abuse.visit_occurrence_id
LEFT JOIN (""" + query_that_results_in_table_of_positive_indicators_of_Opioids + """) table_of_positive_indicators_of_Opioids
ON table_of_visit_occurrences.visit_occurrence_id = table_of_positive_indicators_of_Opioids.visit_occurrence_id
"""

def calculate_time_interval_between_now_and_start_time(start_time):
    now = time.time()
    time_interval_in_seconds = now - start_time
    time_interval_in_whole_minutes = math.floor(time_interval_in_seconds / 60)
    time_interval_during_this_minute = time_interval_in_seconds - time_interval_in_whole_minutes * 60
    return '%d %d' % (time_interval_in_whole_minutes, time_interval_during_this_minute)


if __name__ == "__main__":

    print("Feature matrix")
    feature_matrix = get_data_frame(query_that_results_in_feature_matrix)

    print("Tables of positive indicators of condition")
    print("Table of positive indicators of Anxiety")
    table_of_positive_indicators_of_Anxiety = get_data_frame(query_that_results_in_table_of_positive_indicators_of_Anxiety)
    print("Table of positive indicators of Bipolar disorder")
    table_of_positive_indicators_of_Bipolar_disorder = get_data_frame(query_that_results_in_table_of_positive_indicators_of_Bipolar_disorder)
    print("Table of positive indicators of Depression")
    table_of_positive_indicators_of_Depression = get_data_frame(query_that_results_in_table_of_positive_indicators_of_Depression)
    print("Table of positive indicators of Hypertension")
    table_of_positive_indicators_of_Hypertension = get_data_frame(query_that_results_in_table_of_positive_indicators_of_Hypertension)
    print("Table of positive indicators of Opioid dependence")
    table_of_positive_indicators_of_Opioid_dependence = get_data_frame(query_that_results_in_table_of_positive_indicators_of_Opioid_dependence)
    print("Table of positive indicators of Pain")
    table_of_positive_indicators_of_Pain = get_data_frame(query_that_results_in_table_of_positive_indicators_of_Pain)
    print("Table of positive indicators of Rhinitis")
    table_of_positive_indicators_of_Rhinitis = get_data_frame(query_that_results_in_table_of_positive_indicators_of_Rhinitis)
    print("Table of positive indicators of Non-Opioid Substance abuse")
    table_of_positive_indicators_of_Non_Opioid_Substance_Abuse = get_data_frame(query_that_results_in_table_of_positive_indicators_of_Non_Opioid_Substance_Abuse)

    print("Tables of positive indicators of drug")
    print("Table of positive indicators of ibuprofen")
    table_of_positive_indicators_of_ibuprofen = get_data_frame(query_that_results_in_table_of_positive_indicators_of_ibuprofen)
    print("Table of positive indicators of buprenorphine")
    table_of_positive_indicators_of_buprenorphine = get_data_frame(query_that_results_in_table_of_positive_indicators_of_buprenorphine)
    print("Table of positive indicators of fentanyl")
    table_of_positive_indicators_of_fentanyl = get_data_frame(query_that_results_in_table_of_positive_indicators_of_fentanyl)
    print("Table of positive indicators of morphine")
    table_of_positive_indicators_of_morphine = get_data_frame(query_that_results_in_table_of_positive_indicators_of_morphine)
    print("Table of positive indicators of oxycodone")
    table_of_positive_indicators_of_oxycodone = get_data_frame(query_that_results_in_table_of_positive_indicators_of_oxycodone)
    print("Table of positive indicators of hydromorphone")
    table_of_positive_indicators_of_hydromorphone = get_data_frame(query_that_results_in_table_of_positive_indicators_of_hydromorphone)
    print("Table of positive indicators of aspirin")
    table_of_positive_indicators_of_aspirin = get_data_frame(query_that_results_in_table_of_positive_indicators_of_aspirin)
    print("Table of positive indicators of codeine")
    table_of_positive_indicators_of_codeine = get_data_frame(query_that_results_in_table_of_positive_indicators_of_codeine)
    print("Table of positive indicators of tramadol")
    table_of_positive_indicators_of_tramadol = get_data_frame(query_that_results_in_table_of_positive_indicators_of_tramadol)
    print("Table of positive indicators of nalbuphine")
    table_of_positive_indicators_of_nalbuphine = get_data_frame(query_that_results_in_table_of_positive_indicators_of_nalbuphine)
    print("Table of positive indicators of meperidine")
    table_of_positive_indicators_of_mepiridine = get_data_frame(query_that_results_in_table_of_positive_indicators_of_meperidine)
    print("Table of positive indicators of naltrexone")
    table_of_positive_indicators_of_naltrexone = get_data_frame(query_that_results_in_table_of_positive_indicators_of_naltrexone)
    print("Table of positive indicators of acetaminophen")
    table_of_positive_indicators_of_acetaminophen = get_data_frame(query_that_results_in_table_of_positive_indicators_of_acetaminophen)

    print("Table of positive indicators of procedure")
    print("Table of positive indicators of Mammography")
    table_of_positive_indicators_of_mammography = get_data_frame(query_that_results_in_table_of_positive_indicators_of_Mammography)
    print("Table of positive indicators of Knee procedure")
    table_of_positive_indicators_of_knee_procedure = get_data_frame(query_that_results_in_table_of_positive_indicators_of_Knee_procedure)
    print("Table of positive indicators of Tooth procedure")
    table_of_positive_indicators_of_tooth_procedure = get_data_frame(query_that_results_in_table_of_positive_indicators_of_Tooth_procedure)
    print("Table of positive indicators of Hip procedure")
    table_of_positive_indicators_of_hip_procedure = get_data_frame(query_that_results_in_table_of_positive_indicators_of_Hip_procedure)
    print("Table of positive indicators of Vascular procedure")
    table_of_positive_indicators_of_vascular_procedure = get_data_frame(query_that_results_in_table_of_positive_indicators_of_Vascular_procedure)
    print("Table of positive indicators of Brain procedure")
    table_of_positive_indicators_of_brain_procedure = get_data_frame(query_that_results_in_table_of_positive_indicators_of_Brain_procedure)
    print("Table of positive indicators of Heart procedure")
    table_of_positive_indicators_of_heart_procedure = get_data_frame(query_that_results_in_table_of_positive_indicators_of_Heart_procedure)
    print("Table of positive indicators of procedural ED visit")
    table_of_positive_indicators_of_ED_visits = get_data_frame(query_that_results_in_table_of_positive_indicators_of_procedural_ED_visits)
    print("Table of positive indicators of Head Or Neck procedure")
    table_of_positive_indicators_of_head_or_neck_procedure = get_data_frame(query_that_results_in_table_of_positive_indicators_of_Head_Or_Neck_procedure)

    feature_matrix = pd.merge(feature_matrix, table_of_positive_indicators_of_Anxiety, how = 'left')
    feature_matrix = pd.merge(feature_matrix, table_of_positive_indicators_of_Bipolar_disorder, how = 'left')
    feature_matrix = pd.merge(feature_matrix, table_of_positive_indicators_of_Depression, how = 'left')
    feature_matrix = pd.merge(feature_matrix, table_of_positive_indicators_of_Hypertension, how = 'left')
    feature_matrix = pd.merge(feature_matrix, table_of_positive_indicators_of_Opioid_dependence, how = 'left')
    feature_matrix = pd.merge(feature_matrix, table_of_positive_indicators_of_Pain, how = 'left')
    feature_matrix = pd.merge(feature_matrix, table_of_positive_indicators_of_Rhinitis, how = 'left')
    feature_matrix = pd.merge(feature_matrix, table_of_positive_indicators_of_Non_Opioid_Substance_Abuse, how = 'left')

    feature_matrix = pd.merge(feature_matrix, table_of_positive_indicators_of_ibuprofen, how = 'left')
    feature_matrix = pd.merge(feature_matrix, table_of_positive_indicators_of_buprenorphine, how = 'left')
    feature_matrix = pd.merge(feature_matrix, table_of_positive_indicators_of_fentanyl, how = 'left')
    feature_matrix = pd.merge(feature_matrix, table_of_positive_indicators_of_morphine, how = 'left')
    feature_matrix = pd.merge(feature_matrix, table_of_positive_indicators_of_oxycodone, how = 'left')
    feature_matrix = pd.merge(feature_matrix, table_of_positive_indicators_of_hydromorphone, how = 'left')
    feature_matrix = pd.merge(feature_matrix, table_of_positive_indicators_of_aspirin, how = 'left')
    feature_matrix = pd.merge(feature_matrix, table_of_positive_indicators_of_codeine, how = 'left')
    feature_matrix = pd.merge(feature_matrix, table_of_positive_indicators_of_tramadol, how = 'left')
    feature_matrix = pd.merge(feature_matrix, table_of_positive_indicators_of_nalbuphine, how = 'left')
    feature_matrix = pd.merge(feature_matrix, table_of_positive_indicators_of_mepiridine, how = 'left')
    feature_matrix = pd.merge(feature_matrix, table_of_positive_indicators_of_naltrexone, how = 'left')
    feature_matrix = pd.merge(feature_matrix, table_of_positive_indicators_of_acetaminophen, how = 'left')

    feature_matrix = pd.merge(feature_matrix, table_of_positive_indicators_of_mammography, how = 'left')
    feature_matrix = pd.merge(feature_matrix, table_of_positive_indicators_of_knee_procedure, how = 'left')
    feature_matrix = pd.merge(feature_matrix, table_of_positive_indicators_of_tooth_procedure, how = 'left')
    feature_matrix = pd.merge(feature_matrix, table_of_positive_indicators_of_hip_procedure, how = 'left')
    feature_matrix = pd.merge(feature_matrix, table_of_positive_indicators_of_vascular_procedure, how = 'left')
    feature_matrix = pd.merge(feature_matrix, table_of_positive_indicators_of_brain_procedure, how = 'left')
    feature_matrix = pd.merge(feature_matrix, table_of_positive_indicators_of_heart_procedure, how = 'left')
    feature_matrix = pd.merge(feature_matrix, table_of_positive_indicators_of_ED_visits, how = 'left')
    feature_matrix = pd.merge(feature_matrix, table_of_positive_indicators_of_head_or_neck_procedure, how = 'left')

    feature_matrix = feature_matrix.drop(columns = ["visit_occurrence_id"])

    print(feature_matrix)
    number_of_distinct_patient_IDs_in_feature_matrix = len(pd.unique(feature_matrix["person_id"]))
    print("Number of distinct patient IDs in feature matrix: " + str(number_of_distinct_patient_IDs_in_feature_matrix))
    print("Columns of feature matrix:")
    print(feature_matrix.columns.tolist())

    feature_matrix.to_csv("Feature_Matrix.csv")
