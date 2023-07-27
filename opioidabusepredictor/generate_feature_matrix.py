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

query_that_results_in_table_of_visit_occurrences_for_cohort = """
        SELECT person_ID, visit_occurrence_id, visit_start_datetime
        FROM `""" + os.environ["WORKSPACE_CDR"] + """.visit_occurrence` visit_occurrence 
        WHERE (
            visit_occurrence.PERSON_ID IN (
                SELECT distinct person_id  
                FROM `""" + os.environ["WORKSPACE_CDR"] + """.cb_search_person` cb_search_person  
                WHERE cb_search_person.person_id IN (
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
            )
        )
"""

query_that_results_in_table_of_positive_indicators_of_Opioid_abuse = """
    SELECT
        c_occurrence.visit_occurrence_id,
        1 AS has_Opioid_abuse
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
                        concept_id IN (37016268, 4099935, 438130) 
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
        AND (
            c_occurrence.PERSON_ID IN (
                SELECT distinct person_id  
                FROM `""" + os.environ["WORKSPACE_CDR"] + """.cb_search_person` cb_search_person  
                WHERE cb_search_person.person_id IN (
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
            )
        )
    ) c_occurrence 
    LEFT JOIN `""" + os.environ["WORKSPACE_CDR"] + """.concept` c_standard_concept 
    ON c_occurrence.condition_concept_id = c_standard_concept.concept_id 
    LEFT JOIN `""" + os.environ["WORKSPACE_CDR"] + """.concept` c_type 
    ON c_occurrence.condition_type_concept_id = c_type.concept_id 
    LEFT JOIN `""" + os.environ["WORKSPACE_CDR"] + """.visit_occurrence` v 
    ON c_occurrence.visit_occurrence_id = v.visit_occurrence_id 
    LEFT JOIN `""" + os.environ["WORKSPACE_CDR"] + """.concept` visit 
    ON v.visit_concept_id = visit.concept_id 
    LEFT JOIN `""" + os.environ["WORKSPACE_CDR"] + """.concept` c_source_concept 
    ON c_occurrence.condition_source_concept_id = c_source_concept.concept_id 
    LEFT JOIN `""" + os.environ["WORKSPACE_CDR"] + """.concept` c_status 
    ON c_occurrence.condition_status_concept_id = c_status.concept_id"""

query_that_results_in_table_of_positive_indicators_of_Opioids = """
    SELECT
        d_exposure.visit_occurrence_id,
        1 AS is_exposed_to_Opioids
    FROM
        ( SELECT
            * 
        FROM
            `""" + os.environ["WORKSPACE_CDR"] + """.drug_exposure` d_exposure 
        WHERE
            (
                drug_concept_id IN  (
                    SELECT
                        DISTINCT ca.descendant_id 
                    FROM
                        `""" + os.environ["WORKSPACE_CDR"] + """.cb_criteria_ancestor` ca 
                    JOIN
                        (
                            select
                                distinct c.concept_id 
                            FROM
                                `""" + os.environ["WORKSPACE_CDR"] + """.cb_criteria` c 
                            JOIN
                                (
                                    select
                                        cast(cr.id as string) as id 
                                    FROM
                                        `""" + os.environ["WORKSPACE_CDR"] + """.cb_criteria` cr 
                                    WHERE
                                        concept_id IN (
                                            21604254
                                        ) 
                                        AND full_text LIKE '%_rank1]%'
                                ) a 
                                    ON (
                                        c.path LIKE CONCAT('%.',
                                    a.id,
                                    '.%') 
                                    OR c.path LIKE CONCAT('%.',
                                    a.id) 
                                    OR c.path LIKE CONCAT(a.id,
                                    '.%') 
                                    OR c.path = a.id) 
                                WHERE
                                    is_standard = 1 
                                    AND is_selectable = 1
                                ) b 
                                    ON (
                                        ca.ancestor_id = b.concept_id
                                    )
                            )
                        )  
                        AND (
                            d_exposure.PERSON_ID IN (
                                SELECT
                                    distinct person_id  
                            FROM
                                `""" + os.environ["WORKSPACE_CDR"] + """.cb_search_person` cb_search_person  
                            WHERE
                                cb_search_person.person_id IN (
                                    SELECT
                                        criteria.person_id 
                                    FROM
                                        (SELECT
                                            DISTINCT person_id,
                                            entry_date,
                                            concept_id 
                                        FROM
                                            `""" + os.environ["WORKSPACE_CDR"] + """.cb_search_all_events` 
                                        WHERE
                                            (
                                                concept_id IN (
                                                    SELECT
                                                        DISTINCT ca.descendant_id 
                                                    FROM
                                                        `""" + os.environ["WORKSPACE_CDR"] + """.cb_criteria_ancestor` ca 
                                                    JOIN
                                                        (
                                                            select
                                                                distinct c.concept_id 
                                                            FROM
                                                                `""" + os.environ["WORKSPACE_CDR"] + """.cb_criteria` c 
                                                            JOIN
                                                                (
                                                                    select
                                                                        cast(cr.id as string) as id 
                                                                    FROM
                                                                        `""" + os.environ["WORKSPACE_CDR"] + """.cb_criteria` cr 
                                                                    WHERE
                                                                        concept_id IN (21604254) 
                                                                        AND full_text LIKE '%_rank1]%'
                                                                ) a 
                                                                    ON (
                                                                        c.path LIKE CONCAT('%.',
                                                                    a.id,
                                                                    '.%') 
                                                                    OR c.path LIKE CONCAT('%.',
                                                                    a.id) 
                                                                    OR c.path LIKE CONCAT(a.id,
                                                                    '.%') 
                                                                    OR c.path = a.id) 
                                                                WHERE
                                                                    is_standard = 1 
                                                                    AND is_selectable = 1
                                                                ) b 
                                                                    ON (
                                                                        ca.ancestor_id = b.concept_id
                                                                    )
                                                            ) 
                                                            AND is_standard = 1
                                                        )
                                                ) criteria 
                                            ) 
                                            AND cb_search_person.person_id NOT IN (
                                                SELECT
                                                    criteria.person_id 
                                            FROM
                                                (SELECT
                                                    DISTINCT person_id,
                                                    entry_date,
                                                    concept_id 
                                                FROM
                                                    `""" + os.environ["WORKSPACE_CDR"] + """.cb_search_all_events` 
                                                WHERE
                                                    (
                                                        concept_id IN (
                                                            SELECT
                                                                DISTINCT c.concept_id 
                                                            FROM
                                                                `""" + os.environ["WORKSPACE_CDR"] + """.cb_criteria` c 
                                                            JOIN
                                                                (
                                                                    select
                                                                        cast(cr.id as string) as id 
                                                                    FROM
                                                                        `""" + os.environ["WORKSPACE_CDR"] + """.cb_criteria` cr 
                                                                    WHERE
                                                                        concept_id IN (40493428, 252840, 4092524, 4178964, 76349, 4177243, 4313083, 4241905, 46273652, 196925, 4180791, 436045, 200962, 4091466, 4162860, 4294416, 4092212, 444411, 4154630, 442139, 4309225, 4298032, 40650479, 197807, 4091465, 40490994, 4216273, 133424, 4247726, 4247822, 4181483, 4162994, 4309851, 436671, 4180780, 4300555, 4160780, 4180910, 4247238, 4155297, 4178971, 443568, 75210, 442183, 40492021, 78987, 443388, 195483, 4155285, 4181488, 25189, 4180902, 36676291, 376918, 4297666, 4157332, 40488919, 380661, 256633, 133711, 200348, 4174593, 433143, 4178977, 4178959, 443387, 40480128, 432263, 4312698, 4315805, 4112853, 4297185, 4181333, 442181, 46270738, 4178974, 4246450, 442131, 443391, 444410, 137809, 4181330, 4313916, 40487047, 192581, 4314047, 4092217, 4181351, 37397344, 4112745, 257503, 438693, 139750, 4181338, 198091, 436357, 196359, 432257, 4246451, 258375, 200052, 40488964, 256646, 436353, 438094, 4312023, 31509, 201238, 37310458, 4095312, 138996, 4162253, 378087, 435484, 374874, 36684472, 4314071, 4312288, 261514, 4181332, 44806773, 196360, 4188544, 4298026, 443392, 4295624, 432845, 45769720, 197507, 4233629, 4241917, 199754, 4160276, 440649, 443588, 4089655, 4092511, 439751, 432837, 4095740, 197808, 4162251, 200051, 4310566, 4187850, 26052, 4187849, 4311637, 4246017, 440658, 4247719, 4312685, 4241904, 4181480, 4311342, 4116084, 4111921, 36683531, 441805, 4298028, 437498, 4054503, 4298033, 192568, 4246127, 4312022, 4081044, 197500, 373425, 4338758, 443398, 376647, 4308621, 198988, 40491001, 40492037, 40490993, 4180793, 4181484, 4247338, 4297665, 441515, 74582, 442132, 261236, 4188545, 4180790, 4247331, 133726, 433716, 258369, 136917, 440345, 200959, 320342, 434298, 4334322, 4181350, 40492932, 4178968, 198700, 380055, 440956, 4111776, 4311619, 4247836, 4155171, 432833, 4111805, 433435, 201519, 438386, 4178976, 192855, 4157457, 75512, 4311617, 4157456, 24897, 45770892, 4187848, 80045, 4116241, 45768522, 4311499, 72266, 255507, 193144, 30346, 4315806, 443561, 28083, 4001666, 4089665, 4089756, 439404, 435755, 42536893, 4312944, 443390, 40493020, 4187851, 441233, 4248061, 4247357, 4157454, 4313482, 438699, 318096, 4112974, 432254, 4158563, 4091464, 436635, 198985, 40482750, 254591, 4181343, 197804, 4151250, 40487143, 443386, 46270513, 4157449, 4181354, 4181339, 133147, 443389, 4092382, 45757101, 4313634, 4307263, 4092512, 194589, 253717, 136354, 764225, 195513, 4153882, 4157331, 4214901, 4181477) 
                                                                        AND full_text LIKE '%_rank1]%'
                                                                ) a 
                                                                    ON (
                                                                        c.path LIKE CONCAT('%.',
                                                                    a.id,
                                                                    '.%') 
                                                                    OR c.path LIKE CONCAT('%.',
                                                                    a.id) 
                                                                    OR c.path LIKE CONCAT(a.id,
                                                                    '.%') 
                                                                    OR c.path = a.id) 
                                                                WHERE
                                                                    is_standard = 1 
                                                                    AND is_selectable = 1
                                                                ) 
                                                                AND is_standard = 1 
                                                        )
                                                    ) criteria 
                                                ) ))
                                    ) d_exposure 
                                LEFT JOIN
                                    `""" + os.environ["WORKSPACE_CDR"] + """.concept` d_standard_concept 
                                        ON d_exposure.drug_concept_id = d_standard_concept.concept_id 
                                LEFT JOIN
                                    `""" + os.environ["WORKSPACE_CDR"] + """.concept` d_type 
                                        ON d_exposure.drug_type_concept_id = d_type.concept_id 
                                LEFT JOIN
                                    `""" + os.environ["WORKSPACE_CDR"] + """.concept` d_route 
                                        ON d_exposure.route_concept_id = d_route.concept_id 
                                LEFT JOIN
                                    `""" + os.environ["WORKSPACE_CDR"] + """.visit_occurrence` v 
                                        ON d_exposure.visit_occurrence_id = v.visit_occurrence_id 
                                LEFT JOIN
                                    `""" + os.environ["WORKSPACE_CDR"] + """.concept` d_visit 
                                        ON v.visit_concept_id = d_visit.concept_id 
                                LEFT JOIN
                                    `""" + os.environ["WORKSPACE_CDR"] + """.concept` d_source_concept 
                                        ON d_exposure.drug_source_concept_id = d_source_concept.concept_id"""

query_that_results_in_feature_matrix = """
SELECT
    person_id,
    table_of_visit_occurrences.visit_occurrence_id,
    visit_start_datetime,
    has_Opioid_abuse,
    is_exposed_to_Opioids
FROM (""" + query_that_results_in_table_of_visit_occurrences_for_cohort + """) table_of_visit_occurrences
LEFT JOIN (""" + query_that_results_in_table_of_positive_indicators_of_Opioid_abuse + """) table_of_positive_indicators_of_Opioid_abuse
ON table_of_visit_occurrences.visit_occurrence_id = table_of_positive_indicators_of_Opioid_abuse.visit_occurrence_id
LEFT JOIN (""" + query_that_results_in_table_of_positive_indicators_of_Opioids + """) table_of_positive_indicators_of_Opioids
ON table_of_visit_occurrences.visit_occurrence_id = table_of_positive_indicators_of_Opioids.visit_occurrence_id
"""

if __name__ == "__main__":
    feature_matrix = get_data_frame(query_that_results_in_feature_matrix)
    print(feature_matrix)
