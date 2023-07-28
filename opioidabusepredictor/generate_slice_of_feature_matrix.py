import numpy as np
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

# A0
query_that_results_in_table_of_positive_indicators_of_Opioid_abuse = """
    SELECT
        c_occurrence.visit_occurrence_id,
        1 AS has_Opioid_abuse
    FROM
        ( SELECT
            * 
        FROM
            `""" + os.environ["WORKSPACE_CDR"] + """.condition_occurrence` c_occurrence 
        WHERE
            (
                condition_concept_id IN  (
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
                                concept_id IN (
                                    37016268, 4099935, 438130
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
                        )
                )  
                AND (
                    c_occurrence.PERSON_ID IN (
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
                                ) c_occurrence 
                            LEFT JOIN
                                `""" + os.environ["WORKSPACE_CDR"] + """.concept` c_standard_concept 
                                    ON c_occurrence.condition_concept_id = c_standard_concept.concept_id 
                            LEFT JOIN
                                `""" + os.environ["WORKSPACE_CDR"] + """.concept` c_type 
                                    ON c_occurrence.condition_type_concept_id = c_type.concept_id 
                            LEFT JOIN
                                `""" + os.environ["WORKSPACE_CDR"] + """.visit_occurrence` v 
                                    ON c_occurrence.visit_occurrence_id = v.visit_occurrence_id 
                            LEFT JOIN
                                `""" + os.environ["WORKSPACE_CDR"] + """.concept` visit 
                                    ON v.visit_concept_id = visit.concept_id 
                            LEFT JOIN
                                `""" + os.environ["WORKSPACE_CDR"] + """.concept` c_source_concept 
                                    ON c_occurrence.condition_source_concept_id = c_source_concept.concept_id 
                            LEFT JOIN
                                `""" + os.environ["WORKSPACE_CDR"] + """.concept` c_status 
                                    ON c_occurrence.condition_status_concept_id = c_status.concept_id"""

# A1
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
                                            1123896, 21600593, 21604200, 21604254, 21604291, 21604296, 21604825
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

# C0
query_that_results_in_table_of_positive_indicators_of_Anxiety = """
    SELECT
        c_occurrence.visit_occurrence_id,
        1 AS has_Anxiety
    FROM
        ( SELECT
            * 
        FROM
            `""" + os.environ["WORKSPACE_CDR"] + """.condition_occurrence` c_occurrence 
        WHERE
            (
                condition_concept_id IN  (
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
                                concept_id IN (
                                    35615152, 35615153, 35615155, 36684319, 37109206, 37117155, 381537, 4009184, 4021501, 4035299, 4039212, 4056690, 4084699, 4085058, 4087190, 4098314, 4113821, 4115221, 4117364, 4146660, 4178114, 4193634, 4198826, 4199892, 4203449, 4214746, 4216670, 4221077, 4242095, 42538592, 42538968, 4261239, 4263429, 4288011, 4304010, 4322025, 4328276, 433178, 4338031, 434613, 434628, 436074, 436075, 440690, 441542, 442077, 44784526, 763092
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
                        )
                )  
                AND (
                    c_occurrence.PERSON_ID IN (
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
                                ) c_occurrence 
                            LEFT JOIN
                                `""" + os.environ["WORKSPACE_CDR"] + """.concept` c_standard_concept 
                                    ON c_occurrence.condition_concept_id = c_standard_concept.concept_id 
                            LEFT JOIN
                                `""" + os.environ["WORKSPACE_CDR"] + """.concept` c_type 
                                    ON c_occurrence.condition_type_concept_id = c_type.concept_id 
                            LEFT JOIN
                                `""" + os.environ["WORKSPACE_CDR"] + """.visit_occurrence` v 
                                    ON c_occurrence.visit_occurrence_id = v.visit_occurrence_id 
                            LEFT JOIN
                                `""" + os.environ["WORKSPACE_CDR"] + """.concept` visit 
                                    ON v.visit_concept_id = visit.concept_id 
                            LEFT JOIN
                                `""" + os.environ["WORKSPACE_CDR"] + """.concept` c_source_concept 
                                    ON c_occurrence.condition_source_concept_id = c_source_concept.concept_id 
                            LEFT JOIN
                                `""" + os.environ["WORKSPACE_CDR"] + """.concept` c_status 
                                    ON c_occurrence.condition_status_concept_id = c_status.concept_id"""

# C1
query_that_results_in_table_of_positive_indicators_of_Bipolar_disorder = """
    SELECT
        c_occurrence.visit_occurrence_id,
        1 AS has_Bipolar_disorder
    FROM
        ( SELECT
            * 
        FROM
            `""" + os.environ["WORKSPACE_CDR"] + """.condition_occurrence` c_occurrence 
        WHERE
            (
                condition_concept_id IN  (
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
                                concept_id IN (
                                    35622934, 35624743, 35624744, 35624745, 35624747, 35624748, 37109940, 37117177, 372599, 37312578, 4001733, 4009648, 4028027, 4030856, 4037669, 4071442, 4102603, 4144519, 4148842, 4148934, 4150985, 4154283, 4155798, 4161200, 4166701, 4172156, 4177651, 4185096, 4194222, 4195158, 4200385, 4201739, 4215917, 4220617, 4220618, 4244078, 4262111, 4280361, 42872412, 42872413, 4287544, 43020451, 4307804, 4307956, 4310821, 432290, 4324945, 4327669, 432866, 432876, 433743, 433992, 435225, 435226, 436072, 436086, 436386, 436665, 437250, 437528, 437529, 439001, 439245, 439246, 439248, 439249, 439250, 439251, 439253, 439254, 439255, 439256, 439785, 440067, 440078, 440079, 441834, 441836, 442570, 442600, 443797, 443906
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
                        )
                )  
                AND (
                    c_occurrence.PERSON_ID IN (
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
                                ) c_occurrence 
                            LEFT JOIN
                                `""" + os.environ["WORKSPACE_CDR"] + """.concept` c_standard_concept 
                                    ON c_occurrence.condition_concept_id = c_standard_concept.concept_id 
                            LEFT JOIN
                                `""" + os.environ["WORKSPACE_CDR"] + """.concept` c_type 
                                    ON c_occurrence.condition_type_concept_id = c_type.concept_id 
                            LEFT JOIN
                                `""" + os.environ["WORKSPACE_CDR"] + """.visit_occurrence` v 
                                    ON c_occurrence.visit_occurrence_id = v.visit_occurrence_id 
                            LEFT JOIN
                                `""" + os.environ["WORKSPACE_CDR"] + """.concept` visit 
                                    ON v.visit_concept_id = visit.concept_id 
                            LEFT JOIN
                                `""" + os.environ["WORKSPACE_CDR"] + """.concept` c_source_concept 
                                    ON c_occurrence.condition_source_concept_id = c_source_concept.concept_id 
                            LEFT JOIN
                                `""" + os.environ["WORKSPACE_CDR"] + """.concept` c_status 
                                    ON c_occurrence.condition_status_concept_id = c_status.concept_id"""

# C2
query_that_results_in_table_of_positive_indicators_of_Depression = """
    SELECT
        c_occurrence.visit_occurrence_id,
        1 AS has_Depression
    FROM
        ( SELECT
            * 
        FROM
            `""" + os.environ["WORKSPACE_CDR"] + """.condition_occurrence` c_occurrence 
        WHERE
            (
                condition_concept_id IN  (
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
                                concept_id IN (
                                    35615152, 35615153, 35615155, 35622934, 35624743, 35624748, 3656234, 36713698, 36714389, 36714998, 36715000, 36717092, 37016718, 37018656, 37110429, 37111697, 377527, 379784, 4001733, 4025677, 4031328, 4037669, 40481798, 4049623, 4077577, 4094358, 4095285, 4098302, 4101137, 4103126, 4103574, 4114950, 4124706, 4129184, 4141292, 4141454, 4144233, 4144519, 4148630, 4148934, 4149320, 4149321, 4151170, 4152280, 4154309, 4154391, 4161569, 4174987, 4176002, 4185096, 4191716, 4195572, 4205471, 4214898, 4223090, 4224940, 4228802, 4239471, 4250023, 4262111, 4263748, 4269493, 4282096, 4282316, 42872411, 42872722, 4298317, 43021839, 4304140, 4307111, 4314692, 432285, 4323418, 4324945, 4324959, 4327337, 4328217, 432883, 433440, 4336957, 433751, 4338031, 433991, 434911, 435220, 43531624, 435520, 438406, 438727, 438998, 439254, 439259, 440078, 440383, 440698, 441534, 443864, 44782943, 762503, 762504
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
                        )
                )  
                AND (
                    c_occurrence.PERSON_ID IN (
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
                                ) c_occurrence 
                            LEFT JOIN
                                `""" + os.environ["WORKSPACE_CDR"] + """.concept` c_standard_concept 
                                    ON c_occurrence.condition_concept_id = c_standard_concept.concept_id 
                            LEFT JOIN
                                `""" + os.environ["WORKSPACE_CDR"] + """.concept` c_type 
                                    ON c_occurrence.condition_type_concept_id = c_type.concept_id 
                            LEFT JOIN
                                `""" + os.environ["WORKSPACE_CDR"] + """.visit_occurrence` v 
                                    ON c_occurrence.visit_occurrence_id = v.visit_occurrence_id 
                            LEFT JOIN
                                `""" + os.environ["WORKSPACE_CDR"] + """.concept` visit 
                                    ON v.visit_concept_id = visit.concept_id 
                            LEFT JOIN
                                `""" + os.environ["WORKSPACE_CDR"] + """.concept` c_source_concept 
                                    ON c_occurrence.condition_source_concept_id = c_source_concept.concept_id 
                            LEFT JOIN
                                `""" + os.environ["WORKSPACE_CDR"] + """.concept` c_status 
                                    ON c_occurrence.condition_status_concept_id = c_status.concept_id"""

# C3
query_that_results_in_table_of_positive_indicators_of_Hypertension = """
    SELECT
        c_occurrence.visit_occurrence_id,
        1 AS has_Hypertension
    FROM
        ( SELECT
            * 
        FROM
            `""" + os.environ["WORKSPACE_CDR"] + """.condition_occurrence` c_occurrence 
        WHERE
            (
                condition_concept_id IN  (
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
                                concept_id IN (
                                    193493, 195556, 201313, 312938, 313502, 314369, 314378, 316866, 316994, 319034, 37208172, 376965, 377551, 4013643, 40481896, 4057978, 4062552, 4116347, 4173820, 4183981, 4207534, 4263504, 42709887, 4298200, 43020424, 43020455, 43020457, 43021835, 43021852, 4322893, 4342636, 439694, 439695, 439696, 439698, 442603, 442604, 442626, 442766, 443919, 444101, 44782728, 44784439, 44784621, 44784638, 44784639, 45757137, 45757139, 45757140, 45757356, 45768449
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
                        )
                )  
                AND (
                    c_occurrence.PERSON_ID IN (
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
                                ) c_occurrence"""

# C4
query_that_results_in_table_of_positive_indicators_of_Opioid_dependence = """
    SELECT
        c_occurrence.visit_occurrence_id,
        1 AS has_Opioid_dependence
    FROM
        ( SELECT
            * 
        FROM
            `""" + os.environ["WORKSPACE_CDR"] + """.condition_occurrence` c_occurrence 
        WHERE
            (
                condition_concept_id IN  (
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
                                concept_id IN (
                                    37018689, 37209507, 4099809, 4100520, 4102817, 4103413, 42872387, 432301, 4336384, 438120, 440379, 440693
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
                        )
                )  
                AND (
                    c_occurrence.PERSON_ID IN (
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
                                ) c_occurrence 
                            LEFT JOIN
                                `""" + os.environ["WORKSPACE_CDR"] + """.concept` c_standard_concept 
                                    ON c_occurrence.condition_concept_id = c_standard_concept.concept_id 
                            LEFT JOIN
                                `""" + os.environ["WORKSPACE_CDR"] + """.concept` c_type 
                                    ON c_occurrence.condition_type_concept_id = c_type.concept_id 
                            LEFT JOIN
                                `""" + os.environ["WORKSPACE_CDR"] + """.visit_occurrence` v 
                                    ON c_occurrence.visit_occurrence_id = v.visit_occurrence_id 
                            LEFT JOIN
                                `""" + os.environ["WORKSPACE_CDR"] + """.concept` visit 
                                    ON v.visit_concept_id = visit.concept_id 
                            LEFT JOIN
                                `""" + os.environ["WORKSPACE_CDR"] + """.concept` c_source_concept 
                                    ON c_occurrence.condition_source_concept_id = c_source_concept.concept_id 
                            LEFT JOIN
                                `""" + os.environ["WORKSPACE_CDR"] + """.concept` c_status 
                                    ON c_occurrence.condition_status_concept_id = c_status.concept_id"""

# C5
query_that_results_in_table_of_positive_indicators_of_Pain = """
    SELECT
        c_occurrence.visit_occurrence_id,
        1 AS has_Pain
    FROM
        ( SELECT
            * 
        FROM
            `""" + os.environ["WORKSPACE_CDR"] + """.condition_occurrence` c_occurrence 
        WHERE
            (
                condition_concept_id IN  (
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
                                concept_id IN (
                                    134159, 134736, 137856, 138525, 138845, 192239, 193322, 194133, 194175, 194696, 195083, 197381, 197684, 197988, 198263, 200219, 201347, 201418, 201626, 24134, 259153, 312998, 315832, 35611566, 36674979, 36684444, 36684573, 36684575, 36686912, 36686913, 36712805, 36712807, 37017182, 37108940, 37108941, 37118025, 37204166, 37209603, 37209605, 373852, 375825, 378253, 379031, 380733, 4002014, 4002956, 4008102, 4009391, 4009890, 4010016, 4010017, 4010025, 4010361, 4012075, 4012198, 4012199, 4012222, 4012223, 4012234, 4012690, 4021670, 4024243, 4024561, 4046660, 40481180, 40481920, 40485490, 4048710, 4058670, 4071874, 4077895, 4079724, 4082798, 4083297, 4083769, 4083770, 4083779, 4090433, 4090553, 4090564, 4092930, 4099171, 4099176, 4103476, 4109083, 4109084, 4109085, 4111231, 4115169, 4115170, 4115171, 4115367, 4115368, 4115406, 4115408, 4115409, 4115410, 4115411, 4116166, 4116809, 4116810, 4116811, 4116987, 4116988, 4117695, 4127066, 4128083, 4129418, 4132891, 4132892, 4132926, 4132929, 4132931, 4133037, 4133039, 4133040, 4133638, 4133643, 4134577, 4137674, 4139512, 4144753, 4145372, 4147441, 4147829, 4149024, 4150062, 4150125, 4150129, 4150759, 4160062, 4160900, 4166666, 4167250, 4168213, 4168216, 4168686, 4169580, 4169905, 4170554, 4170962, 4176923, 4182327, 4182562, 4200298, 4201930, 4204199, 4208857, 4218101, 4218793, 4237198, 4237315, 4237595, 4241033, 4244072, 4253797, 42538688, 42539051, 42539474, 4256912, 4260916, 4263576, 4264107, 4264144, 4270932, 4279301, 4297894, 4302739, 4306292, 4308696, 4317968, 4322528, 4322871, 4329041, 4330445, 4331953, 4333227, 433456, 43530621, 43530622, 43530661, 43531612, 436096, 438867, 439080, 439502, 440704, 441334, 442287, 442555, 442752, 443464, 444391, 44782778, 44784631, 45757565, 45763561, 45768450, 45769207, 45771676, 45773181, 46273207, 73819, 75863, 759905, 759906, 759907, 759908, 759909, 759911, 759912, 760837, 760912, 760919, 761157, 761158, 761159, 761703, 761704, 762287, 762288, 762289, 762290, 762291, 762292, 762293, 762294, 762296, 762297, 762298, 762299, 762361, 762377, 762941, 76388, 76458, 765060, 765061, 765131, 765268, 765384, 765422, 765423, 765933, 77074, 77670, 78232, 78234, 78508, 78517
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
                        )
                )  
                AND (
                    c_occurrence.PERSON_ID IN (
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
                                ) c_occurrence"""

# C6
query_that_results_in_table_of_positive_indicators_of_Rhinitis = """
    SELECT
        c_occurrence.visit_occurrence_id,
        1 AS has_Rhinitis
    FROM
        ( SELECT
            * 
        FROM
            `""" + os.environ["WORKSPACE_CDR"] + """.condition_occurrence` c_occurrence 
        WHERE
            (
                condition_concept_id IN  (
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
                                concept_id IN (
                                    252936, 256439, 257007, 259848, 260427, 40481374, 4048171, 40486433, 4049223, 4051476, 4101701, 4110489, 4177553, 4270705, 42709857, 4280726, 42872416, 42873159, 4305500, 4309214, 4316066, 4320791, 4327870, 443558, 45757082, 45766684, 45766713, 46269743, 46269744, 46269789, 46270028, 46270030, 46270156, 46273452, 46273454
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
                        )
                )  
                AND (
                    c_occurrence.PERSON_ID IN (
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
                                ) c_occurrence"""

# C7
query_that_results_in_table_of_positive_indicators_of_Non_Opioid_Substance_Abuse = """
    SELECT
        c_occurrence.visit_occurrence_id,
        1 AS has_Non_Opioid_Substance_Abuse
    FROM
        ( SELECT
            * 
        FROM
            `""" + os.environ["WORKSPACE_CDR"] + """.condition_occurrence` c_occurrence 
        WHERE
            (
                condition_concept_id IN  (
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
                                concept_id IN (
                                    193256, 195300, 196463, 201343, 201612, 318773, 35623150, 35624505, 3654548, 3654785, 36676296, 36676297, 36713086, 36714559, 36715277, 36715922, 36716473, 37016173, 37016176, 37016267, 37017563, 37018356, 37108712, 37108954, 37109022, 37109023, 37110409, 37110429, 37110436, 37110437, 37110441, 37110444, 37110445, 37110468, 37110472, 37110474, 37110490, 37110493, 37116661, 37117049, 37119151, 37203948, 37209507, 372607, 37309681, 37309774, 37311993, 37312034, 37312450, 374317, 374623, 375504, 375519, 375794, 376383, 377830, 378421, 378726, 4002572, 4004672, 4009647, 4010023, 4012869, 4020146, 4022082, 4029464, 4035299, 4035966, 4041191, 4042889, 4042893, 4044237, 40479573, 40482898, 40483111, 40483172, 40483827, 40484946, 4052690, 4053640, 4056690, 4063372, 4078688, 4080762, 4088373, 4089921, 4091714, 4091715, 4092159, 4094638, 4094639, 4096598, 4097389, 4097390, 4099334, 4099809, 4099811, 4100520, 4100526, 4101256, 4102817, 4102821, 4103413, 4103418, 4103419, 4103424, 4103426, 4103853, 4104431, 4104707, 4109691, 4132097, 4136662, 4137236, 4137955, 4139421, 4141524, 4143110, 4143732, 4145220, 4146660, 4146716, 4146763, 4148093, 4148140, 4150794, 4152165, 4155336, 4157200, 4159804, 4159806, 4159810, 4160957, 4161894, 4164432, 4166129, 4168706, 4171175, 4171789, 4171795, 4173746, 4174619, 4174805, 4175635, 4176120, 4176286, 4176464, 4176983, 4178114, 4180736, 4181940, 4183441, 4184438, 4191592, 4192127, 4193868, 4197130, 4197434, 4198826, 4199244, 4202330, 4203152, 4203257, 4205002, 4206341, 4209423, 4214950, 4216493, 4217840, 4218081, 4218106, 4220072, 4220197, 4221077, 4224276, 4224791, 4228331, 4232492, 4233811, 4234597, 4236877, 4237906, 4239381, 4239812, 4245794, 4245840, 42536419, 42537692, 42538589, 42538592, 42539146, 42539355, 4262566, 4264766, 4264889, 4267413, 4272033, 4272313, 4275756, 4279309, 4287251, 4288013, 4290062, 4290538, 4300092, 43020446, 43020473, 43021844, 4302744, 4307098, 4308292, 4310679, 4313135, 4319165, 4319166, 4322698, 432302, 432303, 432304, 4323272, 4323639, 4324044, 432609, 4326515, 4327117, 432878, 432884, 4331287, 433180, 4332880, 4332991, 433452, 433458, 433473, 433735, 433745, 433746, 433753, 4338023, 4338026, 433935, 433994, 434015, 434016, 434019, 4340383, 4340385, 4340386, 4340493, 4340964, 434327, 434328, 434627, 434916, 434917, 434921, 435140, 435231, 435243, 43530680, 43530681, 435532, 435533, 435534, 435718, 435809, 436089, 436097, 436098, 436296, 436389, 436585, 436953, 436954, 437245, 437257, 437264, 437533, 437838, 438126, 438306, 438393, 438648, 438732, 439005, 439277, 439312, 439313, 439554, 439796, 440002, 440069, 440270, 440380, 440381, 440387, 440612, 440685, 440692, 440694, 440891, 440892, 440992, 440996, 441198, 441260, 441261, 441262, 441272, 441276, 441465, 441833, 442582, 442601, 442914, 443236, 443274, 443534, 443930, 444038, 444363, 44782445, 44782714, 44782987, 44783367, 44784619, 44784627, 45757093, 45757783, 45766641, 45766642, 45769462, 45773120, 46269816, 46269817, 46269818, 46269835, 46273635, 761844, 765451
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
                        )
                )  
                AND (
                    c_occurrence.PERSON_ID IN (
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
                                ) c_occurrence"""


#D0
query_that_results_in_positive_indicators_of_ibuprofen = """
    SELECT
        d_exposure.visit_occurrence_id,
        1 AS has_been_exposed_to_ibuprofen
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
                                            1177480
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

#D1
query_that_results_in_positive_indicators_of_buprenorphine = """
    SELECT
        d_exposure.visit_occurrence_id,
        1 AS has_been_exposed_to_buprenorphine
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
                                            1133201
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

#D2
query_that_results_in_positive_indicators_of_fentanyl = """
    SELECT
        d_exposure.visit_occurrence_id,
        1 AS has_been_exposed_to_fentanyl
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
                                            1154029
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

#D3
query_that_results_in_positive_indicators_of_morphine = """
    SELECT
        d_exposure.visit_occurrence_id,
        1 AS has_been_exposed_to_morphine
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
                                            1110410
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

#D4
query_that_results_in_positive_indicators_of_oxycodone= """
    SELECT
        d_exposure.visit_occurrence_id,
        1 AS has_been_exposed_to_oxycodone
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
                                            1124957
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

#D5
query_that_results_in_positive_indicators_of_hydromorphone = """
    SELECT
        d_exposure.visit_occurrence_id,
        1 AS has_been_exposed_to_hydromorphone
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
                                            1126658
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

#D6
query_that_results_in_positive_indicators_of_aspirin = """
    SELECT
        d_exposure.visit_occurrence_id,
        1 AS has_been_exposed_to_aspirin
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
                                            1112807
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

#D7
query_that_results_in_positive_indicators_of_codeine = """
    SELECT
        d_exposure.visit_occurrence_id,
        1 AS has_been_exposed_to_codeine
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
                                            1201620
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

#D8
query_that_results_in_positive_indicators_of_tramadol = """
    SELECT
        d_exposure.visit_occurrence_id,
        1 AS has_been_exposed_to_tramadol
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
                                            1103314
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

#D9
query_that_results_in_positive_indicators_of_nalbuphine = """
    SELECT
        d_exposure.visit_occurrence_id,
        1 AS has_been_exposed_to_nalbuphine
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
                                            1114122
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

#D10
query_that_results_in_positive_indicators_of_mepiridine = """
    SELECT
        d_exposure.visit_occurrence_id,
        1 AS has_been_exposed_to_mepiridine 
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
                                            1102527
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

#D11
query_that_results_in_positive_indicators_of_naltrexone = """
    SELECT
        d_exposure.visit_occurrence_id,
        1 AS has_been_exposed_to_naltrexone
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
                                            1714319
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

#D12
query_that_results_in_positive_indicators_of_acetaminophen = """
    SELECT
        d_exposure.visit_occurrence_id,
        1 AS has_been_exposed_to_acetaminophen
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
                                            1125315
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


query_that_results_in_condition_feature_matrix = """
SELECT
    MAX(person_id) as person_id,
    table_of_visit_occurrences.visit_occurrence_id,
    MAX(visit_start_datetime) as visit_start_datetime,
    MAX(has_Opioid_abuse) as has_Opioid_abuse,
    MAX(is_exposed_to_Opioids) as is_exposed_to_Opioids,
    MAX(has_Anxiety) as has_Anxiety,
    MAX(has_Bipolar_disorder) as has_Bipolar_disorder,
    MAX(has_Depression) as has_Depression,
    MAX(has_Hypertension) as has_Hypertension,
    MAX(has_Opioid_dependence) as has_Opioid_dependence,
    MAX(has_Pain) as has_Pain,
    MAX(has_Rhinitis) as has_Rhinitis,
    MAX(has_Non_Opioid_Substance_Abuse) as Non_Opioid_Substance_Abuse,
FROM (""" + query_that_results_in_table_of_visit_occurrences_for_cohort + """) table_of_visit_occurrences
LEFT JOIN (""" + query_that_results_in_table_of_positive_indicators_of_Opioid_abuse + """) table_of_positive_indicators_of_Opioid_abuse
ON table_of_visit_occurrences.visit_occurrence_id = table_of_positive_indicators_of_Opioid_abuse.visit_occurrence_id
LEFT JOIN (""" + query_that_results_in_table_of_positive_indicators_of_Opioids + """) table_of_positive_indicators_of_Opioids
ON table_of_visit_occurrences.visit_occurrence_id = table_of_positive_indicators_of_Opioids.visit_occurrence_id
LEFT JOIN (""" + query_that_results_in_table_of_positive_indicators_of_Anxiety + """) table_of_positive_indicators_of_Anxiety
ON table_of_visit_occurrences.visit_occurrence_id = table_of_positive_indicators_of_Anxiety.visit_occurrence_id
LEFT JOIN (""" + query_that_results_in_table_of_positive_indicators_of_Bipolar_disorder + """) table_of_positive_indicators_of_Bipolar_disorder
ON table_of_visit_occurrences.visit_occurrence_id = table_of_positive_indicators_of_Bipolar_disorder.visit_occurrence_id
LEFT JOIN (""" + query_that_results_in_table_of_positive_indicators_of_Depression + """) table_of_positive_indicators_of_Depression
ON table_of_visit_occurrences.visit_occurrence_id = table_of_positive_indicators_of_Depression.visit_occurrence_id
LEFT JOIN (""" + query_that_results_in_table_of_positive_indicators_of_Hypertension + """) table_of_positive_indicators_of_Hypertension
ON table_of_visit_occurrences.visit_occurrence_id = table_of_positive_indicators_of_Hypertension.visit_occurrence_id
LEFT JOIN (""" + query_that_results_in_table_of_positive_indicators_of_Opioid_dependence + """) table_of_positive_indicators_of_Opioid_dependence
ON table_of_visit_occurrences.visit_occurrence_id = table_of_positive_indicators_of_Opioid_dependence.visit_occurrence_id
LEFT JOIN (""" + query_that_results_in_table_of_positive_indicators_of_Pain + """) table_of_positive_indicators_of_Pain
ON table_of_visit_occurrences.visit_occurrence_id = table_of_positive_indicators_of_Pain.visit_occurrence_id
LEFT JOIN (""" + query_that_results_in_table_of_positive_indicators_of_Rhinitis + """) table_of_positive_indicators_of_Rhinitis
ON table_of_visit_occurrences.visit_occurrence_id = table_of_positive_indicators_of_Rhinitis.visit_occurrence_id
LEFT JOIN (""" + query_that_results_in_table_of_positive_indicators_of_Non_Opioid_Substance_Abuse + """) table_of_positive_indicators_of_Non_Opioid_Substance_Abuse
ON table_of_visit_occurrences.visit_occurrence_id = table_of_positive_indicators_of_Non_Opioid_Substance_Abuse.visit_occurrence_id
GROUP BY table_of_visit_occurrences.visit_occurrence_id
"""

query_that_results_in_drug_feature_matrix = """
SELECT 
    MAX(person_id) as person_id,
    table_of_visit_occurrences.visit_occurrence_id,
    MAX(visit_start_datetime) as visit_start_datetime,
    MAX(has_been_exposed_to_ibuprofen) as has_been_exposed_to_ibuprofen,
    MAX(has_been_exposed_to_buprenorphine) as has_been_exposed_to_buprenorphine,
    MAX(has_been_exposed_to_fentanyl) as has_been_exposed_to_fentanyl,
    MAX(has_been_exposed_to_morphine) as has_been_exposed_to_morphine,
    MAX(has_been_exposed_to_oxycodone) as has_been_exposed_to_oxycodone,
    MAX(has_been_exposed_to_hydromorphone) as has_been_exposed_to_hydromorphone,
    MAX(has_been_exposed_to_aspirin) as has_been_exposed_to_aspirin,
    MAX(has_been_exposed_to_codeine) as has_been_exposed_to_codeine,
    MAX(has_been_exposed_to_tramadol) as has_been_exposed_to_tramadol,
    MAX(has_been_exposed_to_nalbuphine) as has_been_exposed_to_nalbuphine,
    MAX(has_been_exposed_to_mepiridine) as has_been_exposed_to_mepiridine,
    MAX(has_been_exposed_to_naltrexone) as has_been_exposed_to_naltrexone,
    MAX(has_been_exposed_to_acetaminophen) as has_been_exposed_to_acetaminophen,
    
FROM (""" + query_that_results_in_table_of_visit_occurrences_for_cohort + """) table_of_visit_occurrences
LEFT JOIN(""" + query_that_results_in_positive_indicators_of_ibuprofen + """) table_of_positive_indicators_of_ibuprofen
ON table_of_visit_occurrences.visit_occurrence_id = table_of_positive_indicators_of_ibuprofen.visit_occurrence_id
LEFT JOIN(""" + query_that_results_in_positive_indicators_of_buprenorphine + """) table_of_positive_indicators_of_buprenorphine
ON table_of_visit_occurrences.visit_occurrence_id = table_of_positive_indicators_of_buprenorphine.visit_occurrence_id
LEFT JOIN(""" + query_that_results_in_positive_indicators_of_fentanyl + """) table_of_positive_indicators_of_fentanyl
ON table_of_visit_occurrences.visit_occurrence_id = table_of_positive_indicators_of_fentanyl.visit_occurrence_id
LEFT JOIN(""" + query_that_results_in_positive_indicators_of_morphine + """) table_of_positive_indicators_of_morphine
ON table_of_visit_occurrences.visit_occurrence_id = table_of_positive_indicators_of_morphine.visit_occurrence_id
LEFT JOIN(""" + query_that_results_in_positive_indicators_of_oxycodone + """) table_of_positive_indicators_of_oxycodone
ON table_of_visit_occurrences.visit_occurrence_id = table_of_positive_indicators_of_oxycodone.visit_occurrence_id
LEFT JOIN(""" + query_that_results_in_positive_indicators_of_hydromorphone + """) table_of_positive_indicators_of_hydromorphone
ON table_of_visit_occurrences.visit_occurrence_id = table_of_positive_indicators_of_hydromorphone.visit_occurrence_id
LEFT JOIN(""" + query_that_results_in_positive_indicators_of_aspirin + """) table_of_positive_indicators_of_aspirin
ON table_of_visit_occurrences.visit_occurrence_id = table_of_positive_indicators_of_aspirin.visit_occurrence_id
LEFT JOIN(""" + query_that_results_in_positive_indicators_of_codeine + """) table_of_positive_indicators_of_codeine
ON table_of_visit_occurrences.visit_occurrence_id = table_of_positive_indicators_of_codeine.visit_occurrence_id
LEFT JOIN(""" + query_that_results_in_positive_indicators_of_tramadol + """) table_of_positive_indicators_of_tramadol
ON table_of_visit_occurrences.visit_occurrence_id = table_of_positive_indicators_of_tramadol.visit_occurrence_id
LEFT JOIN(""" + query_that_results_in_positive_indicators_of_nalbuphine + """) table_of_positive_indicators_of_nalbuphine
ON table_of_visit_occurrences.visit_occurrence_id = table_of_positive_indicators_of_nalbuphine.visit_occurrence_id
LEFT JOIN(""" + query_that_results_in_positive_indicators_of_mepiridine + """) table_of_positive_indicators_of_mepiridine
ON table_of_visit_occurrences.visit_occurrence_id = table_of_positive_indicators_of_mepiridine.visit_occurrence_id
LEFT JOIN(""" + query_that_results_in_positive_indicators_of_naltrexone + """) table_of_positive_indicators_of_naltrexone
ON table_of_visit_occurrences.visit_occurrence_id = table_of_positive_indicators_of_naltrexone.visit_occurrence_id
LEFT JOIN(""" + query_that_results_in_positive_indicators_of_acetaminophen + """) table_of_positive_indicators_of_acetaminophen
ON table_of_visit_occurrences.visit_occurrence_id = table_of_positive_indicators_of_acetaminophen.visit_occurrence_id
GROUP BY table_of_visit_occurrences.visit_occurrence_id
"""

query_that_results_in_feature_matrix = """
SELECT *
FROM (""" + query_that_results_in_condition_feature_matrix + """) condition_feature_matrix
LEFT JOIN(""" + query_that_results_in_drug_feature_matrix + """) drug_feature_matrix
ON
condition_feature_matrix.visit_occurrence_id = drug_feature_matrix.visit_occurrence_id
"""

query_that_results_in_table_of_distinct_person_IDs_of_patients_who_have_Opioid_abuse = """
SELECT DISTINCT person_id
FROM (""" + query_that_results_in_feature_matrix + """)
WHERE has_Opioid_abuse = 1
"""

query_that_results_in_number_of_distinct_person_IDs_of_patients_who_have_Opioid_abuse = """
SELECT COUNT(person_id) as number_of_distinct_person_IDs_of_patients_who_have_Opioid_abuse
FROM (""" + query_that_results_in_table_of_distinct_person_IDs_of_patients_who_have_Opioid_abuse + """)
"""

query_that_results_in_table_of_distinct_person_IDs_of_patients_who_do_not_have_Opioid_abuse = """
SELECT DISTINCT person_id
FROM (""" + query_that_results_in_feature_matrix + """)
WHERE person_id NOT IN (""" + query_that_results_in_table_of_distinct_person_IDs_of_patients_who_have_Opioid_abuse + """)
"""

query_that_results_in_table_of_person_IDs_of_patients_who_do_not_have_Opioids_with_equal_number = """
SELECT person_id
FROM (""" + query_that_results_in_table_of_distinct_person_IDs_of_patients_who_do_not_have_Opioid_abuse + """)
ORDER BY RAND()
LIMIT 2631
"""

query_that_results_in_table_of_person_IDs_in_undersample = """
SELECT person_id
FROM (""" + query_that_results_in_table_of_distinct_person_IDs_of_patients_who_have_Opioid_abuse + """)
UNION ALL
(""" + query_that_results_in_table_of_person_IDs_of_patients_who_do_not_have_Opioids_with_equal_number + """)
"""

query_that_results_in_slice_of_feature_matrix_for_undersample = """
SELECT *
FROM (""" + query_that_results_in_feature_matrix + """)
WHERE person_id IN (""" + query_that_results_in_table_of_person_IDs_in_undersample + """)
"""

if __name__ == "__main__":
    feature_matrix = get_data_frame(query_that_results_in_feature_matrix)
    IntegerArray_of_distinct_person_IDs_in_feature_matrix = pd.unique(feature_matrix["person_id"])
    number_of_distinct_person_IDs_in_feature_matrix = len(IntegerArray_of_distinct_person_IDs_in_feature_matrix)
    print("Number of distinct person IDs of:")
    print("1) Feature matrix: " + str(number_of_distinct_person_IDs_in_feature_matrix))

    slice_of_feature_matrix_where_has_Opioid_abuse_is_1 = feature_matrix[feature_matrix["has_Opioid_abuse"] == 1]
    IntegerArray_of_distinct_person_IDs_of_patients_who_have_Opioid_abuse = pd.unique(slice_of_feature_matrix_where_has_Opioid_abuse_is_1["person_id"])
    number_of_distinct_person_IDs_of_patients_who_have_Opioid_abuse = len(IntegerArray_of_distinct_person_IDs_of_patients_who_have_Opioid_abuse)
    print("2) Patients who have Opioid abuse: " + str(number_of_distinct_person_IDs_of_patients_who_have_Opioid_abuse))

    slice_of_feature_matrix_corresponding_to_patients_who_have_Opioid_abuse = feature_matrix[feature_matrix["person_id"].isin(IntegerArray_of_distinct_person_IDs_of_patients_who_have_Opioid_abuse)]
    slice_of_feature_matrix_corresponding_to_patients_who_have_Opioid_abuse_and_where_is_exposed_to_Opioids_is_1 = slice_of_feature_matrix_corresponding_to_patients_who_have_Opioid_abuse[slice_of_feature_matrix_corresponding_to_patients_who_have_Opioid_abuse["is_exposed_to_Opioids"] == 1]
    IntegerArray_of_distinct_person_IDs_of_patients_who_have_Opioid_abuse_and_who_are_exposed_to_Opioids = pd.unique(slice_of_feature_matrix_corresponding_to_patients_who_have_Opioid_abuse_and_where_is_exposed_to_Opioids_is_1["person_id"])
    array_of_distinct_person_IDs_of_patients_who_have_Opioid_abuse_and_who_are_exposed_to_Opioids = np.array(IntegerArray_of_distinct_person_IDs_of_patients_who_have_Opioid_abuse_and_who_are_exposed_to_Opioids)
    number_of_distinct_person_IDs_of_patients_who_have_Opioid_abuse_and_who_are_exposed_to_Opioids = len(IntegerArray_of_distinct_person_IDs_of_patients_who_have_Opioid_abuse_and_who_are_exposed_to_Opioids)
    print("3) Patients who have Opioid abuse and who are exposed to Opioids: " + str(number_of_distinct_person_IDs_of_patients_who_have_Opioid_abuse_and_who_are_exposed_to_Opioids))

    slice_of_feature_matrix_of_patients_who_do_not_have_Opioid_abuse = feature_matrix[~feature_matrix["person_id"].isin(IntegerArray_of_distinct_person_IDs_of_patients_who_have_Opioid_abuse)]
    IntegerArray_of_distinct_person_IDs_of_patients_who_do_not_have_Opioid_abuse = pd.unique(slice_of_feature_matrix_of_patients_who_do_not_have_Opioid_abuse["person_id"])
    number_of_distinct_person_IDs_of_patients_who_do_not_have_Opioid_abuse = len(IntegerArray_of_distinct_person_IDs_of_patients_who_do_not_have_Opioid_abuse)
    print("4) Patients who do not have Opioid abuse: " + str(number_of_distinct_person_IDs_of_patients_who_do_not_have_Opioid_abuse))

    slice_of_feature_matrix_of_patients_who_do_not_have_Opioid_abuse_and_where_is_exposed_to_Opioids_is_1 = slice_of_feature_matrix_of_patients_who_do_not_have_Opioid_abuse[slice_of_feature_matrix_of_patients_who_do_not_have_Opioid_abuse["is_exposed_to_Opioids"] == 1]
    IntegerArray_of_distinct_person_IDs_of_patients_who_do_not_have_Opioid_abuse_and_who_are_exposed_to_Opioids = pd.unique(slice_of_feature_matrix_of_patients_who_do_not_have_Opioid_abuse_and_where_is_exposed_to_Opioids_is_1["person_id"])
    number_of_distinct_person_IDs_of_patients_who_do_not_have_Opioid_abuse_and_who_are_exposed_to_Opioids = len(IntegerArray_of_distinct_person_IDs_of_patients_who_do_not_have_Opioid_abuse_and_who_are_exposed_to_Opioids)
    print("5) Patients who do not have Opioid abuse and who are exposed to Opioids: " + str(number_of_distinct_person_IDs_of_patients_who_do_not_have_Opioid_abuse_and_who_are_exposed_to_Opioids))

    array_of_distinct_person_IDs_of_patients_who_do_not_have_Opioid_abuse_and_who_are_exposed_to_Opioids = np.array(IntegerArray_of_distinct_person_IDs_of_patients_who_do_not_have_Opioid_abuse_and_who_are_exposed_to_Opioids)
    shuffled_array_of_distinct_person_IDs_of_patients_who_do_not_have_Opioid_abuse_and_who_are_exposed_to_Opioids = np.array(array_of_distinct_person_IDs_of_patients_who_do_not_have_Opioid_abuse_and_who_are_exposed_to_Opioids)
    np.random.shuffle(array_of_distinct_person_IDs_of_patients_who_do_not_have_Opioid_abuse_and_who_are_exposed_to_Opioids)
    array_of_distinct_person_IDs_of_patients_who_do_not_have_Opioid_abuse_and_who_are_exposed_to_Opioids_with_equal_number = shuffled_array_of_distinct_person_IDs_of_patients_who_do_not_have_Opioid_abuse_and_who_are_exposed_to_Opioids[:number_of_distinct_person_IDs_of_patients_who_have_Opioid_abuse_and_who_are_exposed_to_Opioids]
    number_of_distinct_person_IDs_of_patients_who_do_not_have_Opioid_abuse_and_who_are_exposed_to_Opioids_with_equal_number = len(array_of_distinct_person_IDs_of_patients_who_do_not_have_Opioid_abuse_and_who_are_exposed_to_Opioids_with_equal_number)
    print("6) Patients who do not have Opioid abuse and who are exposed to Opioids with equal number: " + str(number_of_distinct_person_IDs_of_patients_who_do_not_have_Opioid_abuse_and_who_are_exposed_to_Opioids_with_equal_number))
    print()

    array_of_distinct_person_IDs = np.concatenate([array_of_distinct_person_IDs_of_patients_who_have_Opioid_abuse_and_who_are_exposed_to_Opioids, array_of_distinct_person_IDs_of_patients_who_do_not_have_Opioid_abuse_and_who_are_exposed_to_Opioids_with_equal_number])
    number_of_distinct_person_IDs = len(array_of_distinct_person_IDs)
    slice_of_feature_matrix = feature_matrix[feature_matrix["person_id"].isin(array_of_distinct_person_IDs)]
    print("Slice of feature matrix:")
    print(slice_of_feature_matrix)

    number_of_distinct_person_IDs_in_slice_of_feature_matrix = len(pd.unique(slice_of_feature_matrix["person_id"]))
    print("Number of distinct person IDs of slice of feature matrix: " + str(number_of_distinct_person_IDs_in_slice_of_feature_matrix))

    slice_of_feature_matrix.to_csv("Slice_Of_Feature_Matrix.csv")
