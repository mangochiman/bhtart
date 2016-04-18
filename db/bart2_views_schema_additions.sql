
-- Host: localhost    Database: bart2
-- ------------------------------------------------------
-- Server version	5.1.54-1ubuntu4-log
/*!40101 SET @OLD_CHARACTER_SET_CLIENT=@@CHARACTER_SET_CLIENT */;
/*!40101 SET @OLD_CHARACTER_SET_RESULTS=@@CHARACTER_SET_RESULTS */;
/*!40101 SET @OLD_COLLATION_CONNECTION=@@COLLATION_CONNECTION */;
/*!40101 SET NAMES utf8 */;
/*!40103 SET @OLD_TIME_ZONE=@@TIME_ZONE */;
/*!40103 SET TIME_ZONE='+00:00' */;
/*!40014 SET @OLD_UNIQUE_CHECKS=@@UNIQUE_CHECKS, UNIQUE_CHECKS=0 */;
/*!40014 SET @OLD_FOREIGN_KEY_CHECKS=@@FOREIGN_KEY_CHECKS, FOREIGN_KEY_CHECKS=0 */;
/*!40101 SET @OLD_SQL_MODE=@@SQL_MODE, SQL_MODE='NO_AUTO_VALUE_ON_ZERO' */;
/*!40111 SET @OLD_SQL_NOTES=@@SQL_NOTES, SQL_NOTES=0 */;

-- view to capture avg ART/HIV care treatment time for ART patients at a given site
CREATE OR REPLACE ALGORITHM=UNDEFINED  SQL SECURITY INVOKER
VIEW `patient_service_waiting_time` AS
    SELECT
        `e`.`patient_id` AS `patient_id`,
        cast(`e`.`encounter_datetime` as date) AS `visit_date`,
        min(`e`.`encounter_datetime`) AS `start_time`,
        max(`e`.`encounter_datetime`) AS `finish_time`,
        timediff(max(`e`.`encounter_datetime`),
                min(`e`.`encounter_datetime`)) AS `service_time`
    FROM
        (`encounter` `e`
        join `encounter` `e2` ON (((`e`.`patient_id` = `e2`.`patient_id`)
            AND (`e`.`encounter_type` in (7 , 9, 12, 25, 51, 52, 53, 54, 68)))))
    WHERE
        ((`e`.`encounter_datetime` BETWEEN date_format((now() - interval 7 day),
                '%Y-%m-%d 00:00:00') AND date_format((now() - interval 1 day),
                '%Y-%m-%d 23:59:59'))
            AND (right(`e`.`encounter_datetime`, 2) <> '01')
            AND (right(`e`.`encounter_datetime`, 2) <> '01'))
    GROUP BY `e`.`patient_id` , cast(`e`.`encounter_datetime` as date)
    ORDER BY `e`.`patient_id` , `e`.`encounter_datetime`;

-- Non-voided HIV Clinic Consultation encounters
CREATE OR REPLACE ALGORITHM=UNDEFINED  SQL SECURITY INVOKER
  VIEW `clinic_consultation_encounter` AS
  SELECT `encounter`.`encounter_id` AS `encounter_id`,
         `encounter`.`encounter_type` AS `encounter_type`,
         `encounter`.`patient_id` AS `patient_id`,
         `encounter`.`provider_id` AS `provider_id`,
         `encounter`.`location_id` AS `location_id`,
         `encounter`.`form_id` AS `form_id`,
         `encounter`.`encounter_datetime` AS `encounter_datetime`,
         `encounter`.`creator` AS `creator`,
         `encounter`.`date_created` AS `date_created`,
         `encounter`.`voided` AS `voided`,
         `encounter`.`voided_by` AS `voided_by`,
         `encounter`.`date_voided` AS `date_voided`,
         `encounter`.`void_reason` AS `void_reason`,
         `encounter`.`uuid` AS `uuid`,
         `encounter`.`changed_by` AS `changed_by`,
         `encounter`.`date_changed` AS `date_changed`
  FROM `encounter`
  WHERE (`encounter`.`encounter_type` = 53 AND `encounter`.`voided` = 0);

-- ARV drugs
CREATE OR REPLACE ALGORITHM=UNDEFINED  SQL SECURITY INVOKER
	VIEW `arv_drug` AS
	SELECT `drug_id` FROM `drug`
	WHERE `concept_id` IN (SELECT `concept_id` FROM `concept_set` WHERE `concept_set` = 1085);

-- ARV drugs orders
CREATE OR REPLACE ALGORITHM=UNDEFINED  SQL SECURITY INVOKER
   VIEW `arv_drugs_orders` AS
   SELECT `ord`.`patient_id`, `ord`.`encounter_id`, `ord`.`concept_id`, `ord`.`start_date`
   FROM `orders` `ord`
   WHERE `ord`.`voided` = 0
   AND `ord`.`concept_id` IN (SELECT `concept_id` FROM `concept_set` WHERE `concept_set` = 1085);

-- Non-voided HIV Clinic Registration encounters
CREATE OR REPLACE ALGORITHM=UNDEFINED  SQL SECURITY INVOKER
	VIEW `clinic_registration_encounter` AS
	SELECT `encounter`.`encounter_id` AS `encounter_id`,
         `encounter`.`encounter_type` AS `encounter_type`,
         `encounter`.`patient_id` AS `patient_id`,
         `encounter`.`provider_id` AS `provider_id`,
         `encounter`.`location_id` AS `location_id`,
         `encounter`.`form_id` AS `form_id`,
         `encounter`.`encounter_datetime` AS `encounter_datetime`,
         `encounter`.`creator` AS `creator`,
         `encounter`.`date_created` AS `date_created`,
         `encounter`.`voided` AS `voided`,
         `encounter`.`voided_by` AS `voided_by`,
         `encounter`.`date_voided` AS `date_voided`,
         `encounter`.`void_reason` AS `void_reason`,
         `encounter`.`uuid` AS `uuid`,
         `encounter`.`changed_by` AS `changed_by`,
         `encounter`.`date_changed` AS `date_changed`
	FROM `encounter`
	WHERE (`encounter`.`encounter_type` = 9 AND `encounter`.`voided` = 0);



DROP FUNCTION IF EXISTS date_antiretrovirals_started;

DELIMITER $$
CREATE FUNCTION date_antiretrovirals_started(set_patient_id INT, min_state_date DATE) RETURNS DATE
BEGIN

DECLARE date_started DATE;

SET date_started = (SELECT LEFT(value_datetime,10) FROM obs WHERE concept_id = 2516 AND person_id = set_patient_id LIMIT 1);

if date_started is NULL then
SET date_started = min_state_date;
end if;

RETURN date_started;
END$$
DELIMITER ;

-- The date of the first On ARVs state for each patient
CREATE OR REPLACE ALGORITHM=UNDEFINED  SQL SECURITY INVOKER
  VIEW `earliest_start_date` AS
  SELECT `p`.`patient_id` AS `patient_id`, DATE(patient_start_date(`p`.`patient_id`)) AS `date_enrolled`,
         date_antiretrovirals_started(`p`.`patient_id`, MIN(`s`.`start_date`)) AS `earliest_start_date`, `person`.`death_date` AS death_date,
         (DATEDIFF(date_antiretrovirals_started(`p`.`patient_id`, MIN(`s`.`start_date`)), `person`.`birthdate`)/365.25) AS age_at_initiation,
         DATEDIFF(MIN(`s`.`start_date`), `person`.`birthdate`) AS age_in_days
  FROM ((`patient_program` `p`
  LEFT JOIN `patient_state` `s` ON((`p`.`patient_program_id` = `s`.`patient_program_id`)))
  LEFT JOIN `person` ON((`person`.`person_id` = `p`.`patient_id`)))
  WHERE ((`p`.`voided` = 0) AND (`s`.`voided` = 0) AND (`p`.`program_id` = 1) AND
        (`s`.`state` = 7))
  GROUP BY `p`.`patient_id`;

-- 7937 = Ever registered at ART clinic
CREATE OR REPLACE ALGORITHM=UNDEFINED  SQL SECURITY INVOKER
  VIEW `ever_registered_obs` AS
  SELECT `obs`.`obs_id` AS `obs_id`,
         `obs`.`person_id` AS `person_id`,
         `obs`.`concept_id` AS `concept_id`,
         `obs`.`encounter_id` AS `encounter_id`,
         `obs`.`order_id` AS `order_id`,
         `obs`.`obs_datetime` AS `obs_datetime`,
         `obs`.`location_id` AS `location_id`,
         `obs`.`obs_group_id` AS `obs_group_id`,
         `obs`.`accession_number` AS `accession_number`,
         `obs`.`value_group_id` AS `value_group_id`,
         `obs`.`value_boolean` AS `value_boolean`,
         `obs`.`value_coded` AS `value_coded`,
         `obs`.`value_coded_name_id` AS `value_coded_name_id`,
         `obs`.`value_drug` AS `value_drug`,
         `obs`.`value_datetime` AS `value_datetime`,
         `obs`.`value_numeric` AS `value_numeric`,
         `obs`.`value_modifier` AS `value_modifier`,
         `obs`.`value_text` AS `value_text`,
         `obs`.`date_started` AS `date_started`,
         `obs`.`date_stopped` AS `date_stopped`,
         `obs`.`comments` AS `comments`,
         `obs`.`creator` AS `creator`,
         `obs`.`date_created` AS `date_created`,
         `obs`.`voided` AS `voided`,
         `obs`.`voided_by` AS `voided_by`,
         `obs`.`date_voided` AS `date_voided`,
         `obs`.`void_reason` AS `void_reason`,
         `obs`.`value_complex` AS `value_complex`,
         `obs`.`uuid` AS `uuid`
  FROM `obs`
  WHERE ((`obs`.`concept_id` = 7937) AND (`obs`.`voided` = 0))
  AND (`obs`.`value_coded` = 1065);

CREATE OR REPLACE ALGORITHM=UNDEFINED  SQL SECURITY INVOKER
  VIEW `patient_pregnant_obs` AS
  SELECT `obs`.`obs_id` AS `obs_id`,
         `obs`.`person_id` AS `person_id`,
         `obs`.`concept_id` AS `concept_id`,
         `obs`.`encounter_id` AS `encounter_id`,
         `obs`.`order_id` AS `order_id`,
         `obs`.`obs_datetime` AS `obs_datetime`,
         `obs`.`location_id` AS `location_id`,
         `obs`.`obs_group_id` AS `obs_group_id`,
         `obs`.`accession_number` AS `accession_number`,
         `obs`.`value_group_id` AS `value_group_id`,
         `obs`.`value_boolean` AS `value_boolean`,
         `obs`.`value_coded` AS `value_coded`,
         `obs`.`value_coded_name_id` AS `value_coded_name_id`,
         `obs`.`value_drug` AS `value_drug`,
         `obs`.`value_datetime` AS `value_datetime`,
         `obs`.`value_numeric` AS `value_numeric`,
         `obs`.`value_modifier` AS `value_modifier`,
         `obs`.`value_text` AS `value_text`,
         `obs`.`date_started` AS `date_started`,
         `obs`.`date_stopped` AS `date_stopped`,
         `obs`.`comments` AS `comments`,
         `obs`.`creator` AS `creator`,
         `obs`.`date_created` AS `date_created`,
         `obs`.`voided` AS `voided`,
         `obs`.`voided_by` AS `voided_by`,
         `obs`.`date_voided` AS `date_voided`,
         `obs`.`void_reason` AS `void_reason`,
         `obs`.`value_complex` AS `value_complex`,
         `obs`.`uuid` AS `uuid`
  FROM `obs`
  INNER JOIN `person` ON ((`person`.`person_id` = `obs`.`person_id`))
  WHERE ((`obs`.`concept_id` IN (6131,1755, 7972)) AND
         (`obs`.`value_coded` = 1065) AND
         (`obs`.`voided` = 0) AND
         (`person`.`gender` = 'F'));

CREATE OR REPLACE ALGORITHM=UNDEFINED  SQL SECURITY INVOKER
  VIEW `patient_state_on_arvs` AS
  SELECT `patient_state`.`patient_state_id` AS `patient_state_id`,
         `patient_state`.`patient_program_id` AS `patient_program_id`,
         `patient_state`.`state` AS `state`,
         `patient_state`.`start_date` AS `start_date`,
         `patient_state`.`end_date` AS `end_date`,
         `patient_state`.`creator` AS `creator`,
         `patient_state`.`date_created` AS `date_created`,
         `patient_state`.`changed_by` AS `changed_by`,
         `patient_state`.`date_changed` AS `date_changed`,
         `patient_state`.`voided` AS `voided`,
         `patient_state`.`voided_by` AS `voided_by`,
         `patient_state`.`date_voided` AS `date_voided`,
         `patient_state`.`void_reason` AS `void_reason`,
         `patient_state`.`uuid` AS `uuid`
  FROM `patient_state`
  WHERE (`patient_state`.`state` = 7 AND `patient_state`.`voided` = 0);

CREATE OR REPLACE ALGORITHM=UNDEFINED  SQL SECURITY INVOKER
  VIEW `regimen_observation` AS
  SELECT `obs`.`obs_id` AS `obs_id`,
         `obs`.`person_id` AS `person_id`,
         `obs`.`concept_id` AS `concept_id`,
         `obs`.`encounter_id` AS `encounter_id`,
         `obs`.`order_id` AS `order_id`,
         `obs`.`obs_datetime` AS `obs_datetime`,
         `obs`.`location_id` AS `location_id`,
         `obs`.`obs_group_id` AS `obs_group_id`,
         `obs`.`accession_number` AS `accession_number`,
         `obs`.`value_group_id` AS `value_group_id`,
         `obs`.`value_boolean` AS `value_boolean`,
         `obs`.`value_coded` AS `value_coded`,
         `obs`.`value_coded_name_id` AS `value_coded_name_id`,
         `obs`.`value_drug` AS `value_drug`,
         `obs`.`value_datetime` AS `value_datetime`,
         `obs`.`value_numeric` AS `value_numeric`,
         `obs`.`value_modifier` AS `value_modifier`,
         `obs`.`value_text` AS `value_text`,
         `obs`.`date_started` AS `date_started`,
         `obs`.`date_stopped` AS `date_stopped`,
         `obs`.`comments` AS `comments`,
         `obs`.`creator` AS `creator`,
         `obs`.`date_created` AS `date_created`,
         `obs`.`voided` AS `voided`,
         `obs`.`voided_by` AS `voided_by`,
         `obs`.`date_voided` AS `date_voided`,
         `obs`.`void_reason` AS `void_reason`,
         `obs`.`value_complex` AS `value_complex`,
         `obs`.`uuid` AS `uuid`
  FROM `obs`
  WHERE ((`obs`.`concept_id` = 2559) AND (`obs`.`voided` = 0));

CREATE OR REPLACE ALGORITHM=UNDEFINED  SQL SECURITY INVOKER
  VIEW `start_date_observation` AS
  SELECT `obs`.`person_id` AS `person_id`,
         `obs`.`obs_datetime` AS `obs_datetime`,
         `obs`.`value_datetime` AS `value_datetime`
  FROM `obs`
  WHERE ((`obs`.`concept_id` = 2516) AND (`obs`.`voided` = 0))
  GROUP BY `obs`.`person_id`,`obs`.`value_datetime`;

CREATE OR REPLACE ALGORITHM=UNDEFINED  SQL SECURITY INVOKER
  VIEW `tb_status_observations` AS
  SELECT `obs`.`obs_id` AS `obs_id`,
         `obs`.`person_id` AS `person_id`,
         `obs`.`concept_id` AS `concept_id`,
         `obs`.`encounter_id` AS `encounter_id`,
         `obs`.`order_id` AS `order_id`,
         `obs`.`obs_datetime` AS `obs_datetime`,
         `obs`.`location_id` AS `location_id`,
         `obs`.`obs_group_id` AS `obs_group_id`,
         `obs`.`accession_number` AS `accession_number`,
         `obs`.`value_group_id` AS `value_group_id`,
         `obs`.`value_boolean` AS `value_boolean`,
         `obs`.`value_coded` AS `value_coded`,
         `obs`.`value_coded_name_id` AS `value_coded_name_id`,
         `obs`.`value_drug` AS `value_drug`,
         `obs`.`value_datetime` AS `value_datetime`,
         `obs`.`value_numeric` AS `value_numeric`,
         `obs`.`value_modifier` AS `value_modifier`,
         `obs`.`value_text` AS `value_text`,
         `obs`.`date_started` AS `date_started`,
         `obs`.`date_stopped` AS `date_stopped`,
         `obs`.`comments` AS `comments`,
         `obs`.`creator` AS `creator`,
         `obs`.`date_created` AS `date_created`,
         `obs`.`voided` AS `voided`,
         `obs`.`voided_by` AS `voided_by`,
         `obs`.`date_voided` AS `date_voided`,
         `obs`.`void_reason` AS `void_reason`,
         `obs`.`value_complex` AS `value_complex`,
         `obs`.`uuid` AS `uuid`
  FROM `obs`
  WHERE ((`obs`.`concept_id` = 7459) and (`obs`.`voided` = 0));

-- The following 2 views will be used in calculation of defaulted dates
CREATE OR REPLACE ALGORITHM=UNDEFINED  SQL SECURITY INVOKER
  VIEW `amount_dispensed_obs` AS
  SELECT
    `o`.`person_id`,
    `o`.`encounter_id`,
    `o`.`order_id`,
    `o`.`obs_datetime`,
    `do`.`drug_inventory_id`,
    `do`.`equivalent_daily_dose`,
    `ord`.`start_date`,
    `o`.`value_numeric`
FROM
    `obs` `o`
        INNER JOIN
    `orders` `ord` ON `o`.`order_id` = `ord`.`order_id` and `ord`.`voided` = 0
        INNER JOIN
    `drug_order` `do` ON `ord`.`order_id` = `do`.`order_id`
        INNER JOIN
    `arv_drug` `ad` ON `do`.`drug_inventory_id` = `ad`.`drug_id`
WHERE
    `o`.`concept_id` = 2834 AND `o`.`voided` = 0;

CREATE OR REPLACE ALGORITHM=UNDEFINED  SQL SECURITY INVOKER
  VIEW `amount_brought_back_obs` AS
SELECT
    `o`.`person_id`,
    `o`.`encounter_id`,
    `o`.`order_id`,
    `o`.`obs_datetime`,
    `do`.`drug_inventory_id`,
    `do`.`equivalent_daily_dose`,
    `o`.`value_numeric`,
    `do`.`quantity`
FROM
    `obs` `o`
        INNER JOIN
    `drug_order` `do` ON `o`.`order_id` = `do`.`order_id`
        INNER JOIN
    `arv_drug` `ad` ON `do`.`drug_inventory_id` = `ad`.`drug_id`
WHERE
    `o`.`concept_id` = 2540 AND `o`.`voided` = 0;

CREATE OR REPLACE ALGORITHM=UNDEFINED  SQL SECURITY INVOKER
  VIEW `reason_for_eligibility_obs` AS
SELECT
    `e`.`patient_id`, `n`.`name` AS `reason_for_eligibility`, `o`.`obs_datetime`, `e`.`earliest_start_date`, `e`.`date_enrolled` AS `date_enrolled`
FROM
    `earliest_start_date` `e`
        LEFT JOIN
    `obs` `o` ON `e`.`patient_id` = `o`.`person_id`
        AND `o`.`concept_id` = 7563
        AND `o`.`voided` = 0
        LEFT JOIN
    `concept_name` `n` ON `n`.`concept_id` = `o`.`value_coded`
        AND `n`.`concept_name_type` = 'FULLY_SPECIFIED'
        AND `n`.`voided` = 0
ORDER BY `e`.`patient_id` , `o`.`obs_datetime` DESC;

CREATE OR REPLACE ALGORITHM=UNDEFINED  SQL SECURITY INVOKER
  VIEW `patients_with_has_transfer_letter_yes` AS
SELECT
    `o`.`person_id`, `p`.`gender`, `o`.`obs_datetime`, `o`.`date_created`, `e`.`earliest_start_date`
FROM
    `obs` `o`
        INNER JOIN
    `person` `p` ON `p`.`person_id` = `o`.`person_id`
        AND `p`.`voided` = 0
        AND `o`.`voided` = 0
        INNER JOIN
    `earliest_start_date` `e` ON `e`.`patient_id` = `o`.`person_id`
WHERE
    `o`.`concept_id` = 6393
        AND `o`.`value_coded` = 1065
        AND `o`.`voided` = 0;

CREATE OR REPLACE ALGORITHM=UNDEFINED  SQL SECURITY INVOKER
  VIEW `all_patients_attributes` AS
SELECT
    `person_id`,
    MAX(CASE WHEN `person_attribute_type_id` = 13 THEN `value` END) AS `occupation`,
    MAX(CASE WHEN `person_attribute_type_id` = 12 THEN `value` END) AS `cell_phone`,
    MAX(CASE WHEN `person_attribute_type_id` = 14 THEN `value` END) AS `home_phone`,
    MAX(CASE WHEN `person_attribute_type_id` = 15 THEN `value` END) AS `office_phone`
FROM
    `person_attribute`
WHERE `voided` = 0
GROUP BY `person_id`;

CREATE OR REPLACE ALGORITHM=UNDEFINED  SQL SECURITY INVOKER
  VIEW `all_patient_identifiers` AS
SELECT
    `patient_id`,
    MAX(CASE WHEN `identifier_type` = 1 THEN `identifier` END) AS `openmrs_ident_type`,
    MAX(CASE WHEN `identifier_type` = 3 THEN `identifier` END) AS `national_id`,
    MAX(CASE WHEN `identifier_type` = 4 THEN `identifier` END) AS `arv_number`,
    MAX(CASE WHEN `identifier_type` = 2 THEN `identifier` END) AS `legacy_id`,
    MAX(CASE WHEN `identifier_type` = 5 THEN `identifier` END) AS `prev_art_number`,
    MAX(CASE WHEN `identifier_type` = 7 THEN `identifier` END) AS `tb_number`,
    MAX(CASE WHEN `identifier_type` = 17 THEN `identifier` END) AS `filing_number`,
    MAX(CASE WHEN `identifier_type` = 18 THEN `identifier` END) AS `archived_filing_number`,
    MAX(CASE WHEN `identifier_type` = 22 THEN `identifier` END) AS `pre_art_number`
FROM
    `patient_identifier`
WHERE `voided` = 0
GROUP BY `patient_id`;

CREATE OR REPLACE ALGORITHM=UNDEFINED  SQL SECURITY INVOKER
  VIEW `all_person_addresses` AS
SELECT
    `p`. *
FROM
    `person_address` `p`
WHERE
    `p`.`person_address_id` = (SELECT
            MAX(`pad`.`person_address_id`)
        FROM
            `person_address` `pad`
        WHERE
            `pad`.`person_id` = `p`.`person_id`
                AND `pad`.`voided` = 0)
        AND `p`.`voided` = 0;

CREATE OR REPLACE ALGORITHM=UNDEFINED  SQL SECURITY INVOKER
  VIEW `guardians` AS
SELECT
    `person_a` AS `patient_id`,
    `person_b` AS `guardian_id`,
    `per`.`gender` AS `gender`,
    `p`.`given_name` AS `given_name`,
    `p`.`family_name` AS `family_name`,
    `p`.`middle_name` AS `middle_name`,
    `per`.`birthdate_estimated` AS `birthdate_estimated`,
    `per`.`birthdate` AS `birthdate`,
    `pa`.`address2` AS `home_district`,
    `pa`.`state_province` AS `current_district`,
    `pa`.`address1` AS `landmark`,
    `pa`.`city_village` AS `current_residence`,
    `pa`.`county_district` AS `traditional_authority`
FROM
    `relationship` `r`
        INNER JOIN
    `person_name` `p` ON `p`.`person_id` = `r`.`person_b`
        LEFT JOIN
    `all_person_addresses` `pa` ON `pa`.`person_id` = `p`.`person_id`
        INNER JOIN
    `person` `per` ON `per`.`person_id` = `p`.`person_id` AND `p`.`voided` = 0
WHERE
    `r`.`voided` = 0
AND `r`.`person_a` IN (SELECT `e`.`patient_id` FROM `earliest_start_date` `e`)
ORDER BY `patient_id`;

CREATE OR REPLACE ALGORITHM=UNDEFINED  SQL SECURITY INVOKER
  VIEW `patients_demographics` AS
SELECT
    `esd`.`patient_id`,
    `p`.`given_name` AS `given_name`,
    `p`.`family_name` AS `family_name`,
    `p`.`middle_name` AS `middle_name`,
    `per`.`gender` AS `gender`,
    `per`.`birthdate_estimated`,
    `per`.`birthdate` AS `birthdate`,
    `pa`.`address2` AS `home_district`,
    `pa`.`state_province` AS `current_district`,
    `pa`.`address1` AS `landmark`,
    `pa`.`city_village` AS `current_residence`,
    `pa`.`county_district` AS `traditional_authority`,
    `esd`.`date_enrolled`,
    `esd`.`earliest_start_date`,
    `esd`.`death_date`,
    `esd`.`age_at_initiation`,
    `esd`.`age_in_days`
FROM
    `earliest_start_date` `esd`
        INNER JOIN
    `person_name` `p` ON `p`.`person_id` = `esd`.`patient_id` and `p`.`voided` = 0
        LEFT JOIN
    `all_person_addresses` `pa` ON `pa`.`person_id` = `p`.`person_id` and `pa`.`voided` = 0
        INNER JOIN
    `person` `per` ON `per`.`person_id` = `p`.`person_id` and `per`.`voided` = 0
GROUP BY `esd`.`patient_id`
ORDER BY `patient_id`;

DROP FUNCTION IF EXISTS earliest_start_date_at_clinic;

DELIMITER $$
CREATE FUNCTION earliest_start_date_at_clinic(set_patient_id INT) RETURNS DATE
BEGIN

DECLARE date_started DATE;

SET date_started = (SELECT MIN(start_date) FROM patient_state WHERE voided = 0 AND state = 7 AND patient_program_id IN (SELECT patient_program_id FROM patient_program WHERE patient_id = set_patient_id AND voided = 0 AND program_id = 1));

RETURN date_started;
END$$
DELIMITER ;

--
-- Dumping routines for database 'bart2'
--
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = utf8 */ ;
/*!50003 SET character_set_results = utf8 */ ;
/*!50003 SET collation_connection  = utf8_general_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = '' */ ;
DROP FUNCTION IF EXISTS `age`;
DELIMITER ;;
/*!50003 CREATE*/ /*!50020 */ /*!50003 FUNCTION `age`(birthdate varchar(10),visit_date varchar(10),date_created varchar(10),est int) RETURNS int(11)
    DETERMINISTIC
BEGIN
DECLARE n INT;

DECLARE birth_month INT;
DECLARE birth_day INT;

DECLARE year_when_patient_created INT;

DECLARE cur_month INT;
DECLARE cur_year INT;

set birth_month = (SELECT MONTH(FROM_DAYS(TO_DAYS(birthdate))));
set birth_day = (SELECT DAY(FROM_DAYS(TO_DAYS(birthdate))));

set cur_month = (SELECT MONTH(CURDATE()));
set cur_year = (SELECT YEAR(CURDATE()));

set year_when_patient_created = (SELECT YEAR(FROM_DAYS(TO_DAYS(date_created))));

set n =  (SELECT DATE_FORMAT(FROM_DAYS(TO_DAYS(visit_date)-TO_DAYS(DATE(birthdate))), '%Y')+0);

if birth_month = 7 and birth_day = 1 and est = 1 and cur_month < birth_month and year_when_patient_created = cur_year then set n=(n + 1);
end if;

RETURN n;
END */;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = utf8 */ ;
/*!50003 SET character_set_results = utf8 */ ;
/*!50003 SET collation_connection  = utf8_general_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = '' */ ;

DROP FUNCTION IF EXISTS `age_group`;
DELIMITER ;;
/*!50003 CREATE*/ /*!50020 */ /*!50003 FUNCTION `age_group`(birthdate varchar(10),visit_date varchar(10),date_created varchar(10),est int) RETURNS varchar(25) CHARSET latin1
    DETERMINISTIC
BEGIN
DECLARE avg VARCHAR(25);
DECLARE mths INT;
DECLARE n INT;

set avg="none";
set n =  (SELECT age(birthdate,visit_date,date_created,est));
set mths = (SELECT extract(MONTH FROM DATE(visit_date))-extract(MONTH FROM DATE(birthdate)));

if n >= 1 AND n < 5 then set avg="1 to < 5";
elseif n >= 5 AND n <= 14 then set avg="5 to 14";
elseif n > 14 AND n < 20 then set avg="> 14 to < 20";
elseif n >= 20 AND n < 30 then set avg="20 to < 30";
elseif n >= 30 AND n < 40 then set avg="30 to < 40";
elseif n >= 40 AND n < 50 then set avg="40 to < 50";
elseif n >= 50 then set avg="50 and above";
end if;

if mths >= 0 AND mths < 6 and avg="none" then set avg="< 6 months";
elseif mths >= 6 AND n < 12 and avg="none"then set avg="6 months to < 1 yr";
end if;

RETURN avg;
END */;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = utf8 */ ;
/*!50003 SET character_set_results = utf8 */ ;
/*!50003 SET collation_connection  = utf8_general_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = '' */ ;

DROP FUNCTION IF EXISTS `current_defaulter`;
DELIMITER ;;
/*!50003 CREATE*/ /*!50020 */ /*!50003 FUNCTION `current_defaulter`(my_patient_id INT, my_end_date DATETIME) RETURNS int(1)
BEGIN
	DECLARE done INT DEFAULT FALSE;
	DECLARE my_start_date, my_expiry_date, my_obs_datetime DATETIME;
	DECLARE my_daily_dose, my_quantity, my_pill_count, my_total_text, my_total_numeric DECIMAL;
	DECLARE my_drug_id, flag INT;

	DECLARE cur1 CURSOR FOR SELECT d.drug_inventory_id, o.start_date, d.equivalent_daily_dose daily_dose, d.quantity, o.start_date FROM drug_order d
		INNER JOIN arv_drug ad ON d.drug_inventory_id = ad.drug_id
		INNER JOIN orders o ON d.order_id = o.order_id
			AND d.quantity > 0
			AND o.voided = 0
			AND o.start_date <= my_end_date
			AND o.patient_id = my_patient_id;

	DECLARE CONTINUE HANDLER FOR NOT FOUND SET done = TRUE;

	SELECT MAX(o.start_date) INTO @obs_datetime FROM drug_order d
		INNER JOIN arv_drug ad ON d.drug_inventory_id = ad.drug_id
		INNER JOIN orders o ON d.order_id = o.order_id
			AND d.quantity > 0
			AND o.voided = 0
			AND o.start_date <= my_end_date
			AND o.patient_id = my_patient_id
		GROUP BY o.patient_id;

	OPEN cur1;

	SET flag = 0;

	read_loop: LOOP
		FETCH cur1 INTO my_drug_id, my_start_date, my_daily_dose, my_quantity, my_obs_datetime;

		IF done THEN
			CLOSE cur1;
			LEAVE read_loop;
		END IF;

		IF DATE(my_obs_datetime) = DATE(@obs_datetime) THEN

            SET my_pill_count = drug_pill_count(my_patient_id, my_drug_id, my_obs_datetime);

            SET @expiry_date = ADDDATE(my_start_date, ((my_quantity + my_pill_count)/my_daily_dose));

			IF my_expiry_date IS NULL THEN
				SET my_expiry_date = @expiry_date;
			END IF;

			IF @expiry_date < my_expiry_date THEN
				SET my_expiry_date = @expiry_date;
            END IF;
        END IF;
    END LOOP;

    IF DATEDIFF(my_end_date, my_expiry_date) > 56 THEN
        SET flag = 1;
    END IF;

	RETURN flag;
END */;;
DELIMITER ;

/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = utf8 */ ;
/*!50003 SET character_set_results = utf8 */ ;
/*!50003 SET collation_connection  = utf8_general_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = '' */ ;

DROP FUNCTION IF EXISTS `drug_pill_count`;
DELIMITER ;;
/*!50003 CREATE*/ /*!50020 */ /*!50003 FUNCTION `drug_pill_count`(my_patient_id INT, my_drug_id INT, my_date DATE) RETURNS DECIMAL
BEGIN
	DECLARE done INT DEFAULT FALSE;
	DECLARE my_pill_count, my_total_text, my_total_numeric DECIMAL;

	DECLARE cur1 CURSOR FOR SELECT SUM(ob.value_numeric), SUM(CAST(ob.value_text AS DECIMAL)) FROM obs ob
                        INNER JOIN drug_order do ON ob.order_id = do.order_id
                        INNER JOIN orders o ON do.order_id = o.order_id
                    WHERE ob.person_id = my_patient_id
                        AND ob.concept_id = 2540
                        AND ob.voided = 0
                        AND o.voided = 0
                        AND do.drug_inventory_id = my_drug_id
                        AND DATE(ob.obs_datetime) = my_date
                    GROUP BY ob.person_id;

	DECLARE CONTINUE HANDLER FOR NOT FOUND SET done = TRUE;

	OPEN cur1;

	SET my_pill_count = 0;

	read_loop: LOOP
		FETCH cur1 INTO my_total_numeric, my_total_text;

		IF done THEN
			CLOSE cur1;
			LEAVE read_loop;
		END IF;

        IF my_total_numeric IS NULL THEN
            SET my_total_numeric = 0;
        END IF;

        IF my_total_text IS NULL THEN
            SET my_total_text = 0;
        END IF;

        SET my_pill_count = my_total_numeric + my_total_text;
    END LOOP;

	RETURN my_pill_count;
END */;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = utf8 */ ;
/*!50003 SET character_set_results = utf8 */ ;
/*!50003 SET collation_connection  = utf8_general_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = '' */ ;

DROP FUNCTION IF EXISTS `current_state_for_program`;
DELIMITER ;;
/*!50003 CREATE*/ /*!50020 */ /*!50003 FUNCTION `current_state_for_program`(my_patient_id INT, my_program_id INT, my_end_date DATETIME) RETURNS int(11)
BEGIN
  SET @state_id = NULL;
  SET @new_state_id = NULL;
	SELECT  patient_program_id INTO @patient_program_id FROM patient_program
			WHERE patient_id = my_patient_id
				AND program_id = my_program_id
				AND voided = 0
				ORDER BY patient_program_id DESC LIMIT 1;


	SELECT state, start_date INTO @state_id, @start_date FROM patient_state
		WHERE patient_program_id = @patient_program_id
			AND voided = 0
			AND start_date <= my_end_date
		ORDER BY start_date DESC, date_created DESC, patient_state_id DESC LIMIT 1;

   IF ( @state_id != 3 ) THEN

      SELECT state INTO @new_state_id FROM patient_state
		   WHERE patient_program_id = @patient_program_id
			AND voided = 0
			AND start_date = @start_date
         AND state = 3 LIMIT 1;
   END IF;

    IF ( @new_state_id IS NOT NULL ) THEN
        RETURN @new_state_id;
    END IF;

	RETURN @state_id;
END */;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = utf8 */ ;
/*!50003 SET character_set_results = utf8 */ ;
/*!50003 SET collation_connection  = utf8_general_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = '' */ ;

DROP FUNCTION IF EXISTS `current_state_for_patient_in_flat_tables`;
DELIMITER ;;
/*!50003 CREATE*/ /*!50020 */ /*!50003 FUNCTION `current_state_for_patient_in_flat_tables`(my_patient_id INT, my_end_date DATETIME) RETURNS varchar(255)
BEGIN
  SET @state_id = NULL;
	SELECT current_hiv_program_state INTO @state_id FROM flat_table2
    WHERE current_hiv_program_state IS NOT NULL and current_hiv_program_start_date IS NOT NULL
      AND patient_id = my_patient_id
      AND current_hiv_program_start_date <= my_end_date
    ORDER BY patient_id, current_hiv_program_start_date DESC
    LIMIT 1;

	RETURN @state_id;
END */;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = utf8 */ ;
/*!50003 SET character_set_results = utf8 */ ;
/*!50003 SET collation_connection  = utf8_general_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = '' */ ;

DROP FUNCTION IF EXISTS `current_hiv_program_start_date_max`;
DELIMITER ;;
/*!50003 CREATE*/ /*!50020 */ /*!50003 FUNCTION `current_hiv_program_start_date_max`(my_patient_id INT, my_end_date DATETIME) RETURNS varchar(10) CHARSET latin1
    DETERMINISTIC
BEGIN
  SET @patient_id = NULL;
	SELECT max(ft3.current_hiv_program_start_date) INTO @patient_id FROM flat_table2 ft3
    WHERE ft3.patient_id = my_patient_id
	    AND ft3.current_hiv_program_start_date <= my_end_date
	    AND ft3.current_hiv_program_state = 'On antiretrovirals'
	    AND ft3.current_hiv_program_start_date IS NOT NULL;

	RETURN @patient_id;
END */;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = utf8 */ ;
/*!50003 SET character_set_results = utf8 */ ;
/*!50003 SET collation_connection  = utf8_general_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = '' */ ;

DROP FUNCTION IF EXISTS `last_text_for_obs`;
DELIMITER ;;
/*!50003 CREATE*/ /*!50020 */ /*!50003 FUNCTION `last_text_for_obs`(my_patient_id INT, my_encounter_type_id INT, my_concept_id INT, my_end_date DATETIME) RETURNS VARCHAR(255)
BEGIN
  SET @obs_value = NULL;
	SELECT o.encounter_id INTO @encounter_id FROM encounter e
			INNER JOIN obs o ON e.encounter_id = o.encounter_id AND o.concept_id = my_concept_id AND o.voided = 0
		WHERE e.encounter_type = my_encounter_type_id
			AND e.voided = 0
			AND e.patient_id = my_patient_id
			AND e.encounter_datetime <= my_end_date
		ORDER BY e.encounter_datetime DESC LIMIT 1;

	SELECT cn.name INTO @obs_value FROM obs o
			LEFT JOIN concept_name cn ON o.value_coded = cn.concept_id AND cn.concept_name_type = 'FULLY_SPECIFIED'
		WHERE encounter_id = @encounter_id
			AND o.voided = 0
			AND o.concept_id = my_concept_id
			AND o.voided = 0 LIMIT 1;

	IF @obs_value IS NULL THEN
		SELECT value_text INTO @obs_value FROM obs
			WHERE encounter_id = @encounter_id
				AND voided = 0
				AND concept_id = my_concept_id
				AND voided = 0 LIMIT 1;
	END IF;

	IF @obs_value IS NULL THEN
		SELECT value_numeric INTO @obs_value FROM obs
			WHERE encounter_id = @encounter_id
				AND voided = 0
				AND concept_id = my_concept_id
				AND voided = 0 LIMIT 1;
	END IF;

	RETURN @obs_value;
END */;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = utf8 */ ;
/*!50003 SET character_set_results = utf8 */ ;
/*!50003 SET collation_connection  = utf8_general_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = '' */ ;

DROP FUNCTION IF EXISTS `current_text_for_obs`;
DELIMITER ;;
/*!50003 CREATE*/ /*!50020 */ /*!50003 FUNCTION `current_text_for_obs`(my_patient_id INT, my_encounter_type_id INT, my_concept_id INT, my_end_date DATETIME) RETURNS VARCHAR(255)
BEGIN
  SET @obs_value = NULL;
	SELECT encounter_id INTO @encounter_id FROM encounter
		WHERE encounter_type = my_encounter_type_id
			AND voided = 0
			AND patient_id = my_patient_id
			AND encounter_datetime <= my_end_date
		ORDER BY encounter_datetime DESC LIMIT 1;

	SELECT cn.name INTO @obs_value FROM obs o
			LEFT JOIN concept_name cn ON o.value_coded = cn.concept_id AND cn.concept_name_type = 'FULLY_SPECIFIED'
		WHERE encounter_id = @encounter_id
			AND o.voided = 0
			AND o.concept_id = my_concept_id
			AND o.voided = 0 LIMIT 1;

	IF @obs_value IS NULL THEN
		SELECT value_text INTO @obs_value FROM obs
			WHERE encounter_id = @encounter_id
				AND voided = 0
				AND concept_id = my_concept_id
				AND voided = 0 LIMIT 1;
	END IF;

	IF @obs_value IS NULL THEN
		SELECT value_numeric INTO @obs_value FROM obs
			WHERE encounter_id = @encounter_id
				AND voided = 0
				AND concept_id = my_concept_id
				AND voided = 0 LIMIT 1;
	END IF;

	RETURN @obs_value;
END */;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = utf8 */ ;
/*!50003 SET character_set_results = utf8 */ ;
/*!50003 SET collation_connection  = utf8_general_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = '' */ ;

DROP FUNCTION IF EXISTS `current_value_for_obs`;
DELIMITER ;;
/*!50003 CREATE*/ /*!50020 */ /*!50003 FUNCTION `current_value_for_obs`(my_patient_id INT, my_encounter_type_id INT, my_concept_id INT, my_end_date DATETIME) RETURNS int(11)
BEGIN
  SET @obs_value_coded = NULL;
	SELECT encounter_id INTO @encounter_id FROM encounter
		WHERE encounter_type = my_encounter_type_id
			AND voided = 0
			AND patient_id = my_patient_id
			AND encounter_datetime <= my_end_date
		ORDER BY encounter_datetime DESC LIMIT 1;

	SELECT value_coded INTO @obs_value_coded FROM obs
			WHERE encounter_id = @encounter_id
				AND voided = 0
				AND concept_id = my_concept_id
				AND voided = 0 LIMIT 1;

	RETURN @obs_value_coded;
END */;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = utf8 */ ;
/*!50003 SET character_set_results = utf8 */ ;
/*!50003 SET collation_connection  = utf8_general_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = '' */ ;

DROP FUNCTION IF EXISTS `current_value_for_obs_at_initiation`;
DELIMITER ;;
/*!50003 CREATE*/ /*!50020 */ /*!50003 FUNCTION `current_value_for_obs_at_initiation`(my_patient_id INT, my_earliest_start_date DATETIME, my_encounter_type_id INT, my_concept_id INT, my_end_date DATETIME) RETURNS int(11)
BEGIN
	DECLARE obs_value_coded, my_encounter_id INT;

	SELECT encounter_id INTO my_encounter_id FROM encounter
		WHERE encounter_type = my_encounter_type_id
			AND voided = 0
			AND patient_id = my_patient_id
			AND encounter_datetime <= ADDDATE(DATE(my_earliest_start_date), 1)
		ORDER BY encounter_datetime DESC LIMIT 1;

	IF my_encounter_id IS NULL THEN
		SELECT encounter_id INTO my_encounter_id FROM encounter
			WHERE encounter_type = my_encounter_type_id
				AND voided = 0
				AND patient_id = my_patient_id
				AND encounter_datetime <= my_end_date
                AND encounter_datetime >= ADDDATE(DATE(my_earliest_start_date), 1)
			ORDER BY encounter_datetime LIMIT 1;
	END IF;

	SELECT value_coded INTO obs_value_coded FROM obs
			WHERE encounter_id = my_encounter_id
				AND voided = 0
				AND concept_id = my_concept_id
				AND voided = 0 LIMIT 1;

	RETURN obs_value_coded;
END */;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = utf8 */ ;
/*!50003 SET character_set_results = utf8 */ ;
/*!50003 SET collation_connection  = utf8_general_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = '' */ ;

DROP FUNCTION IF EXISTS `patient_start_date`;
DELIMITER ;;
/*!50003 CREATE*/ /*!50020 */ /*!50003 FUNCTION `patient_start_date`(patient_id int) RETURNS varchar(10) CHARSET latin1
    DETERMINISTIC
BEGIN
DECLARE start_date VARCHAR(10);
DECLARE dispension_concept_id INT;
DECLARE arv_concept INT;

set dispension_concept_id = (SELECT concept_id FROM concept_name WHERE name = 'AMOUNT DISPENSED');
set arv_concept = (SELECT concept_id FROM concept_name WHERE name = "ANTIRETROVIRAL DRUGS");

set start_date = (SELECT DATE(obs_datetime) FROM obs WHERE person_id = patient_id AND concept_id = dispension_concept_id AND value_drug IN (SELECT drug_id FROM drug d  WHERE d.concept_id IN (SELECT cs.concept_id FROM concept_set cs WHERE cs.concept_set = arv_concept)) ORDER BY obs_datetime DESC LIMIT 1);

RETURN start_date;
END */;;

DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;
/*!40103 SET TIME_ZONE=@OLD_TIME_ZONE */;

/*!40101 SET SQL_MODE=@OLD_SQL_MODE */;
/*!40014 SET FOREIGN_KEY_CHECKS=@OLD_FOREIGN_KEY_CHECKS */;
/*!40014 SET UNIQUE_CHECKS=@OLD_UNIQUE_CHECKS */;
/*!40111 SET SQL_NOTES=@OLD_SQL_NOTES */;


DROP FUNCTION IF EXISTS `current_defaulter_date`;
DELIMITER ;;
/*!50003 CREATE*/ /*!50020 */ /*!50003 FUNCTION `current_defaulter_date`(my_patient_id INT, my_end_date DATETIME) RETURNS DATE
BEGIN
	DECLARE done INT DEFAULT FALSE;
	DECLARE my_start_date, my_expiry_date, my_obs_datetime, my_defaulted_date DATETIME;
	DECLARE my_daily_dose, my_quantity, my_pill_count, my_total_text, my_total_numeric DECIMAL;
	DECLARE my_drug_id, flag INT;

	DECLARE cur1 CURSOR FOR SELECT d.drug_inventory_id, o.start_date, d.equivalent_daily_dose daily_dose, d.quantity, o.start_date FROM drug_order d
		INNER JOIN arv_drug ad ON d.drug_inventory_id = ad.drug_id
		INNER JOIN orders o ON d.order_id = o.order_id
			AND d.quantity > 0
			AND o.voided = 0
			AND o.start_date <= my_end_date
			AND o.patient_id = my_patient_id;

	DECLARE CONTINUE HANDLER FOR NOT FOUND SET done = TRUE;

	SELECT MAX(o.start_date) INTO @obs_datetime FROM drug_order d
		INNER JOIN arv_drug ad ON d.drug_inventory_id = ad.drug_id
		INNER JOIN orders o ON d.order_id = o.order_id
			AND d.quantity > 0
			AND o.voided = 0
			AND o.start_date <= my_end_date
			AND o.patient_id = my_patient_id
		GROUP BY o.patient_id;

	OPEN cur1;

	SET flag = 0;

	read_loop: LOOP
		FETCH cur1 INTO my_drug_id, my_start_date, my_daily_dose, my_quantity, my_obs_datetime;

		IF done THEN
			CLOSE cur1;
			LEAVE read_loop;
		END IF;

		IF DATE(my_obs_datetime) = DATE(@obs_datetime) THEN

            SET my_pill_count = drug_pill_count(my_patient_id, my_drug_id, my_obs_datetime);

            SET @expiry_date = ADDDATE(my_start_date, ((my_quantity + my_pill_count)/my_daily_dose));

			IF my_expiry_date IS NULL THEN
				SET my_expiry_date = @expiry_date;
			END IF;

			IF @expiry_date < my_expiry_date THEN
				SET my_expiry_date = @expiry_date;
            END IF;
        END IF;
    END LOOP;

    IF DATEDIFF(my_end_date, my_expiry_date) > 56 THEN
        SET my_defaulted_date = ADDDATE(my_expiry_date, 56);
    END IF;

	RETURN my_defaulted_date;
END */;;
DELIMITER ;

/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = utf8 */ ;
/*!50003 SET character_set_results = utf8 */ ;
/*!50003 SET collation_connection  = utf8_general_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = '' */ ;

DROP FUNCTION IF EXISTS `patient_max_defaulted_date`;
DELIMITER ;;
/*!50003 CREATE*/ /*!50020 */ /*!50003 FUNCTION `patient_max_defaulted_date`(m_patient_id int, my_end_date DATETIME) RETURNS DATE
BEGIN

DECLARE my_defaulted_date DATETIME;

set my_defaulted_date = (SELECT MAX(defaulted_date) FROM patient_defaulted_dates WHERE patient_id = m_patient_id AND start_date <= my_end_date);

RETURN my_defaulted_date;
END */;;

/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = utf8 */ ;
/*!50003 SET character_set_results = utf8 */ ;
/*!50003 SET collation_connection  = utf8_general_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = '' */ ;
-- Dump completed on 2012-05-03 21:13:17
