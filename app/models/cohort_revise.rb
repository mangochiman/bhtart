class CohortRevise



  def self.get_indicators(start_date, end_date)
  time_started = Time.now().strftime('%Y-%m-%d %H:%M:%S')
=begin
    ActiveRecord::Base.connection.execute <<EOF
      DROP TABLE IF EXISTS `temp_earliest_start_date`;
EOF


    ActiveRecord::Base.connection.execute <<EOF
      CREATE TABLE temp_earliest_start_date
select
        `p`.`patient_id` AS `patient_id`,
        `p`.`earliest_start_date` AS `earliest_start_date`,
        `p`.`death_date` AS `death_date`,
        `p`.`gender` AS `gender`,
        TRUNCATE(`p`.`age_at_initiation`,0) AS `age_at_initiation`,
        `p`.`age_in_days` AS `age_in_days`,
        cast(`pf`.`encounter_datetime` as date) AS `date_enrolled`
    from
        (`patients_on_arvs` `p`
        join `patient_first_arv_amount_dispensed` `pf` ON ((`pf`.`patient_id` = `p`.`patient_id`)))
    group by `p`.`patient_id`
EOF

=end
ActiveRecord::Base.connection.execute <<EOF
  DROP FUNCTION IF EXISTS `last_text_for_obs`;
EOF

    ActiveRecord::Base.connection.execute <<EOF
CREATE FUNCTION last_text_for_obs(my_patient_id INT, my_encounter_type_id INT, my_concept_id INT, my_regimem_given INT, unknown_regimen_value INT, my_end_date DATETIME) RETURNS varchar(255)

BEGIN
  SET @obs_value = NULL;
  SET @encounter_id = NULL;

  SELECT o.encounter_id INTO @encounter_id FROM encounter e
  	INNER JOIN obs o ON e.encounter_id = o.encounter_id AND o.concept_id IN (my_concept_id, @unknown_drug_concept_id) AND o.voided = 0
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
    SELECT 'unknown_drug_value' INTO @obs_value FROM obs
    WHERE encounter_id = @encounter_id
    AND voided = 0
    AND concept_id = my_regimem_given
    AND value_coded = unknown_regimen_value
    AND voided = 0 LIMIT 1;
  END IF;

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
END;
EOF


    ActiveRecord::Base.connection.execute <<EOF
      DROP FUNCTION IF EXISTS `current_defaulter`;
EOF

    ActiveRecord::Base.connection.execute <<EOF
CREATE FUNCTION `current_defaulter`(my_patient_id INT, my_end_date DATETIME) RETURNS int(1)
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
END;
EOF


    ActiveRecord::Base.connection.execute <<EOF
      DROP FUNCTION IF EXISTS `patient_outcome`;
EOF

    ActiveRecord::Base.connection.execute <<EOF
CREATE FUNCTION patient_outcome(patient_id INT, visit_date date) RETURNS varchar(25)
DETERMINISTIC
BEGIN
DECLARE set_program_id INT;
DECLARE set_patient_state INT;
DECLARE set_outcome varchar(25);

SET set_program_id = (SELECT program_id FROM program WHERE name ="HIV PROGRAM" LIMIT 1);

SET set_patient_state = (SELECT state FROM `patient_state` INNER JOIN patient_program p ON p.patient_program_id = patient_state.patient_program_id WHERE (patient_state.voided = 0 AND p.voided = 0 AND p.program_id = program_id AND start_date <= visit_date AND p.patient_id = patient_id) AND (patient_state.voided = 0) ORDER BY start_date DESC, patient_state.date_created DESC LIMIT 1);


IF set_patient_state = 1 THEN
  SET set_outcome = 'Pre-ART (Continue)';
END IF;

IF set_patient_state = 2   THEN
  SET set_outcome = 'Patient transferred out';
END IF;

IF set_patient_state = 3 THEN
  SET set_outcome = 'Patient died';
END IF;

IF set_patient_state = 6 THEN
  SET set_outcome = 'Treatment stopped';
END IF;

IF set_patient_state = 7 THEN
  SET set_patient_state = current_defaulter(patient_id, visit_date);

  IF set_patient_state = 1 THEN
    SET set_outcome = 'Defaulted';
  END IF;

  IF set_patient_state = 0 THEN
    SET set_outcome = 'On antiretrovirals';
  END IF;
END IF;

IF set_outcome IS NULL THEN
  SET set_patient_state = current_defaulter(patient_id, visit_date);

  IF set_patient_state = 1 THEN
    SET set_outcome = 'Defaulted';
  END IF;

  IF set_outcome IS NULL THEN
    SET set_outcome = 'On antiretrovirals';
  END IF;

END IF;

RETURN set_outcome;
END;
EOF


    ActiveRecord::Base.connection.execute <<EOF
      DROP FUNCTION IF EXISTS `re_initiated_check`;
EOF

    ActiveRecord::Base.connection.execute <<EOF
CREATE FUNCTION re_initiated_check(set_patient_id INT, set_date_enrolled DATE) RETURNS VARCHAR(15)
DETERMINISTIC
BEGIN
DECLARE re_initiated VARCHAR(15) DEFAULT 'N/A';
DECLARE check_one INT DEFAULT 0;
DECLARE check_two INT DEFAULT 0;

DECLARE yes_concept INT;
DECLARE no_concept INT;
DECLARE date_art_last_taken_concept INT;
DECLARE taken_arvs_concept INT;

set yes_concept = (SELECT concept_id FROM concept_name WHERE name ='YES' LIMIT 1);
set no_concept = (SELECT concept_id FROM concept_name WHERE name ='NO' LIMIT 1);
set date_art_last_taken_concept = (SELECT concept_id FROM concept_name WHERE name ='DATE ART LAST TAKEN' LIMIT 1);
set taken_arvs_concept = (SELECT concept_id FROM concept_name WHERE name ='HAS THE PATIENT TAKEN ART IN THE LAST TWO MONTHS' LIMIT 1);


set check_one = (SELECT esd.patient_id FROM temp_earliest_start_date esd INNER JOIN clinic_registration_encounter e ON esd.patient_id = e.patient_id INNER JOIN ever_registered_obs AS ero ON e.encounter_id = ero.encounter_id INNER JOIN obs o ON o.encounter_id = e.encounter_id AND o.concept_id = date_art_last_taken_concept AND o.voided = 0 WHERE ((o.concept_id = date_art_last_taken_concept AND (DATEDIFF(o.obs_datetime,o.value_datetime)) > 56)) AND esd.date_enrolled = set_date_enrolled AND esd.patient_id = set_patient_id GROUP BY esd.patient_id);

set check_two = (SELECT esd.patient_id FROM temp_earliest_start_date esd INNER JOIN clinic_registration_encounter e ON esd.patient_id = e.patient_id INNER JOIN ever_registered_obs AS ero ON e.encounter_id = ero.encounter_id INNER JOIN obs o ON o.encounter_id = e.encounter_id AND o.concept_id = taken_arvs_concept AND o.voided = 0 WHERE  ((o.concept_id = taken_arvs_concept AND o.value_coded = no_concept)) AND esd.date_enrolled = set_date_enrolled AND esd.patient_id = set_patient_id GROUP BY esd.patient_id);

if check_one >= 1 then set re_initiated ="Re-initiated";
elseif check_two >= 1 then set re_initiated ="Re-initiated";
end if;


RETURN re_initiated;
END;
EOF

    ActiveRecord::Base.connection.execute <<EOF
      DROP FUNCTION IF EXISTS `died_in`;
EOF

    ActiveRecord::Base.connection.execute <<EOF
CREATE FUNCTION died_in(set_patient_id INT, set_status VARCHAR(25), date_enrolled DATE) RETURNS varchar(25)
DETERMINISTIC
BEGIN
DECLARE set_outcome varchar(25) default 'N/A';
DECLARE date_of_death DATE;
DECLARE num_of_months INT;

IF set_status = 'Patient died' THEN

  SET date_of_death = (SELECT death_date FROM temp_earliest_start_date WHERE patient_id = set_patient_id);

  IF date_of_death IS NULL THEN
    RETURN 'Unknown';
  END IF;


  set num_of_months = (TIMESTAMPDIFF(month, date(date_enrolled), date(date_of_death)));

  IF num_of_months < 2 THEN set set_outcome ="1st month";
  ELSEIF num_of_months = 2 THEN set set_outcome ="2nd month";
  ELSEIF num_of_months = 3 THEN set set_outcome ="3rd month";
  ELSEIF num_of_months > 3 THEN set set_outcome ="4+ months";
  END IF;


END IF;

RETURN set_outcome;
END;
EOF

#=end
      #Get earliest date enrolled
      cum_start_date = self.get_cum_start_date
      cohort = CohortService.new(cum_start_date)

      #Total registered
      cohort.total_registered = self.total_registered(start_date, end_date)
      cohort.cum_total_registered = self.total_registered(cum_start_date, end_date)

      #Patients initiated on ART first time
      cohort.initiated_on_art_first_time = self.initiated_on_art_first_time(start_date, end_date)
      cohort.cum_initiated_on_art_first_time = self.initiated_on_art_first_time(cum_start_date, end_date)

      #Patients re-initiated on ART
      cohort.re_initiated_on_art = self.re_initiated_on_art(start_date, end_date)
      cohort.cum_re_initiated_on_art = self.re_initiated_on_art(cum_start_date, end_date)

      #Patients transferred in on ART
      cohort.transfer_in = self.transfer_in(start_date, end_date)
      cohort.cum_transfer_in = self.transfer_in(cum_start_date, end_date)


      #All males
      cohort.all_males = self.males(start_date, end_date)
      cohort.cum_all_males = self.males(cum_start_date, end_date)

      #Pregnant females (all ages)
=begin
Unique PatientProgram entries at the current location for those patients with at least one state ON ARVs and earliest start date of the 'ON ARVs' state within the quarter and having gender of related PERSON entry as F for female and 'IS PATIENT PREGNANT?' observation answered 'YES' in related HIV CLINIC CONSULTATION encounters within 28 days from earliest registration date OR in HIV Staging encounters
=end
      cohort.pregnant_females_all_ages = self.pregnant_females_all_ages(start_date, end_date)
      cohort.cum_pregnant_females_all_ages = self.pregnant_females_all_ages(cum_start_date, end_date)

      #Non-pregnant females (all ages)
=begin
      Unique PatientProgram entries at the current location for those patients with at least one state ON ARVs
      and earliest start date of the 'ON ARVs' state within the quarter and having gender of
      related PERSON entry as F for female and no entries of 'IS PATIENT PREGNANT?' observation answered 'YES'
      in related HIV CLINIC CONSULTATION encounters not within 28 days from earliest registration date
=end
      cohort.non_pregnant_females = self.non_pregnant_females(start_date, end_date, cohort.pregnant_females_all_ages)
      cohort.cum_non_pregnant_females = self.non_pregnant_females(cum_start_date, end_date, cohort.cum_pregnant_females_all_ages)

      #Children below 24 months at ART initiation
      cohort.children_below_24_months_at_art_initiation = self.children_below_24_months_at_art_initiation(start_date, end_date)
      cohort.cum_children_below_24_months_at_art_initiation = self.children_below_24_months_at_art_initiation(cum_start_date, end_date)

      #Children 24 months – 14 years at ART initiation
      cohort.children_24_months_14_years_at_art_initiation = self.children_24_months_14_years_at_art_initiation(start_date, end_date)
      cohort.cum_children_24_months_14_years_at_art_initiation = self.children_24_months_14_years_at_art_initiation(cum_start_date, end_date)

      #Adults at ART initiation
      cohort.adults_at_art_initiation = self.adults_at_art_initiation(start_date, end_date)
      cohort.cum_adults_at_art_initiation = self.adults_at_art_initiation(cum_start_date, end_date)

      #Unknown age
      cohort.unknown_age = self.unknown_age(start_date, end_date)
      cohort.cum_unknown_age = self.unknown_age(cum_start_date, end_date)

=begin
      Unique PatientProgram entries at the current location for those patients with at least one state ON ARVs
      and earliest start date of the 'ON ARVs' state within the quarter
      and having a REASON FOR ELIGIBILITY observation with an answer as PRESUMED SEVERE HIV
=end
      cohort.presumed_severe_hiv_disease_in_infants = self.presumed_severe_hiv_disease_in_infants(start_date, end_date)
      cohort.cum_presumed_severe_hiv_disease_in_infants = self.presumed_severe_hiv_disease_in_infants(cum_start_date, end_date)

=begin
      Confirmed HIV infection in infants (PCR)

      Unique PatientProgram entries at the current location for those patients with at least one state ON ARVs
      and earliest start date of the 'ON ARVs' state within the quarter and
      having a REASON FOR ELIGIBILITY observation with an answer as HIV PCR
=end
      cohort.confirmed_hiv_infection_in_infants_pcr = self.confirmed_hiv_infection_in_infants_pcr(start_date, end_date)
      cohort.cum_confirmed_hiv_infection_in_infants_pcr = self.confirmed_hiv_infection_in_infants_pcr(cum_start_date, end_date)

=begin
      WHO stage 1 or 2, CD4 below threshold
      Unique PatientProgram entries at the current location for those patients with at least one state ON ARVs
      and earliest start date of the 'ON ARVs' state within the quarter and having a REASON FOR ELIGIBILITY
      observation with an answer as CD4 COUNT LESS THAN OR EQUAL TO 350 or CD4 COUNT LESS THAN OR EQUAL TO 750
=end
      cohort.who_stage_two = self.who_stage_two(start_date, end_date)
      cohort.cum_who_stage_two = self.who_stage_two(cum_start_date, end_date)


=begin
    Breastfeeding mothers

    Unique PatientProgram entries at the current location for those patients with at least one state
    ON ARVs and earliest start date of the 'ON ARVs' state within the quarter
    and having a REASON FOR ELIGIBILITY observation with an answer as BREASTFEEDING
=end
    cohort.breastfeeding_mothers = self.breastfeeding_mothers(start_date, end_date)
    cohort.cum_breastfeeding_mothers = self.breastfeeding_mothers(cum_start_date, end_date)

=begin
  Pregnant women

  Unique PatientProgram entries at the current location for those patients with at least one state ON ARVs
  and earliest start date of the 'ON ARVs' state within the quarter
  and having a REASON FOR ELIGIBILITY observation with an answer as PATIENT PREGNANT
=end
    cohort.pregnant_women = self.pregnant_women(start_date, end_date)
    cohort.cum_pregnant_women = self.pregnant_women(cum_start_date, end_date)

=begin
  WHO STAGE 3
  Unique PatientProgram entries at the current location for those patients with at least
  one state ON ARVs and earliest start date of the 'ON ARVs' state within the quarter
  and having a REASON FOR ELIGIBILITY observation with an answer as WHO STAGE III
=end
    cohort.who_stage_three = self.who_stage_three(start_date, end_date)
    cohort.cum_who_stage_three = self.who_stage_three(cum_start_date, end_date)

=begin
  WHO STAGE 4
  Unique PatientProgram entries at the current location for those patients with at least
  one state ON ARVs and earliest start date of the 'ON ARVs' state within the quarter
  and having a REASON FOR ELIGIBILITY observation with an answer as WHO STAGE IV
=end
    cohort.who_stage_four = self.who_stage_four(start_date, end_date)
    cohort.cum_who_stage_four = self.who_stage_four(cum_start_date, end_date)

=begin
    Unknown / other reason outside guidelines
    Unique PatientProgram entries at the current location for those patients with at least one state ON ARVs
    and earliest start date of the 'ON ARVs' state within the quarter
    and having a REASON FOR ELIGIBILITY observation with an answer as UNKNOWN
=end
    cohort.unknown_other_reason_outside_guidelines = self.unknown_other_reason_outside_guidelines(start_date, end_date)
    cohort.cum_unknown_other_reason_outside_guidelines = self.unknown_other_reason_outside_guidelines(cum_start_date, end_date)

=begin
   Children 12-23 months

   Unique PatientProgram entries at the current location for those patients with at least one state
   ON ARVs and earliest start date of the 'ON ARVs' state within the quarter and having
   Confirmed HIV Infection (HIV Rapid antibody test or DNA-PCR), regardless of WHO stage and CD4 Count
=end
    cohort.children_12_23_months = self.children_12_23_months(start_date, end_date)
    cohort.cum_children_12_23_months = self.children_12_23_months(cum_start_date, end_date)

=begin
    TB within the last 2 years

    Unique PatientProgram entries at the current location for those patients with at least one state ON ARVs
    and earliest start date of the 'ON ARVs' state within the quarter
    and having a TB WITHIN THE LAST 2 YEARS observation at the HIV staging encounter on the initiation date
=end
    cohort.tb_within_the_last_two_years = self.tb_within_the_last_two_years(start_date, end_date)
    cohort.cum_tb_within_the_last_two_years = self.tb_within_the_last_two_years(cum_start_date, end_date)

=begin
    Current EPISODE OF TB

    Unique PatientProgram entries at the current location for those patients with at least one state
    ON ARVs and earliest start date of the 'ON ARVs' state within the quarter and having a
    CURRENT EPISODE OF TB observation at the HIV staging encounter on the initiation date
=end

    cohort.current_episode_of_tb = self.current_episode_of_tb(start_date, end_date)
    cohort.cum_current_episode_of_tb = self.current_episode_of_tb(cum_start_date, end_date)

=begin
    Kaposis Sarcoma

    Unique PatientProgram entries at the current location for those patients with at least one state ON ARVs
    and earliest start date of the 'ON ARVs' state within the quarter and having a KAPOSIS SARCOMA observation
    at the HIV staging encounter on the initiation date
=end
    cohort.kaposis_sarcoma = self.kaposis_sarcoma(start_date, end_date)
    cohort.cum_kaposis_sarcoma = self.kaposis_sarcoma(cum_start_date, end_date)

=begin
    From this point going down: we update temp_earliest_start_date cum_outcome field to have the latest Cumulative outcome
=end
    self.update_cum_outcome(end_date)


=begin
    Total Alive and On ART
    Unique PatientProgram entries at the current location for those patients with at least one state
    ON ARVs and earliest start date of the 'ON ARVs' state less than or equal to end date of quarter
    and latest state is ON ARVs  (Excluding defaulters)
=end
    cohort.total_alive_and_on_art                      = self.get_outcome('On antiretrovirals')
    cohort.died_within_the_1st_month_of_art_initiation = self.died_in('1st month')
    cohort.died_within_the_2nd_month_of_art_initiation = self.died_in('2nd month')
    cohort.died_within_the_3rd_month_of_art_initiation = self.died_in('3rd month')
    cohort.died_after_the_3rd_month_of_art_initiation  = self.died_in('4+ months')
    cohort.died_total                                  = self.get_outcome('Patient died')
    cohort.defaulted                                   = self.get_outcome('Defaulted')
    cohort.stopped_art                                 = self.get_outcome('Treatment stopped')
    cohort.transfered_out                              = self.get_outcome('Patient transferred out')
    cohort.unknown_outcome                             = self.get_outcome('Pre-ART (Continue)')

=begin
    ARV Regimen category
    Alive and On ART and Value Coded of the latest 'Regimen Category' Observation
    of each patient that is linked to the Dispensing encounter in the reporting period
=end

    @@regimen_categories = self.cal_regimem_category(cohort.total_alive_and_on_art, end_date)

    cohort.zero_a           = self.get_regimen_category('0A')
    cohort.one_a            = self.get_regimen_category('1A')
    cohort.zero_p           = self.get_regimen_category('0P')
    cohort.one_p            = self.get_regimen_category('1P')
    cohort.two_a            = self.get_regimen_category('2A')
    cohort.two_p            = self.get_regimen_category('2P')
    cohort.three_a          = self.get_regimen_category('3A')
    cohort.three_p          = self.get_regimen_category('3P')
    cohort.four_a           = self.get_regimen_category('4A')
    cohort.four_p           = self.get_regimen_category('4P')
    cohort.five_a           = self.get_regimen_category('5A')
    cohort.six_a            = self.get_regimen_category('6A')
    cohort.seven_a          = self.get_regimen_category('7A')
    cohort.eight_a          = self.get_regimen_category('8A')
    cohort.nine_p           = self.get_regimen_category('9P')
    cohort.unknown_regimen  = self.get_regimen_category('unknown_regimen')

=begin
    Total patients with side effects:
    Alive and On ART patients with DRUG INDUCED observations during their last HIV CLINIC CONSULTATION encounter up to the reporting period
=end
    cohort.total_patients_with_side_effects = self.total_patients_with_side_effects(cohort.total_alive_and_on_art, end_date)
    cohort.total_patients_without_side_effects = self.total_patients_without_side_effects(cohort.total_alive_and_on_art, end_date)


=begin
    TB Status
    Alive and On ART with 'TB Status' observation value of 'TB not Suspected' or 'TB Suspected'
    or 'TB confirmed and on Treatment', or 'TB confirmed and not on Treatment' or 'Unknown TB status'
    during their latest HIV Clinic Consultaiton encounter in the reporting period
=end
    @@tb_status = self.cal_tb_status(cohort.total_alive_and_on_art, end_date)

    cohort.tb_suspected = self.get_tb_status('TB suspected')
    cohort.tb_not_suspected = self.get_tb_status('TB NOT suspected')
    cohort.tb_confirmed_on_tb_treatment = self.get_tb_status('Confirmed TB on treatment')
    cohort.tb_confirmed_currently_not_yet_on_tb_treatment = self.get_tb_status('Confirmed TB NOT on treatment')
    cohort.unknown_tb_status = self.get_tb_status('unknown_tb_status')

    runtime = "Started at: #{time_started}. Finished at: #{Time.now().strftime('%Y-%m-%d %H:%M:%S')}"
    return [cohort, runtime]
  end


  private
  def self.cal_tb_status(patient_list, end_date)
    patient_ids = []
    tb_status = []

    (patient_list || []).each do |row|
      patient_ids << row['patient_id'].to_i
    end

    return [] if patient_ids.blank?

    tb_status_concept_id = ConceptName.find_by_name('TB STATUS').concept_id

    data = ActiveRecord::Base.connection.select_all <<EOF
      SELECT person_id, value_coded, value_coded_name_id,  cn.name as tb_status
      FROM obs o
       LEFT JOIN concept_name cn ON o.value_coded = cn.concept_id AND cn.concept_name_type = 'FULLY_SPECIFIED'
      WHERE o.voided = 0 AND o.concept_id = #{tb_status_concept_id}
      AND o.person_id IN(#{patient_ids.join(',')}) AND
      o.obs_datetime <= '#{end_date.to_date.strftime('%Y-%m-%d 23:59:59')}'
      AND o.obs_datetime = (
        SELECT max(obs_datetime) FROM obs WHERE concept_id = #{tb_status_concept_id}
        AND voided = 0 AND person_id = o.person_id AND
        obs_datetime <= '#{end_date.to_date.strftime('%Y-%m-%d 23:59:59')}'
      ) GROUP BY person_id
EOF
    (data || []).each do |patient_tb_status|
      status = patient_tb_status['tb_status']
      status = 'unknown_tb_status' if status.blank?
      tb_status << {
        :patient_id => patient_tb_status['person_id'].to_i,
        :tb_status => status
      }
    end
    return tb_status
  end

  def self.get_tb_status(tb_status)
    registered = []
      (@@tb_status || []).each do |status|
        if tb_status == status[:tb_status]
          registered << {:patient_id => status[:patient_id], :tb_status => status[:tb_status]}
        end
      end

      return registered
  end

  def self.total_patients_with_side_effects(data, start_date, end_date)
    patient_ids = []
    (data || []).each do |row|
      patient_ids << row['patient_id'].to_i
    end

    return [] if patient_ids.blank?
    result = []

  	drug_induced_concept_id = ConceptName.find_by_name('Drug induced').concept_id
    malawi_art_side_effects_concept_id = ConceptName.find_by_name('Malawi ART side effects').concept_id
    no_side_effects_concept_id = ConceptName.find_by_name('No').concept_id

    malawi_art_side_effects =  ActiveRecord::Base.connection.select_all <<EOF
            SELECT * FROM obs o WHERE o.voided = 0 AND o.concept_id IN (#{malawi_art_side_effects_concept_id})
            AND o.person_id IN (#{patient_ids.join(',')})
            AND o.value_coded != #{no_side_effects_concept_id}
            AND o.obs_datetime <= '#{end_date.to_date.strftime('%Y-%m-%d 23:59:59')}'
            AND o.obs_datetime = (
              SELECT max(obs_datetime) FROM obs WHERE concept_id IN (#{malawi_art_side_effects_concept_id})
              AND value_coded != #{no_side_effects_concept_id}
              AND voided = 0 AND person_id = o.person_id
              AND obs_datetime BETWEEN  '#{start_date}' AND '#{end_date.to_date.strftime('%Y-%m-%d 23:59:59')}'
            ) GROUP BY person_id
EOF

    data = ActiveRecord::Base.connection.select_all <<EOF
        SELECT * FROM obs o WHERE o.voided = 0 AND o.concept_id = #{drug_induced_concept_id}
        AND o.person_id IN (#{patient_ids.join(',')}) AND
        o.obs_datetime <= '#{end_date.to_date.strftime('%Y-%m-%d 23:59:59')}'
        AND o.obs_datetime = (
          SELECT max(obs_datetime) FROM obs WHERE concept_id = #{drug_induced_concept_id}
          AND voided = 0 AND person_id = o.person_id
          AND obs_datetime BETWEEN '#{start_date}' AND '#{end_date.to_date.strftime('%Y-%m-%d 23:59:59')}'
        ) GROUP BY person_id
EOF
    (data || []).each do |row|
  	   result << row['person_id'].to_i
    end

    (malawi_art_side_effects || []).each do |row|
      result << row['person_id'].to_i
    end
     return result.uniq if !result.blank?
  end

  def self.total_patients_without_side_effects(data, start_date, end_date)
    patient_ids = []; drug_induced_ids = []
    (data || []).each do |row|
      patient_ids << row['patient_id'].to_i
    end

    return [] if patient_ids.blank?
    result = []

  	drug_induced_concept_id = ConceptName.find_by_name('Drug induced').concept_id
    malawi_art_side_effects_concept_id = ConceptName.find_by_name('Malawi ART side effects').concept_id
    no_side_effects_concept_id = ConceptName.find_by_name('No').concept_id
    symptom_present_concept_id = ConceptName.find_by_name('Symptom present').concept_id

    malawi_art_side_effects =  ActiveRecord::Base.connection.select_all <<EOF
            SELECT * FROM obs o WHERE o.voided = 0 AND o.concept_id IN (#{malawi_art_side_effects_concept_id})
            AND o.person_id IN (#{patient_ids.join(',')})
            AND o.value_coded = #{no_side_effects_concept_id}
            AND o.obs_datetime <= '#{end_date.to_date.strftime('%Y-%m-%d 23:59:59')}'
            AND o.obs_datetime = (
              SELECT max(obs_datetime) FROM obs WHERE concept_id IN (#{malawi_art_side_effects_concept_id})
              AND value_coded = #{no_side_effects_concept_id}
              AND voided = 0 AND person_id = o.person_id
              AND obs_datetime BETWEEN  '#{start_date}' AND '#{end_date.to_date.strftime('%Y-%m-%d 23:59:59')}'
            ) GROUP BY person_id
EOF
  drug_induced = ActiveRecord::Base.connection.select_all <<EOF
        SELECT * FROM obs o WHERE o.voided = 0 AND o.concept_id = #{drug_induced_concept_id}
        AND o.person_id IN (#{patient_ids.join(',')}) AND
        o.obs_datetime <= '#{end_date.to_date.strftime('%Y-%m-%d 23:59:59')}'
        AND o.obs_datetime = (
          SELECT max(obs_datetime) FROM obs WHERE concept_id = #{drug_induced_concept_id}
          AND voided = 0 AND person_id = o.person_id
          AND obs_datetime BETWEEN '#{start_date}' AND '#{end_date.to_date.strftime('%Y-%m-%d 23:59:59')}'
        ) GROUP BY person_id
EOF

    (drug_induced || []).each do |row|
      drug_induced_ids << row['person_id']
    end
    drug_induced_ids = [0] if drug_induced_ids.blank?

    patients = ActiveRecord::Base.connection.select_all <<EOF
        SELECT * FROM obs o WHERE o.voided = 0 AND o.concept_id = #{symptom_present_concept_id}
        AND (o.person_id IN (#{patient_ids.join(',')}) AND
             o.person_id NOT IN (#{drug_induced_ids.join(',')}))
		AND o.obs_datetime <= '#{end_date.to_date.strftime('%Y-%m-%d 23:59:59')}'
        AND o.obs_datetime = (
          SELECT max(obs_datetime) FROM obs WHERE concept_id = #{drug_induced_concept_id}
          AND voided = 0 AND person_id = o.person_id
          AND obs_datetime BETWEEN '#{start_date}' AND '#{end_date.to_date.strftime('%Y-%m-%d 23:59:59')}'
        ) GROUP BY person_id
EOF

    (patients || []).each do |row|
  	   result << row['person_id'].to_i
    end

    (malawi_art_side_effects || []).each do |row|
      result << row['person_id'].to_i
    end
     return result
  end

  def self.cal_regimem_category(patient_list, end_date)
    regimens = []

    patient_ids = []

    (patient_list || []).each do |row|
      patient_ids << row['patient_id'].to_i
    end

    return [] if patient_ids.blank?

    dispensing_encounter_id = EncounterType.find_by_name("DISPENSING").id
    regimen_category = ConceptName.find_by_name("REGIMEN CATEGORY").concept_id
    regimem_given_concept = ConceptName.find_by_name('ARV REGIMENS RECEIVED ABSTRACTED CONSTRUCT').concept_id
    unknown_regimen_given = ConceptName.find_by_name('UNKNOWN ANTIRETROVIRAL DRUG').concept_id

    data = ActiveRecord::Base.connection.select_all <<EOF
      SELECT t.patient_id,
      last_text_for_obs(t.patient_id, #{dispensing_encounter_id}, #{regimen_category}, #{regimem_given_concept}, #{unknown_regimen_given}, '#{end_date}') regimen_category
      FROM temp_earliest_start_date t
      WHERE t.patient_id IN (#{patient_ids.join(', ')}) GROUP BY patient_id;
EOF
    (data || []).each do |regimen_attr|
        regimen = regimen_attr['regimen_category']
        regimen = 'unknown_regimen' if regimen.blank?
        regimens << {
          :patient_id => regimen_attr['patient_id'].to_i,
          :regimen_category => regimen
        }
      end

      return regimens
  end

  def self.get_regimen_category(arv_regimen_category)
    registered = []
      (@@regimen_categories || []).each do |regimen_attr|
        if arv_regimen_category == regimen_attr[:regimen_category]
          registered << {:patient_id => regimen_attr[:patient_id], :regimen => regimen_attr[:regimen_category]}
        end
      end

      return registered
  end

  def self.died_in(month_str)
    registered = []
    data = ActiveRecord::Base.connection.select_all <<EOF
      SELECT died_in(t.patient_id, cum_outcome, date_enrolled) died_in FROM temp_patient_outcomes o
      INNER JOIN temp_earliest_start_date t USING(patient_id)
      WHERE cum_outcome = 'Patient died' GROUP BY patient_id
      HAVING died_in = '#{month_str}';
EOF


    (data || []).each do |patient|
      registered << patient
    end

    return registered
  end

  def self.get_outcome(outcome)
    registered = []
    total_alive_and_on_art = ActiveRecord::Base.connection.select_all <<EOF
      SELECT * FROM temp_patient_outcomes
      WHERE cum_outcome = '#{outcome}' GROUP BY patient_id;
EOF


    (total_alive_and_on_art || []).each do |patient|
      registered << patient
    end

    return registered
  end

  def self.update_cum_outcome(end_date)

      ActiveRecord::Base.connection.execute <<EOF
        DROP TABLE IF EXISTS `temp_patient_outcomes`;
EOF

      ActiveRecord::Base.connection.execute <<EOF
        CREATE TABLE temp_patient_outcomes
          SELECT patient_id, patient_outcome(patient_id, '#{end_date}') cum_outcome
        FROM temp_earliest_start_date
        WHERE date_enrolled <= '#{end_date}';
EOF

  end

  def self.kaposis_sarcoma(start_date, end_date)
    #KAPOSIS SARCOMA
    concept_id = ConceptName.find_by_name('KAPOSIS SARCOMA').concept_id
    who_stages_criteria = ConceptName.find_by_name('Who stages criteria present').concept_id
    registered = []

    total_registered = ActiveRecord::Base.connection.select_all <<EOF
      SELECT * FROM temp_earliest_start_date t
      INNER JOIN obs ON t.patient_id = obs.person_id
      WHERE date_enrolled BETWEEN '#{start_date}' AND '#{end_date}'
      AND (value_coded = #{concept_id}) AND concept_id = #{who_stages_criteria}
      AND voided = 0 AND DATE(obs_datetime) <= DATE(date_enrolled) GROUP BY patient_id;
EOF

    (total_registered || []).each do |patient|
      registered << patient
    end

  end

  def self.current_episode_of_tb(start_date, end_date)
    #CURRENT EPISODE OF TB
    eptb_concept_id = ConceptName.find_by_name('EXTRAPULMONARY TUBERCULOSIS (EPTB)').concept_id
    pulmonary_tb_concept_id = ConceptName.find_by_name('PULMONARY TUBERCULOSIS').concept_id
    current_ptb_concept_id = ConceptName.find_by_name('PULMONARY TUBERCULOSIS (CURRENT)').concept_id

    who_stages_criteria = ConceptName.find_by_name('Who stages criteria present').concept_id
    registered = []

    total_registered = ActiveRecord::Base.connection.select_all <<EOF
      SELECT * FROM temp_earliest_start_date t
      INNER JOIN obs ON t.patient_id = obs.person_id
      WHERE date_enrolled BETWEEN '#{start_date}' AND '#{end_date}'
      AND (value_coded IN (#{eptb_concept_id}, #{pulmonary_tb_concept_id}, #{current_ptb_concept_id}) AND concept_id = #{who_stages_criteria})
      AND voided = 0 AND DATE(obs_datetime) <= DATE(date_enrolled) GROUP BY patient_id;
EOF

    (total_registered || []).each do |patient|
      registered << patient
    end
  end

  def self.tb_within_the_last_two_years(start_date, end_date)
    #Pulmonary tuberculosis within the last 2 years
    pulmonary_tb_within_last_2yrs_concept_id = ConceptName.find_by_name('Pulmonary tuberculosis within the last 2 years').concept_id
    ptb_within_the_past_two_yrs_concept_id = ConceptName.find_by_name('Ptb within the past two years').concept_id
    who_stages_criteria = ConceptName.find_by_name('Who stages criteria present').concept_id
    registered = []

    total_registered = ActiveRecord::Base.connection.select_all <<EOF
      SELECT * FROM temp_earliest_start_date t
      INNER JOIN obs ON t.patient_id = obs.person_id
      WHERE date_enrolled BETWEEN '#{start_date}' AND '#{end_date}'
      AND (value_coded IN (#{pulmonary_tb_within_last_2yrs_concept_id}, #{ptb_within_the_past_two_yrs_concept_id})) AND concept_id = #{who_stages_criteria}
      AND voided = 0 AND DATE(obs_datetime) <= DATE(date_enrolled) GROUP BY patient_id;
EOF

    (total_registered || []).each do |patient|
      registered << patient
    end
  end

  def self.children_12_23_months(start_date, end_date)
    reason_concept_id = ConceptName.find_by_name('HIV DNA POLYMERASE CHAIN REACTION').concept_id

    registered = []
    total_registered = ActiveRecord::Base.connection.select_all <<EOF
      SELECT * FROM temp_earliest_start_date t
      INNER JOIN obs ON t.patient_id = obs.person_id
      WHERE date_enrolled BETWEEN '#{start_date}' AND '#{end_date}'
      AND (value_coded = #{reason_concept_id}) AND voided = 0 GROUP BY patient_id;
EOF

    (total_registered || []).each do |patient|
      registered << patient
    end

  end

  def self.unknown_other_reason_outside_guidelines(start_date, end_date)
    reason_for_art = ConceptName.find_by_name('REASON FOR ART ELIGIBILITY').concept_id
    reason_concept_id = ConceptName.find_by_name('Unknown').concept_id

    registered = []
    total_registered = ActiveRecord::Base.connection.select_all <<EOF
      SELECT * FROM temp_earliest_start_date t
      INNER JOIN obs ON t.patient_id = obs.person_id
      WHERE date_enrolled BETWEEN '#{start_date}' AND '#{end_date}'
      AND concept_id = #{reason_for_art} AND (value_coded = #{reason_concept_id})
      AND voided = 0 GROUP BY patient_id;
EOF

    (total_registered || []).each do |patient|
      registered << patient
    end

  end

  def self.who_stage_four(start_date, end_date)
    reason_for_art = ConceptName.find_by_name('REASON FOR ART ELIGIBILITY').concept_id
    reason_concept_id = ConceptName.find_by_name('WHO stage IV adult').concept_id
    reason2_concept_id = ConceptName.find_by_name('WHO stage IV peds').concept_id
    reason3_concept_id = ConceptName.find_by_name('WHO STAGE 4').concept_id

    registered = []
    total_registered = ActiveRecord::Base.connection.select_all <<EOF
      SELECT * FROM temp_earliest_start_date t
      INNER JOIN obs ON t.patient_id = obs.person_id
      WHERE date_enrolled BETWEEN '#{start_date}' AND '#{end_date}'
      AND concept_id = #{reason_for_art} AND (value_coded = #{reason_concept_id}
      OR value_coded = #{reason2_concept_id} OR value_coded = #{reason3_concept_id})
      AND voided = 0 GROUP BY patient_id;
EOF

    (total_registered || []).each do |patient|
      registered << patient
    end

  end

  def self.who_stage_three(start_date, end_date)
    reason_for_art = ConceptName.find_by_name('REASON FOR ART ELIGIBILITY').concept_id
    reason_concept_id = ConceptName.find_by_name('WHO stage III adult').concept_id
    reason2_concept_id = ConceptName.find_by_name('WHO stage III peds').concept_id
    reason3_concept_id = ConceptName.find_by_name('WHO STAGE 3').concept_id

    registered = []
    total_registered = ActiveRecord::Base.connection.select_all <<EOF
      SELECT * FROM temp_earliest_start_date t
      INNER JOIN obs ON t.patient_id = obs.person_id
      WHERE date_enrolled BETWEEN '#{start_date}' AND '#{end_date}'
      AND concept_id = #{reason_for_art} AND (value_coded = #{reason_concept_id}
      OR value_coded = #{reason2_concept_id} OR value_coded = #{reason3_concept_id})
      AND voided = 0 GROUP BY patient_id;
EOF

    (total_registered || []).each do |patient|
      registered << patient
    end

  end

  def self.pregnant_women(start_date, end_date)
    reason_for_art = ConceptName.find_by_name('REASON FOR ART ELIGIBILITY').concept_id
    reason_concept_id = ConceptName.find_by_name('PATIENT PREGNANT').concept_id

    registered = []
    total_registered = ActiveRecord::Base.connection.select_all <<EOF
      SELECT * FROM temp_earliest_start_date t
      INNER JOIN obs ON t.patient_id = obs.person_id
      WHERE date_enrolled BETWEEN '#{start_date}' AND '#{end_date}'
      AND concept_id = #{reason_for_art} AND value_coded = #{reason_concept_id}
      AND voided = 0 GROUP BY patient_id;
EOF

    (total_registered || []).each do |patient|
      registered << patient
    end

  end

  def self.breastfeeding_mothers(start_date, end_date)
    reason_for_art = ConceptName.find_by_name('REASON FOR ART ELIGIBILITY').concept_id
    reason_concept_id = ConceptName.find_by_name('BREASTFEEDING').concept_id

    registered = []
    total_registered = ActiveRecord::Base.connection.select_all <<EOF
      SELECT * FROM temp_earliest_start_date t
      INNER JOIN obs ON t.patient_id = obs.person_id
      WHERE date_enrolled BETWEEN '#{start_date}' AND '#{end_date}'
      AND concept_id = #{reason_for_art} AND value_coded = #{reason_concept_id}
      AND voided = 0 GROUP BY patient_id;
EOF

    (total_registered || []).each do |patient|
      registered << patient
    end

  end

  def self.who_stage_two(start_date, end_date)
    reason_for_art = ConceptName.find_by_name('REASON FOR ART ELIGIBILITY').concept_id
    reason_concept_id = ConceptName.find_by_name('CD4 COUNT LESS THAN OR EQUAL TO 350').concept_id
    reason2_concept_id = ConceptName.find_by_name('CD4 COUNT LESS THAN OR EQUAL TO 750').concept_id
    reason3_concept_id = ConceptName.find_by_name('CD4 COUNT LESS THAN OR EQUAL TO 250').concept_id
    reason4_concept_id = ConceptName.find_by_name('LYMPHOCYTE COUNT BELOW THRESHOLD WITH WHO STAGE 2').concept_id

    registered = []
    total_registered = ActiveRecord::Base.connection.select_all <<EOF
      SELECT * FROM temp_earliest_start_date t
      INNER JOIN obs ON t.patient_id = obs.person_id
      WHERE date_enrolled BETWEEN '#{start_date}' AND '#{end_date}'
      AND concept_id = #{reason_for_art} AND
      (value_coded = #{reason_concept_id} OR value_coded = #{reason2_concept_id}
      OR value_coded = #{reason3_concept_id} OR value_coded = #{reason4_concept_id})
      AND voided = 0 GROUP BY patient_id;
EOF

    (total_registered || []).each do |patient|
      registered << patient
    end

  end

  def self.confirmed_hiv_infection_in_infants_pcr(start_date, end_date)
    reason_for_art = ConceptName.find_by_name('REASON FOR ART ELIGIBILITY').concept_id
    reason_concept_id = ConceptName.find_by_name('HIV PCR').concept_id

    registered = []
    total_registered = ActiveRecord::Base.connection.select_all <<EOF
      SELECT * FROM temp_earliest_start_date t
      INNER JOIN obs ON t.patient_id = obs.person_id
      WHERE date_enrolled BETWEEN '#{start_date}' AND '#{end_date}'
      AND concept_id = #{reason_for_art} AND (value_coded = #{reason_concept_id})
      AND voided = 0 GROUP BY patient_id;
EOF

    (total_registered || []).each do |patient|
      registered << patient
    end

  end

  def self.presumed_severe_hiv_disease_in_infants(start_date, end_date)
    reason_for_art = ConceptName.find_by_name('REASON FOR ART ELIGIBILITY').concept_id
    reason_concept_id = ConceptName.find_by_name('PRESUMED SEVERE HIV').concept_id

    registered = []
    total_registered = ActiveRecord::Base.connection.select_all <<EOF
      SELECT * FROM temp_earliest_start_date t
      INNER JOIN obs ON t.patient_id = obs.person_id
      WHERE date_enrolled BETWEEN '#{start_date}' AND '#{end_date}'
      AND concept_id = #{reason_for_art} AND value_coded = #{reason_concept_id}
      AND voided = 0 GROUP BY patient_id;
EOF

    (total_registered || []).each do |patient|
      registered << patient
    end

  end

  def self.unknown_age(start_date, end_date)
    registered = []
    total_registered = ActiveRecord::Base.connection.select_all <<EOF
      SELECT * FROM temp_earliest_start_date
      WHERE date_enrolled BETWEEN '#{start_date}' AND '#{end_date}'
      AND age_at_initiation IS NULL GROUP BY patient_id;
EOF

    (total_registered || []).each do |patient|
      registered << patient
    end

    return registered
  end

  def self.adults_at_art_initiation(start_date, end_date)
    registered = []
    total_registered = ActiveRecord::Base.connection.select_all <<EOF
      SELECT * FROM temp_earliest_start_date
      WHERE date_enrolled BETWEEN '#{start_date}' AND '#{end_date}'
      AND age_at_initiation > 14 GROUP BY patient_id;
EOF

    (total_registered || []).each do |patient|
      registered << patient
    end

    return registered
  end

  def self.children_24_months_14_years_at_art_initiation(start_date, end_date)
    registered = []
    total_registered = ActiveRecord::Base.connection.select_all <<EOF
      SELECT * FROM temp_earliest_start_date
      WHERE date_enrolled BETWEEN '#{start_date}' AND '#{end_date}'
      AND age_at_initiation BETWEEN  2 AND 14 GROUP BY patient_id;
EOF

    (total_registered || []).each do |patient|
      registered << patient
    end

    return registered
  end

  def self.children_below_24_months_at_art_initiation(start_date, end_date)
    registered = []
    total_registered = ActiveRecord::Base.connection.select_all <<EOF
      SELECT * FROM temp_earliest_start_date
      WHERE date_enrolled BETWEEN '#{start_date}' AND '#{end_date}'
      AND age_at_initiation < 2 GROUP BY patient_id;
EOF

    (total_registered || []).each do |patient|
      registered << patient
    end

    return registered
  end

  def self.non_pregnant_females(start_date, end_date, pregnant_women = [])
    registered = [] ; pregnant_women_ids = []

    (pregnant_women || []).each do |patient|
      pregnant_women_ids << patient[:patient_id]
    end
    pregnant_women_ids = [0] if pregnant_women_ids.blank?

    data = ActiveRecord::Base.connection.select_all <<EOF
      SELECT * FROM temp_earliest_start_date t
      WHERE date_enrolled BETWEEN '#{start_date}' AND '#{end_date}'
      AND (gender = 'F' OR gender = 'Female')
      AND t.patient_id NOT IN(#{pregnant_women_ids.join(',')}) GROUP BY patient_id;
EOF

    (data || []).each do |patient|
      registered << patient
    end

    return registered
  end

  def self.pregnant_females_all_ages(start_date, end_date)
    registered = [] ; patient_id_plus_date_enrolled = []
    data = ActiveRecord::Base.connection.select_all <<EOF
      SELECT * FROM temp_earliest_start_date t
      INNER JOIN person p ON p.person_id = t.patient_id
      WHERE date_enrolled BETWEEN '#{start_date}' AND '#{end_date}'
      AND (gender = 'F' OR gender = 'Female') GROUP BY patient_id;
EOF

    (data || []).each do |patient|
      patient_id_plus_date_enrolled << [patient['patient_id'].to_i, patient['date_enrolled'].to_date]
    end

    yes_concept_id = ConceptName.find_by_name('Yes').concept_id
    preg_concept_id = ConceptName.find_by_name('IS PATIENT PREGNANT?').concept_id
    patient_preg_concept_id = ConceptName.find_by_name('PATIENT PREGNANT').concept_id
    preg_at_initiation_concept_id = ConceptName.find_by_name('PREGNANT AT INITIATION?').concept_id


    (patient_id_plus_date_enrolled || []).each do |patient_id, date_enrolled|
      result = ActiveRecord::Base.connection.select_value <<EOF
        SELECT * FROM obs
        WHERE obs_datetime BETWEEN '#{date_enrolled.strftime('%Y-%m-%d 00:00:00')}'
        AND '#{(date_enrolled + 28.days).strftime('%Y-%m-%d 23:59:59')}'
        AND person_id = #{patient_id} AND value_coded = #{yes_concept_id}
        AND concept_id IN (#{preg_concept_id}, #{patient_preg_concept_id}, #{preg_at_initiation_concept_id}) AND voided = 0 GROUP BY person_id;
EOF

      registered << {:patient_id => patient_id, :date_enrolled => date_enrolled } unless result.blank?
    end

    return registered
  end

  def self.males(start_date, end_date)
    registered = []
    data = ActiveRecord::Base.connection.select_all <<EOF
      SELECT * FROM temp_earliest_start_date t
      WHERE date_enrolled BETWEEN '#{start_date}' AND '#{end_date}'
      AND (gender = 'Male' OR gender = 'M') GROUP BY patient_id;
EOF

    (data || []).each do |patient|
      registered << patient
    end

    return registered
  end

  def self.transfer_in(start_date, end_date)
    registered = []
    re_initiated_on_art_patient_ids = []

    data = ActiveRecord::Base.connection.select_all <<EOF
      SELECT re_initiated_check(patient_id, date_enrolled) re_initiated FROM temp_earliest_start_date
      WHERE date_enrolled BETWEEN '#{start_date}' AND '#{end_date}'
      AND DATE(date_enrolled) != DATE(earliest_start_date)
      GROUP BY patient_id
      HAVING re_initiated != 'Re-initiated';
EOF

    (data || []).each do |patient|
      registered << patient
    end

    return registered
  end

  def self.re_initiated_on_art(start_date, end_date)
    registered = []
    data = ActiveRecord::Base.connection.select_all <<EOF
      SELECT re_initiated_check(patient_id, date_enrolled) re_initiated FROM temp_earliest_start_date
      WHERE date_enrolled BETWEEN '#{start_date}' AND '#{end_date}'
      AND DATE(date_enrolled) != DATE(earliest_start_date)
      GROUP BY patient_id
      HAVING re_initiated = 'Re-initiated';
EOF

    (data || []).each do |patient|
      registered << patient
    end

    return registered
  end

  def self.initiated_on_art_first_time(start_date, end_date)
    registered = []
    data = ActiveRecord::Base.connection.select_all <<EOF
      SELECT * FROM temp_earliest_start_date
      WHERE date_enrolled BETWEEN '#{start_date}' AND '#{end_date}'
      AND DATE(date_enrolled) = DATE(earliest_start_date)
      GROUP BY patient_id;
EOF

    (data || []).each do |patient|
      registered << patient
    end

    return registered
  end

  def self.get_cum_start_date
    cum_start_date = ActiveRecord::Base.connection.select_value <<EOF
      SELECT MIN(date_enrolled) FROM temp_earliest_start_date;
EOF

    return cum_start_date.to_date rescue nil
  end

  def self.total_registered(start_date, end_date)
    registered = []
    total_registered = ActiveRecord::Base.connection.select_all <<EOF
      SELECT * FROM temp_earliest_start_date
      WHERE date_enrolled BETWEEN '#{start_date}' AND '#{end_date}' GROUP BY patient_id;
EOF

    (total_registered || []).each do |patient|
      registered << patient
    end

    return registered
  end


end
