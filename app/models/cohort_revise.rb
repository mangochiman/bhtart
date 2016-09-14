class CohortRevise



  def self.get_indicators(start_date, end_date)
=begin   
    ActiveRecord::Base.connection.execute <<EOF
      DROP TABLE IF EXISTS `temp_earliest_start_date`;
EOF


    ActiveRecord::Base.connection.execute <<EOF
      CREATE TABLE temp_earliest_start_date 
select 
        `p`.`patient_id` AS `patient_id`,
        cast(patient_start_date(`p`.`patient_id`) as date) AS `date_enrolled`,
        date_antiretrovirals_started(`p`.`patient_id`, min(`s`.`start_date`)) AS `earliest_start_date`,
        `person`.`death_date` AS `death_date`,
        TRUNCATE(((to_days(date_antiretrovirals_started(`p`.`patient_id`, min(`s`.`start_date`))) - to_days(`person`.`birthdate`)) / 365.25), 0) AS `age_at_initiation`,
        (to_days(min(`s`.`start_date`)) - to_days(`person`.`birthdate`)) AS `age_in_days`
    from
        ((`patient_program` `p`
        left join `patient_state` `s` ON ((`p`.`patient_program_id` = `s`.`patient_program_id`)))
        left join `person` ON ((`person`.`person_id` = `p`.`patient_id`)))
    where
        (
          (`p`.`voided` = 0)
          and (`s`.`voided` = 0)
          and (`p`.`program_id` = 1)
          and (`s`.`state` = 7)
        )
    group by `p`.`patient_id`;
EOF

=end
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
Unique PatientProgram entries at the current location for those patients with at least one state ON ARVs and earliest start date of the 'ON ARVs' state within the quarter and having gender of related PERSON entry as F for female and no entries of 'IS PATIENT PREGNANT?' observation answered 'YES' in related HIV CLINIC CONSULTATION encounters not within 28 days from earliest registration date 
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
               :presumed_severe_hiv_disease_in_infants, :confirmed_hiv_infection_in_infants_pcr,
               :tb_within_the_last_two_years, :current_episode_of_tb, :kaposis_sarcoma,
               :presumed_severe_hiv_disease_in_infants, :confirmed_hiv_infection_in_infants_pcr,
               :who_stage_two, :children_12_23_months, :breastfeeding_mothers, :pregnant_women,
               :who_stage_three, :who_stage_four, :unknown_other_reason_outside_guidelines
=end

      return cohort
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
      INNER JOIN person p ON p.person_id = t.patient_id
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

    (patient_id_plus_date_enrolled || []).each do |patient_id, date_enrolled|
      result = ActiveRecord::Base.connection.select_value <<EOF
        SELECT * FROM obs
        WHERE obs_datetime BETWEEN '#{date_enrolled.strftime('%Y-%m-%d 00:00:00')}' 
        AND '#{(date_enrolled + 28.days).strftime('%Y-%m-%d 23:59:59')}' 
        AND person_id = #{patient_id} AND value_coded = #{yes_concept_id} 
        AND concept_id = #{preg_concept_id} AND voided = 0 GROUP BY person_id;
EOF

      registered << {:patient_id => patient_id, :date_enrolled => date_enrolled } unless result.blank?  
    end
 
    return registered 
  end

  def self.males(start_date, end_date)
    registered = []
    data = ActiveRecord::Base.connection.select_all <<EOF
      SELECT * FROM temp_earliest_start_date t
      INNER JOIN person p ON p.person_id = t.patient_id
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

