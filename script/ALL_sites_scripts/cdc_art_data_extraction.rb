
Source_db = YAML.load(File.open(File.join(RAILS_ROOT, "config/database.yml"), "r"))['production']["database"]
CDCDataExtraction = 1

def start
  facility_name = GlobalProperty.find_by_sql("select property_value from global_property where property = 'current_health_center_name'").map(&:property_value).first

  start_date = "2016-10-01".to_date
  end_date = "2016-12-31".to_date

  puts "CDC ART data extraction............................................................................................"

  puts "New on ART (Newly registered)......................................................................................"
  total_new_on_art, new_on_art_less_1, new_on_art_between_1_and_9, new_on_art_btwn_10_14_female, new_on_art_btwn_10_14_male, new_on_art_less_15_19_female, new_on_art_less_15_19_male, new_on_art_less_20_24_female, new_on_art_less_20_24_male, new_on_art_less_25_49_female, new_on_art_less_25_49_male, new_on_art_less_more_than_50_female, new_on_art_less_more_than_50_male, new_on_art_less_15_female, new_on_art_less_15_male, new_on_art_more_15_female,  new_on_art_more_15_male = new_on_art(start_date, end_date, nil, nil)

  puts "Receving ART (Total registered- Cumulative)........................................................................................."
  total_receiving_art_cumulative, receiving_art_cumulative_less_1, receiving_art_cumulative_between_1_and_9, receiving_art_cumulative_btwn_10_14_female, receiving_art_cumulative_btwn_10_14_male, receiving_art_cumulative_less_15_19_female, receiving_art_cumulative_less_15_19_male, receiving_art_cumulative_less_20_24_female, receiving_art_cumulative_less_20_24_male, receiving_art_cumulative_less_25_49_female, receiving_art_cumulative_less_25_49_male, receiving_art_cumulative_less_more_than_50_female, receiving_art_cumulative_less_more_than_50_male, receiving_art_cumulative_less_15_female, receiving_art_cumulative_less_15_male, receiving_art_cumulative_more_15_female,  receiving_art_cumulative_more_15_male = receiving_art_cumulative(start_date, end_date, nil, nil)

  puts "PLHIV with screened TB status positive............................................................................."
  total_plhiv_screened_tb_status, plhiv_screened_tb_status_less_15_female, plhiv_screened_tb_status_less_15_male, plhiv_screened_tb_status_more_15_female, plhiv_screened_tb_status_more_15_male = plhiv_screened_tb_status(start_date, end_date, nil, nil)

  puts "Total alive and on ARVS at 12 months after initiation.............................................."
  total_alive_and_on_ARVS_at_12_months_after_initiation, alive_and_on_ARVS_at_12_months_after_initiation_less_1, alive_and_on_ARVS_at_12_months_after_initiation_between_1_and_9, alive_and_on_ARVS_at_12_months_after_initiation_btwn_10_14_female, alive_and_on_ARVS_at_12_months_after_initiation_btwn_10_14_male, alive_and_on_ARVS_at_12_months_after_initiation_less_15_19_female, alive_and_on_ARVS_at_12_months_after_initiation_less_15_19_male, alive_and_on_ARVS_at_12_months_after_initiation_less_20_24_female, alive_and_on_ARVS_at_12_months_after_initiation_less_20_24_male, alive_and_on_ARVS_at_12_months_after_initiation_less_25_49_female, alive_and_on_ARVS_at_12_months_after_initiation_less_25_49_male, alive_and_on_ARVS_at_12_months_after_initiation_less_more_than_50_female, alive_and_on_ARVS_at_12_months_after_initiation_less_more_than_50_male, alive_and_on_ARVS_at_12_months_after_initiation_less_15_female, alive_and_on_ARVS_at_12_months_after_initiation_less_15_male, alive_and_on_ARVS_at_12_months_after_initiation_more_15_female, alive_and_on_ARVS_at_12_months_after_initiation_more_15_male = alive_and_on_ARVS_at_12_months_after_initiation(start_date, end_date, nil, nil)

  puts "Total initiated in 12 months........................................................................."
  total_total_initiated_in_12_months, total_initiated_in_12_months_less_1, total_initiated_in_12_months_between_1_and_9, total_initiated_in_12_months_btwn_10_14_female, total_initiated_in_12_months_btwn_10_14_male, total_initiated_in_12_months_less_15_19_female, total_initiated_in_12_months_less_15_19_male, total_initiated_in_12_months_less_20_24_female, total_initiated_in_12_months_less_20_24_male, total_initiated_in_12_months_less_25_49_female, total_initiated_in_12_months_less_25_49_male, total_initiated_in_12_months_less_more_than_50_female, total_initiated_in_12_months_less_more_than_50_male, total_initiated_in_12_months_less_15_female, total_initiated_in_12_months_less_15_male, total_initiated_in_12_months_more_15_female, total_initiated_in_12_months_more_15_male = total_initiated_in_12_months(start_date, end_date, nil, nil)

  puts "PEADs with outcomes........................................................................."
  cumulative_children_less_15_years = cumulative_children_less_15_years(start_date, end_date)
  patients_alive_and_on_arvs_all_ages, patients_alive_and_on_arvs_less_15_years, patients_alive_and_on_arvs_less_bwtn_10_14_years, patients_alive_and_on_arvs_only_14_years, patients_alive_and_on_arvs_between_15_and_19_years, patients_alive_and_on_arvs_more_than_20_years = peads_receiving_art_cumulative(start_date, end_date)
  treatment_outcome_for_children_less_15yrs_died = patients_by_age_and_outcomes(start_date, end_date, "Patient died", 0, 14)
  treatment_outcome_for_children_less_15yrs_defaulted = patients_by_age_and_outcomes(start_date, end_date, "Defaulted", 0, 14)
  treatment_outcome_for_children_less_15yrs_stopped = patients_by_age_and_outcomes(start_date, end_date, "Treatment stopped", 0, 14)
  treatment_outcome_for_children_less_15yrs_transferred_out = patients_by_age_and_outcomes(start_date, end_date, "Patient transferred out", 0, 14)

  if CDCDataExtraction == 1
    $resultsOutput = File.open("./CDCDataExtraction_" + "#{facility_name}" + ".txt", "w")
    $resultsOutput  << "Newly patients regigestered...........................................................\n"
    $resultsOutput  << "total_new_on_art: #{total_new_on_art}\n new_on_art_less_1: #{new_on_art_less_1}\n new_on_art_between_1_and_9: #{new_on_art_between_1_and_9}\n new_on_art_btwn_10_14_female: #{new_on_art_btwn_10_14_female}\n new_on_art_btwn_10_14_male: #{new_on_art_btwn_10_14_male}\n new_on_art_less_15_19_female: #{new_on_art_less_15_19_female}\n new_on_art_less_15_19_male: #{new_on_art_less_15_19_male}\n new_on_art_less_20_24_female: #{new_on_art_less_20_24_female}\n new_on_art_less_20_24_male: #{new_on_art_less_20_24_male}\n new_on_art_less_25_49_female: #{new_on_art_less_25_49_female}\n new_on_art_less_25_49_male: #{new_on_art_less_25_49_male}\n new_on_art_less_more_than_50_female: #{new_on_art_less_more_than_50_female}\n new_on_art_less_more_than_50_male: #{new_on_art_less_more_than_50_male}\n new_on_art_less_15_female: #{new_on_art_less_15_female}\n new_on_art_less_15_male: #{new_on_art_less_15_male}\n new_on_art_more_15_female: #{new_on_art_more_15_female}\n new_on_art_more_15_male: #{new_on_art_more_15_male}\n"
    $resultsOutput  << "\n Ever received ARVS (cumulative) regigestered...........................................................\n"
    $resultsOutput  << "total_receiving_art_cumulative: #{total_receiving_art_cumulative}\n receiving_art_cumulative_less_1: #{receiving_art_cumulative_less_1}\n receiving_art_cumulative_between_1_and_9: #{receiving_art_cumulative_between_1_and_9}\n receiving_art_cumulative_btwn_10_14_female: #{receiving_art_cumulative_btwn_10_14_female}\n receiving_art_cumulative_btwn_10_14_male: #{receiving_art_cumulative_btwn_10_14_male}\n receiving_art_cumulative_less_15_19_female: #{receiving_art_cumulative_less_15_19_female}\n receiving_art_cumulative_less_15_19_male: #{receiving_art_cumulative_less_15_19_male}\n receiving_art_cumulative_less_20_24_female: #{receiving_art_cumulative_less_20_24_female}\n receiving_art_cumulative_less_20_24_male: #{receiving_art_cumulative_less_20_24_male}\n receiving_art_cumulative_less_25_49_female: #{receiving_art_cumulative_less_25_49_female}\n receiving_art_cumulative_less_25_49_male: #{receiving_art_cumulative_less_25_49_male}\n receiving_art_cumulative_less_more_than_50_female: #{receiving_art_cumulative_less_more_than_50_female}\n receiving_art_cumulative_less_more_than_50_male: #{receiving_art_cumulative_less_more_than_50_male}\n receiving_art_cumulative_less_15_female: #{receiving_art_cumulative_less_15_female}\n receiving_art_cumulative_less_15_male: #{receiving_art_cumulative_less_15_male}\n receiving_art_cumulative_more_15_female: #{receiving_art_cumulative_more_15_female}\n receiving_art_cumulative_more_15_male: #{receiving_art_cumulative_more_15_male}\n"
    $resultsOutput  << "\nNewly PLHIV total Registered screened TB status...........................................................\n"
    $resultsOutput  << "total_plhiv_screened_tb_status: #{total_plhiv_screened_tb_status}\n plhiv_screened_tb_status_less_15_female: #{plhiv_screened_tb_status_less_15_female}\n plhiv_screened_tb_status_less_15_male: #{plhiv_screened_tb_status_less_15_male}\n plhiv_screened_tb_status_more_15_female: #{plhiv_screened_tb_status_more_15_female}\n plhiv_screened_tb_status_more_15_male: #{plhiv_screened_tb_status_more_15_male}\n"
    $resultsOutput  << "\nTotal alive and on ARVs after 12 months of initiation...........................................................\n"
    $resultsOutput  << "total_alive_and_on_ARVS_at_12_months_after_initiation: #{total_alive_and_on_ARVS_at_12_months_after_initiation}\n alive_and_on_ARVS_at_12_months_after_initiation_less_1: #{alive_and_on_ARVS_at_12_months_after_initiation_less_1}\n alive_and_on_ARVS_at_12_months_after_initiation_between_1_and_9: #{alive_and_on_ARVS_at_12_months_after_initiation_between_1_and_9}\n alive_and_on_ARVS_at_12_months_after_initiation_btwn_10_14_female: #{alive_and_on_ARVS_at_12_months_after_initiation_btwn_10_14_female}\n alive_and_on_ARVS_at_12_months_after_initiation_btwn_10_14_male: #{alive_and_on_ARVS_at_12_months_after_initiation_btwn_10_14_male}\n alive_and_on_ARVS_at_12_months_after_initiation_less_15_19_female: #{alive_and_on_ARVS_at_12_months_after_initiation_less_15_19_female}\n alive_and_on_ARVS_at_12_months_after_initiation_less_15_19_male: #{alive_and_on_ARVS_at_12_months_after_initiation_less_15_19_male}\n alive_and_on_ARVS_at_12_months_after_initiation_less_20_24_female: #{alive_and_on_ARVS_at_12_months_after_initiation_less_20_24_female}\n alive_and_on_ARVS_at_12_months_after_initiation_less_20_24_male: #{alive_and_on_ARVS_at_12_months_after_initiation_less_20_24_male}\n alive_and_on_ARVS_at_12_months_after_initiation_less_25_49_female: #{alive_and_on_ARVS_at_12_months_after_initiation_less_25_49_female}\n alive_and_on_ARVS_at_12_months_after_initiation_less_25_49_male: #{alive_and_on_ARVS_at_12_months_after_initiation_less_25_49_male}\n alive_and_on_ARVS_at_12_months_after_initiation_less_more_than_50_female: #{alive_and_on_ARVS_at_12_months_after_initiation_less_more_than_50_female}\n alive_and_on_ARVS_at_12_months_after_initiation_less_more_than_50_male: #{alive_and_on_ARVS_at_12_months_after_initiation_less_more_than_50_male}\n alive_and_on_ARVS_at_12_months_after_initiation_less_15_female: #{alive_and_on_ARVS_at_12_months_after_initiation_less_15_female}\n alive_and_on_ARVS_at_12_months_after_initiation_less_15_male: #{alive_and_on_ARVS_at_12_months_after_initiation_less_15_male}\n alive_and_on_ARVS_at_12_months_after_initiation_more_15_female: #{alive_and_on_ARVS_at_12_months_after_initiation_more_15_female}\n alive_and_on_ARVS_at_12_months_after_initiation_more_15_male: #{alive_and_on_ARVS_at_12_months_after_initiation_more_15_male}\n"
    $resultsOutput  << "\nTotal inititiated in 12 months...........................................................\n"
    $resultsOutput  << "total_initiated_in_12_months: #{total_total_initiated_in_12_months}\n total_initiated_in_12_months_less_1: #{total_initiated_in_12_months_less_1}\n total_initiated_in_12_months_between_1_and_9: #{total_initiated_in_12_months_between_1_and_9}\n total_initiated_in_12_months_btwn_10_14_female: #{total_initiated_in_12_months_btwn_10_14_female}\n total_initiated_in_12_months_btwn_10_14_male: #{total_initiated_in_12_months_btwn_10_14_male}\n total_initiated_in_12_months_less_15_19_female: #{total_initiated_in_12_months_less_15_19_female}\n total_initiated_in_12_months_less_15_19_male: #{total_initiated_in_12_months_less_15_19_male}\n total_initiated_in_12_months_less_20_24_female: #{total_initiated_in_12_months_less_20_24_female}\n total_initiated_in_12_months_less_20_24_male: #{total_initiated_in_12_months_less_20_24_male}\n total_initiated_in_12_months_less_25_49_female: #{total_initiated_in_12_months_less_25_49_female}\n total_initiated_in_12_months_less_25_49_male: #{total_initiated_in_12_months_less_25_49_male}\n total_initiated_in_12_months_less_more_than_50_female: #{total_initiated_in_12_months_less_more_than_50_female}\n total_initiated_in_12_months_less_more_than_50_male: #{total_initiated_in_12_months_less_more_than_50_male}\n total_initiated_in_12_months_less_15_female: #{total_initiated_in_12_months_less_15_female}\n total_initiated_in_12_months_less_15_male: #{total_initiated_in_12_months_less_15_male}\n total_initiated_in_12_months_more_15_female: #{total_initiated_in_12_months_more_15_female}\n total_initiated_in_12_months_more_15_male: #{total_initiated_in_12_months_more_15_male}\n"
    $resultsOutput  << "\nChildren with outcomes...........................................................\n"
    $resultsOutput  << "patients_alive_and_on_arvs_all_ages: #{patients_alive_and_on_arvs_all_ages}\n  cumulative_children_less_15_years : #{cumulative_children_less_15_years }\n  patients_alive_and_on_arvs_less_15_years: #{patients_alive_and_on_arvs_less_15_years}\n  patients_alive_and_on_arvs_less_bwtn_10_14_years: #{patients_alive_and_on_arvs_less_bwtn_10_14_years}\n  patients_alive_and_on_arvs_only_14_years: #{patients_alive_and_on_arvs_only_14_years}\n  patients_alive_and_on_arvs_between_15_and_19_years: #{patients_alive_and_on_arvs_between_15_and_19_years}\n  patients_alive_and_on_arvs_more_than_20_years: #{patients_alive_and_on_arvs_more_than_20_years}\n  treatment_outcome_for_children_less_15yrs_died: #{treatment_outcome_for_children_less_15yrs_died}\n  treatment_outcome_for_children_less_15yrs_defaulted: #{treatment_outcome_for_children_less_15yrs_defaulted}\n  treatment_outcome_for_children_less_15yrs_stopped: #{treatment_outcome_for_children_less_15yrs_stopped}\n  treatment_outcome_for_children_less_15yrs_transferred_out: #{treatment_outcome_for_children_less_15yrs_transferred_out}\n "
  end

  if CDCDataExtraction == 1
    $resultsOutput.close()
  end

end

def self.cumulative_children_less_15_years(start_date, end_date)
  new_on_art = ActiveRecord::Base.connection.select_all <<EOF
    SELECT * FROM earliest_start_date
    WHERE date_enrolled <= '#{end_date}'
    AND age_at_initiation between 0 and 14;
EOF

  return new_on_art.count
end

def self.new_on_art(start_date, end_date, min_age = nil, max_age = nil, gender = [])
  patient_ids = []
  patients_id =   ActiveRecord::Base.connection.select_all <<EOF
    SELECT * FROM earliest_start_date
      WHERE date_enrolled BETWEEN '#{start_date}' AND  '#{end_date}';
EOF

  (patients_id || []).each do |patient|
    patient_ids << patient['patient_id'].to_i
  end

  new_on_art = ActiveRecord::Base.connection.select_all <<EOF
                SELECT esd.*
                 FROM patients_on_arvs esd
                 LEFT JOIN clinic_registration_encounter e ON esd.patient_id = e.patient_id
                 LEFT JOIN ever_registered_obs AS ero ON e.encounter_id = ero.encounter_id
                 WHERE esd.patient_id IN (#{patient_ids.join(',')}) AND
                 (ero.obs_id IS NULL)
                 GROUP BY esd.patient_id
EOF

  total_new_on_art = []; new_on_art_less_1  = []; new_on_art_between_1_and_9 = []
  new_on_art_btwn_10_14_female  = []; new_on_art_btwn_10_14_male = []; new_on_art_less_15_19_female = []
  new_on_art_less_15_19_male = []; new_on_art_less_20_24_female = []; new_on_art_less_20_24_male = []
  new_on_art_less_25_49_female = []; new_on_art_less_25_49_male = []; new_on_art_less_more_than_50_female = []
  new_on_art_less_more_than_50_male = []; new_on_art_less_15_female = []; new_on_art_less_15_male = []
  new_on_art_more_15_female = []; new_on_art_more_15_male = []

  (new_on_art || []).each do |patient|
        if patient['age_at_initiation'].to_i <= 1
          new_on_art_less_1 << patient['patient_id'].to_i
        elsif patient['age_at_initiation'].to_i  >= 2 && patient['age_at_initiation'].to_i  <= 9
          new_on_art_between_1_and_9 << patient['patient_id'].to_i
        end

        if (patient['age_at_initiation'].to_i  >= 10 && patient['age_at_initiation'].to_i  <= 14)
          if (patient['gender'] == "M")
            new_on_art_btwn_10_14_male << patient['patient_id'].to_i
          else
            new_on_art_btwn_10_14_female << patient['patient_id'].to_i
          end
        end

        if (patient['age_at_initiation'].to_i  >= 15 && patient['age_at_initiation'].to_i  <= 19)
          if (patient['gender'] == "F")
            new_on_art_less_15_19_female << patient['patient_id'].to_i
          else
            new_on_art_less_15_19_male << patient['patient_id'].to_i
          end
        end

        if (patient['age_at_initiation'].to_i  >= 20 && patient['age_at_initiation'].to_i  <= 24)
          if (patient['gender'] == "M")
            new_on_art_less_20_24_male << patient['patient_id'].to_i
          else
            new_on_art_less_20_24_female << patient['patient_id'].to_i
          end
        end

        if (patient['age_at_initiation'].to_i  >= 25 && patient['age_at_initiation'].to_i  <= 49)
          if (patient['gender'] == "M")
            new_on_art_less_25_49_male << patient['patient_id'].to_i
          else
            new_on_art_less_25_49_female << patient['patient_id'].to_i
          end
        end

        if (patient['age_at_initiation'].to_i  >= 50)
          if (patient['gender'] == "M")
            new_on_art_less_more_than_50_male << patient['patient_id'].to_i
          else
            new_on_art_less_more_than_50_female << patient['patient_id'].to_i
          end
        end

        if (patient['age_at_initiation'].to_i  <= 14)
          if (patient['gender'] == "M")
            new_on_art_less_15_male << patient['patient_id'].to_i
          else
            new_on_art_less_15_female << patient['patient_id'].to_i
          end
       end

        if (patient['age_at_initiation'].to_i  >= 15)
          if (patient['gender'] ==  "M")
            new_on_art_more_15_male << patient['patient_id'].to_i
          else
            new_on_art_more_15_female << patient['patient_id'].to_i
          end
        end
  end

  (new_on_art || []).each do |patient|
    total_new_on_art << patient['patient_id'].to_i
  end

  return [total_new_on_art.count, new_on_art_less_1.count,
  new_on_art_between_1_and_9.count,
  new_on_art_btwn_10_14_female.count,
  new_on_art_btwn_10_14_male.count,
  new_on_art_less_15_19_female.count,
  new_on_art_less_15_19_male.count,
  new_on_art_less_20_24_female.count,
  new_on_art_less_20_24_male.count,
  new_on_art_less_25_49_female.count,
  new_on_art_less_25_49_male.count,
  new_on_art_less_more_than_50_female.count,
  new_on_art_less_more_than_50_male.count,
  new_on_art_less_15_female.count,
  new_on_art_less_15_male.count,
  new_on_art_more_15_female.count,
  new_on_art_more_15_male.count]
end

def self.receiving_art_cumulative(start_date, end_date, min_age = nil, max_age = nil, gender = [])

  art_defaulters = ActiveRecord::Base.connection.select_all <<EOF
        SELECT p.person_id AS patient_id, current_defaulter(p.person_id, '#{end_date}') AS def
				FROM earliest_start_date e
         Inner JOIN person p on p.person_id = e.patient_id and p.voided  = 0
				WHERE p.dead = 0
        AND date_enrolled <= '#{end_date}'
				GROUP BY p.person_id
				HAVING def = 1 AND current_state_for_program(p.person_id, 1, '#{end_date}') NOT IN (6, 2, 3)
EOF

    patient_ids = []
    (art_defaulters || []).each do |patient|
      patient_ids << patient['patient_id'].to_i
    end

  receiving_art_cumulative = ActiveRecord::Base.connection.select_all <<EOF
    SELECT e.patient_id, current_state_for_program(e.patient_id, 1, '#{end_date}') AS state, date_enrolled, age_at_initiation, gender
    FROM earliest_start_date e
    WHERE date_enrolled <= '#{end_date}'
    AND e.patient_id NOT IN (#{patient_ids.join(',')})
    GROUP BY e.patient_id
    HAVING state = 7;
EOF

    total_receiving_art_cumulative = []; receiving_art_cumulative_less_1  = []; receiving_art_cumulative_between_1_and_9 = []
    receiving_art_cumulative_btwn_10_14_female  = []; receiving_art_cumulative_btwn_10_14_male = []; receiving_art_cumulative_less_15_19_female = []
    receiving_art_cumulative_less_15_19_male = []; receiving_art_cumulative_less_20_24_female = []; receiving_art_cumulative_less_20_24_male = []
    receiving_art_cumulative_less_25_49_female = []; receiving_art_cumulative_less_25_49_male = []; receiving_art_cumulative_less_more_than_50_female = []
    receiving_art_cumulative_less_more_than_50_male = []; receiving_art_cumulative_less_15_female = []; receiving_art_cumulative_less_15_male = []
    receiving_art_cumulative_more_15_female = []; receiving_art_cumulative_more_15_male = []

    (receiving_art_cumulative || []).each do |patient|
          if patient['age_at_initiation'].to_i <= 1
            receiving_art_cumulative_less_1 << patient
          elsif patient['age_at_initiation'].to_i >= 2 && patient['age_at_initiation'].to_i <= 9
            receiving_art_cumulative_between_1_and_9 << patient
          end

          if (patient['age_at_initiation'].to_i >= 10 && patient['age_at_initiation'].to_i <= 14)
            if (patient['gender'] == "M")
              receiving_art_cumulative_btwn_10_14_male << patient
            else
              receiving_art_cumulative_btwn_10_14_female << patient
            end
          end

          if (patient['age_at_initiation'].to_i >= 15 && patient['age_at_initiation'].to_i <= 19)
            if (patient['gender'] == "M")
              receiving_art_cumulative_less_15_19_male << patient
            else
              receiving_art_cumulative_less_15_19_female << patient
            end
          end

          if (patient['age_at_initiation'].to_i >= 20 && patient['age_at_initiation'].to_i <= 24)
            if (patient['gender'] == "M")
              receiving_art_cumulative_less_20_24_male << patient
            else
              receiving_art_cumulative_less_20_24_female << patient
            end
          end

          if (patient['age_at_initiation'].to_i >= 25 && patient['age_at_initiation'].to_i <= 49)
            if (patient['gender'] == "M")
              receiving_art_cumulative_less_25_49_male << patient
            else
              receiving_art_cumulative_less_25_49_female << patient
            end
          end

          if (patient['age_at_initiation'].to_i >= 50)
            if (patient['gender'] == "M")
              receiving_art_cumulative_less_more_than_50_male << patient
            else
              receiving_art_cumulative_less_more_than_50_female << patient
            end
          end

          if (patient['age_at_initiation'].to_i < 15)
            if (patient['gender'] == "M")
              receiving_art_cumulative_less_15_male << patient
            else
              receiving_art_cumulative_less_15_female << patient
            end
         end

          if (patient['age_at_initiation'].to_i >= 15)
            if (patient['gender'] ==  "M")
              receiving_art_cumulative_more_15_male << patient
            else
              receiving_art_cumulative_more_15_female << patient
            end
          end
    end

    total_receiving_art_cumulative = receiving_art_cumulative

    return [total_receiving_art_cumulative.count, receiving_art_cumulative_less_1.count,
    receiving_art_cumulative_between_1_and_9.count,
    receiving_art_cumulative_btwn_10_14_female.count,
    receiving_art_cumulative_btwn_10_14_male.count,
    receiving_art_cumulative_less_15_19_female.count,
    receiving_art_cumulative_less_15_19_male.count,
    receiving_art_cumulative_less_20_24_female.count,
    receiving_art_cumulative_less_20_24_male.count,
    receiving_art_cumulative_less_25_49_female.count,
    receiving_art_cumulative_less_25_49_male.count,
    receiving_art_cumulative_less_more_than_50_female.count,
    receiving_art_cumulative_less_more_than_50_male.count,
    receiving_art_cumulative_less_15_female.count,
    receiving_art_cumulative_less_15_male.count,
    receiving_art_cumulative_more_15_female.count,
    receiving_art_cumulative_more_15_male.count]
end

def self.plhiv_screened_tb_status(start_date, end_date, min_age = nil, max_age = nil, gender = [])
  art_defaulters = ActiveRecord::Base.connection.select_all <<EOF
        SELECT p.person_id AS patient_id, current_defaulter(p.person_id, '#{end_date}') AS def
				FROM earliest_start_date e
         Inner JOIN person p on p.person_id = e.patient_id and p.voided  = 0
				WHERE p.dead = 0
        AND date_enrolled <= '#{end_date}'
				GROUP BY p.person_id
				HAVING def = 1 AND current_state_for_program(p.person_id, 1, '#{end_date}') NOT IN (6, 2, 3)
EOF

    patient_ids = []
    (art_defaulters || []).each do |patient|
      patient_ids << patient['patient_id'].to_i
    end

  alive_and_on_arvs = ActiveRecord::Base.connection.select_all <<EOF
    SELECT e.patient_id, current_state_for_program(e.patient_id, 1, '#{end_date}') AS state, date_enrolled, age_at_initiation, gender
    FROM earliest_start_date e
    WHERE date_enrolled <= '#{end_date}'
    AND e.patient_id NOT IN (#{patient_ids.join(',')})
    GROUP BY e.patient_id
    HAVING state = 7;
EOF
   total_alive_ids = []
  (alive_and_on_arvs || []).each do |patient|
    total_alive_ids << patient['patient_id'].to_i
  end

    plhiv_screened_tb_status = ActiveRecord::Base.connection.select_all <<EOF
      SELECT esd.* FROM earliest_start_date esd
        INNER JOIN obs o  ON esd.patient_id = o.person_id
      WHERE o.person_id IN (#{total_alive_ids.join(',')})
      AND o.concept_id = 7459
      AND value_coded IN (7456, 7458)
      AND DATE(o.obs_datetime) < '#{end_date}' and o.voided = 0
      GROUP BY o.person_id;
EOF

  total_plhiv_screened_tb_status = []; plhiv_screened_tb_status_less_1  = []; plhiv_screened_tb_status_between_1_and_9 = []
  plhiv_screened_tb_status_btwn_10_14_female  = []; plhiv_screened_tb_status_btwn_10_14_male = []; plhiv_screened_tb_status_less_15_19_female = []
  plhiv_screened_tb_status_less_15_19_male = []; plhiv_screened_tb_status_less_20_24_female = []; plhiv_screened_tb_status_less_20_24_male = []
  plhiv_screened_tb_status_less_25_49_female = []; plhiv_screened_tb_status_less_25_49_male = []; plhiv_screened_tb_status_less_more_than_50_female = []
  plhiv_screened_tb_status_less_more_than_50_male = []; plhiv_screened_tb_status_less_15_female = []; plhiv_screened_tb_status_less_15_male = []
  plhiv_screened_tb_status_more_15_female = []; plhiv_screened_tb_status_more_15_male = []

  (plhiv_screened_tb_status || []).each do |patient|
        if patient['age_at_initiation'].to_i <= 1
          plhiv_screened_tb_status_less_1 << patient
        elsif patient['age_at_initiation'].to_i >= 2 && patient['age_at_initiation'].to_i <= 9
          plhiv_screened_tb_status_between_1_and_9 << patient
        end

        if (patient['age_at_initiation'].to_i >= 10 && patient['age_at_initiation'].to_i <= 14)
          if (patient['gender'] == "M")
            plhiv_screened_tb_status_btwn_10_14_male << patient
          else
            plhiv_screened_tb_status_btwn_10_14_female << patient
          end
        end

        if (patient['age_at_initiation'].to_i >= 15 && patient['age_at_initiation'].to_i <= 19)
          if (patient['gender'] == "M")
            plhiv_screened_tb_status_less_15_19_male << patient
          else
            plhiv_screened_tb_status_less_15_19_female << patient
          end
        end

        if (patient['age_at_initiation'].to_i >= 20 && patient['age_at_initiation'].to_i <= 24)
          if (patient['gender'] == "M")
            plhiv_screened_tb_status_less_20_24_male << patient
          else
            plhiv_screened_tb_status_less_20_24_female << patient
          end
        end

        if (patient['age_at_initiation'].to_i >= 25 && patient['age_at_initiation'].to_i <= 49)
          if (patient['gender'] == "M")
            plhiv_screened_tb_status_less_25_49_male << patient
          else
            plhiv_screened_tb_status_less_25_49_female << patient
          end
        end

        if (patient['age_at_initiation'].to_i >= 50)
          if (patient['gender'] == "M")
            plhiv_screened_tb_status_less_more_than_50_male << patient
          else
            plhiv_screened_tb_status_less_more_than_50_female << patient
          end
        end

        if (patient['age_at_initiation'].to_i < 15)
          if (patient['gender'] == "M")
            plhiv_screened_tb_status_less_15_male << patient
          else
            plhiv_screened_tb_status_less_15_female << patient
          end
       end

        if (patient['age_at_initiation'].to_i >= 15)
          if (patient['gender'] ==  "M")
            plhiv_screened_tb_status_more_15_male << patient
          else
            plhiv_screened_tb_status_more_15_female << patient
          end
        end
  end

  total_plhiv_screened_tb_status = plhiv_screened_tb_status

  return [total_plhiv_screened_tb_status.count,
  plhiv_screened_tb_status_less_15_female.count,
  plhiv_screened_tb_status_less_15_male.count,
  plhiv_screened_tb_status_more_15_female.count,
  plhiv_screened_tb_status_more_15_male.count]
end

def self.alive_and_on_ARVS_at_12_months_after_initiation(start_date, end_date, min_age = nil, max_age = nil, gender = [])

    art_defaulters = ActiveRecord::Base.connection.select_all <<EOF
          SELECT p.person_id AS patient_id, current_defaulter(p.person_id, '#{end_date}') AS def
  				FROM earliest_start_date e
           Inner JOIN person p on p.person_id = e.patient_id and p.voided  = 0
  				WHERE p.dead = 0
          AND date_enrolled <= '#{end_date}'
  				GROUP BY p.person_id
  				HAVING def = 1 AND current_state_for_program(p.person_id, 1, '#{end_date}') NOT IN (6, 2, 3)
EOF

      @patient_ids = []
      (art_defaulters || []).each do |patient|
        @patient_ids << patient['patient_id'].to_i
      end

    alive_and_on_ARVS_at_12_months_after_initiation = ActiveRecord::Base.connection.select_all <<EOF
      SELECT e.patient_id, current_state_for_program(e.patient_id, 1, '#{end_date}') AS state, date_enrolled, age_at_initiation, gender
      FROM earliest_start_date e
      WHERE date_enrolled BETWEEN '2016-01-01' AND '#{end_date}'
      AND e.patient_id NOT IN (#{@patient_ids.join(',')})
      GROUP BY e.patient_id
      HAVING state = 7;
EOF

    total_alive_and_on_ARVS_at_12_months_after_initiation = []; alive_and_on_ARVS_at_12_months_after_initiation_less_1  = []; alive_and_on_ARVS_at_12_months_after_initiation_between_1_and_9 = []
    alive_and_on_ARVS_at_12_months_after_initiation_btwn_10_14_female  = []; alive_and_on_ARVS_at_12_months_after_initiation_btwn_10_14_male = []; alive_and_on_ARVS_at_12_months_after_initiation_less_15_19_female = []
    alive_and_on_ARVS_at_12_months_after_initiation_less_15_19_male = []; alive_and_on_ARVS_at_12_months_after_initiation_less_20_24_female = []; alive_and_on_ARVS_at_12_months_after_initiation_less_20_24_male = []
    alive_and_on_ARVS_at_12_months_after_initiation_less_25_49_female = []; alive_and_on_ARVS_at_12_months_after_initiation_less_25_49_male = []; alive_and_on_ARVS_at_12_months_after_initiation_less_more_than_50_female = []
    alive_and_on_ARVS_at_12_months_after_initiation_less_more_than_50_male = []; alive_and_on_ARVS_at_12_months_after_initiation_less_15_female = []; alive_and_on_ARVS_at_12_months_after_initiation_less_15_male = []
    alive_and_on_ARVS_at_12_months_after_initiation_more_15_female = []; alive_and_on_ARVS_at_12_months_after_initiation_more_15_male = []

    (alive_and_on_ARVS_at_12_months_after_initiation || []).each do |patient|
          if patient['age_at_initiation'].to_i <= 1
            alive_and_on_ARVS_at_12_months_after_initiation_less_1 << patient
          elsif patient['age_at_initiation'].to_i >= 2 && patient['age_at_initiation'].to_i <= 9
            alive_and_on_ARVS_at_12_months_after_initiation_between_1_and_9 << patient
          end

          if (patient['age_at_initiation'].to_i >= 10 && patient['age_at_initiation'].to_i <= 14)
            if (patient['gender'] == "M")
              alive_and_on_ARVS_at_12_months_after_initiation_btwn_10_14_male << patient
            else
              alive_and_on_ARVS_at_12_months_after_initiation_btwn_10_14_female << patient
            end
          end

          if (patient['age_at_initiation'].to_i >= 15 && patient['age_at_initiation'].to_i <= 19)
            if (patient['gender'] == "M")
              alive_and_on_ARVS_at_12_months_after_initiation_less_15_19_male << patient
            else
              alive_and_on_ARVS_at_12_months_after_initiation_less_15_19_female << patient
            end
          end

          if (patient['age_at_initiation'].to_i >= 20 && patient['age_at_initiation'].to_i <= 24)
            if (patient['gender'] == "M")
              alive_and_on_ARVS_at_12_months_after_initiation_less_20_24_male << patient
            else
              alive_and_on_ARVS_at_12_months_after_initiation_less_20_24_female << patient
            end
          end

          if (patient['age_at_initiation'].to_i >= 25 && patient['age_at_initiation'].to_i <= 49)
            if (patient['gender'] == "M")
              alive_and_on_ARVS_at_12_months_after_initiation_less_25_49_male << patient
            else
              alive_and_on_ARVS_at_12_months_after_initiation_less_25_49_female << patient
            end
          end

          if (patient['age_at_initiation'].to_i >= 50)
            if (patient['gender'] == "M")
              alive_and_on_ARVS_at_12_months_after_initiation_less_more_than_50_male << patient
            else
              alive_and_on_ARVS_at_12_months_after_initiation_less_more_than_50_female << patient
            end
          end

          if (patient['age_at_initiation'].to_i < 15)
            if (patient['gender'] == "M")
              alive_and_on_ARVS_at_12_months_after_initiation_less_15_male << patient
            else
              alive_and_on_ARVS_at_12_months_after_initiation_less_15_female << patient
            end
         end

          if (patient['age_at_initiation'].to_i >= 15)
            if (patient['gender'] ==  "M")
              alive_and_on_ARVS_at_12_months_after_initiation_more_15_male << patient
            else
              alive_and_on_ARVS_at_12_months_after_initiation_more_15_female << patient
            end
          end
    end

    total_alive_and_on_ARVS_at_12_months_after_initiation = alive_and_on_ARVS_at_12_months_after_initiation

    return [total_alive_and_on_ARVS_at_12_months_after_initiation.count, alive_and_on_ARVS_at_12_months_after_initiation_less_1.count,
    alive_and_on_ARVS_at_12_months_after_initiation_between_1_and_9.count,
    alive_and_on_ARVS_at_12_months_after_initiation_btwn_10_14_female.count,
    alive_and_on_ARVS_at_12_months_after_initiation_btwn_10_14_male.count,
    alive_and_on_ARVS_at_12_months_after_initiation_less_15_19_female.count,
    alive_and_on_ARVS_at_12_months_after_initiation_less_15_19_male.count,
    alive_and_on_ARVS_at_12_months_after_initiation_less_20_24_female.count,
    alive_and_on_ARVS_at_12_months_after_initiation_less_20_24_male.count,
    alive_and_on_ARVS_at_12_months_after_initiation_less_25_49_female.count,
    alive_and_on_ARVS_at_12_months_after_initiation_less_25_49_male.count,
    alive_and_on_ARVS_at_12_months_after_initiation_less_more_than_50_female.count,
    alive_and_on_ARVS_at_12_months_after_initiation_less_more_than_50_male.count,
    alive_and_on_ARVS_at_12_months_after_initiation_less_15_female.count,
    alive_and_on_ARVS_at_12_months_after_initiation_less_15_male.count,
    alive_and_on_ARVS_at_12_months_after_initiation_more_15_female.count,
    alive_and_on_ARVS_at_12_months_after_initiation_more_15_male.count]
end

def self.total_initiated_in_12_months(start_date, end_date, min_age = nil, max_age = nil, gender = [])
    total_initiated_in_12_months = ActiveRecord::Base.connection.select_all <<EOF
      SELECT *
      FROM earliest_start_date
      WHERE date_enrolled BETWEEN '2016-01-01' AND '#{end_date}';
EOF

    total_tota_initiated_in_12_months = []; total_initiated_in_12_months_less_1  = []; total_initiated_in_12_months_between_1_and_9 = []
    total_initiated_in_12_months_btwn_10_14_female  = []; total_initiated_in_12_months_btwn_10_14_male = []; total_initiated_in_12_months_less_15_19_female = []
    total_initiated_in_12_months_less_15_19_male = []; total_initiated_in_12_months_less_20_24_female = []; total_initiated_in_12_months_less_20_24_male = []
    total_initiated_in_12_months_less_25_49_female = []; total_initiated_in_12_months_less_25_49_male = []; total_initiated_in_12_months_less_more_than_50_female = []
    total_initiated_in_12_months_less_more_than_50_male = []; total_initiated_in_12_months_less_15_female = []; total_initiated_in_12_months_less_15_male = []
    total_initiated_in_12_months_more_15_female = []; total_initiated_in_12_months_more_15_male = []

    (total_initiated_in_12_months || []).each do |patient|
          if patient['age_at_initiation'].to_i <= 1
            total_initiated_in_12_months_less_1 << patient
          elsif patient['age_at_initiation'].to_i >= 2 && patient['age_at_initiation'].to_i <= 9
            total_initiated_in_12_months_between_1_and_9 << patient
          end

          if (patient['age_at_initiation'].to_i >= 10 && patient['age_at_initiation'].to_i <= 14)
            if (patient['gender'] == "M")
              total_initiated_in_12_months_btwn_10_14_male << patient
            else
              total_initiated_in_12_months_btwn_10_14_female << patient
            end
          end

          if (patient['age_at_initiation'].to_i >= 15 && patient['age_at_initiation'].to_i <= 19)
            if (patient['gender'] == "M")
              total_initiated_in_12_months_less_15_19_male << patient
            else
              total_initiated_in_12_months_less_15_19_female << patient
            end
          end

          if (patient['age_at_initiation'].to_i >= 20 && patient['age_at_initiation'].to_i <= 24)
            if (patient['gender'] == "M")
              total_initiated_in_12_months_less_20_24_male << patient
            else
              total_initiated_in_12_months_less_20_24_female << patient
            end
          end

          if (patient['age_at_initiation'].to_i >= 25 && patient['age_at_initiation'].to_i <= 49)
            if (patient['gender'] == "M")
              total_initiated_in_12_months_less_25_49_male << patient
            else
              total_initiated_in_12_months_less_25_49_female << patient
            end
          end

          if (patient['age_at_initiation'].to_i >= 50)
            if (patient['gender'] == "M")
              total_initiated_in_12_months_less_more_than_50_male << patient
            else
              total_initiated_in_12_months_less_more_than_50_female << patient
            end
          end

          if (patient['age_at_initiation'].to_i < 15)
            if (patient['gender'] == "M")
              total_initiated_in_12_months_less_15_male << patient
            else
              total_initiated_in_12_months_less_15_female << patient
            end
         end

          if (patient['age_at_initiation'].to_i >= 15)
            if (patient['gender'] ==  "M")
              total_initiated_in_12_months_more_15_male << patient
            else
              total_initiated_in_12_months_more_15_female << patient
            end
          end
    end

    total_total_initiated_in_12_months = total_initiated_in_12_months

    return [total_total_initiated_in_12_months.count, total_initiated_in_12_months_less_1.count,
    total_initiated_in_12_months_between_1_and_9.count,
    total_initiated_in_12_months_btwn_10_14_female.count,
    total_initiated_in_12_months_btwn_10_14_male.count,
    total_initiated_in_12_months_less_15_19_female.count,
    total_initiated_in_12_months_less_15_19_male.count,
    total_initiated_in_12_months_less_20_24_female.count,
    total_initiated_in_12_months_less_20_24_male.count,
    total_initiated_in_12_months_less_25_49_female.count,
    total_initiated_in_12_months_less_25_49_male.count,
    total_initiated_in_12_months_less_more_than_50_female.count,
    total_initiated_in_12_months_less_more_than_50_male.count,
    total_initiated_in_12_months_less_15_female.count,
    total_initiated_in_12_months_less_15_male.count,
    total_initiated_in_12_months_more_15_female.count,
    total_initiated_in_12_months_more_15_male.count]
end

def self.peads_receiving_art_cumulative(start_date, end_date, min_age = nil, max_age = nil, gender = [])
  art_defaulters = ActiveRecord::Base.connection.select_all <<EOF
        SELECT p.person_id AS patient_id, current_defaulter(p.person_id, '#{end_date}') AS def
        FROM earliest_start_date e
         Inner JOIN person p on p.person_id = e.patient_id and p.voided  = 0
        WHERE p.dead = 0
        AND date_enrolled <= '#{end_date}'
        GROUP BY p.person_id
        HAVING def = 1 AND current_state_for_program(p.person_id, 1, '#{end_date}') NOT IN (6, 2, 3)
EOF

    patient_ids = []
    (art_defaulters || []).each do |patient|
      patient_ids << patient['patient_id'].to_i
    end

  receiving_art_cumulative = ActiveRecord::Base.connection.select_all <<EOF
    SELECT e.patient_id, current_state_for_program(e.patient_id, 1, '#{end_date}') AS state, date_enrolled, age_at_initiation, gender
    FROM earliest_start_date e
    WHERE date_enrolled <= '#{end_date}'
    AND e.patient_id NOT IN (#{patient_ids.join(',')})
    GROUP BY e.patient_id
    HAVING state = 7;
EOF

    patients_alive_and_on_arvs_all_ages = []
    patients_alive_and_on_arvs_less_15_years = []
    patients_alive_and_on_arvs_less_bwtn_10_14_years = []
    patients_alive_and_on_arvs_only_14_years = []
    patients_alive_and_on_arvs_between_15_and_19_years = []
    patients_alive_and_on_arvs_more_than_20_years = []

    (receiving_art_cumulative || []).each do |patient|
      if (patient['age_at_initiation'].to_i < 15)
        patients_alive_and_on_arvs_less_15_years << patient
      end

      if (patient['age_at_initiation'].to_i >= 10 && patient['age_at_initiation'].to_i <= 14)
        patients_alive_and_on_arvs_less_bwtn_10_14_years << patient
      end

      if (patient['age_at_initiation'].to_i == 14)
        patients_alive_and_on_arvs_only_14_years << patient
      end

      if (patient['age_at_initiation'].to_i >= 15 && patient['age_at_initiation'].to_i <= 19)
        patients_alive_and_on_arvs_between_15_and_19_years << patient
      end

      if (patient['age_at_initiation'].to_i >= 20 )
        patients_alive_and_on_arvs_more_than_20_years << patient
      end
    end

    patients_alive_and_on_arvs_all_ages = receiving_art_cumulative

    return [patients_alive_and_on_arvs_all_ages.count,patients_alive_and_on_arvs_less_15_years.count,
            patients_alive_and_on_arvs_less_bwtn_10_14_years.count,
            patients_alive_and_on_arvs_only_14_years.count,
            patients_alive_and_on_arvs_between_15_and_19_years.count,
            patients_alive_and_on_arvs_more_than_20_years.count]
end

def self.patients_by_age_and_outcomes(start_date, end_date, patient_outcome, min_age = nil, max_age = nil, gender = [])
  if (max_age.blank? && min_age.blank?)
    condition = ""
  elsif (max_age.blank?)
    condition = "AND age_at_initiation >= #{min_age}"
  else
    condition = "AND age_at_initiation  BETWEEN #{min_age} and #{max_age}"
  end

  unless gender.blank?
    patients_outcome = ActiveRecord::Base.connection.select_all <<EOF
      select e.patient_id, e.earliest_start_date, e.date_enrolled, age_at_initiation,
             patient_outcome(e.patient_id, '#{end_date}') as outcome
      from earliest_start_date e
      where date_enrolled <= '#{end_date}'
      and gender = '#{gender}'   #{condition}
      Having outcome = '#{patient_outcome}';
EOF
  else
    patients_outcome = ActiveRecord::Base.connection.select_all <<EOF
      select e.patient_id, e.earliest_start_date, e.date_enrolled, age_at_initiation,
             patient_outcome(e.patient_id, '#{end_date}') as outcome
      from earliest_start_date e
      where date_enrolled <= '#{end_date}'
      and gender IN ('F', 'M')   #{condition}
      Having outcome = '#{patient_outcome}';
EOF
  end
  if patients_outcome.blank?
    result = 0
  else
    result = patients_outcome.count
  end
  return  result
end

start
