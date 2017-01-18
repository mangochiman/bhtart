
Source_db = YAML.load(File.open(File.join(RAILS_ROOT, "config/database.yml"), "r"))['production']["database"]
CDCDataExtraction = 1

def start
  facility_name = GlobalProperty.find_by_sql("select property_value from global_property where property = 'current_health_center_name'").map(&:property_value).first

  start_date = "2016-01-01".to_date
  end_date = "2016-03-31".to_date
  puts "CDC ART data extraction............................................................................................"

  puts "New on ART (Newly registered)......................................................................................"
  total_new_on_art = new_on_art(start_date, end_date, nil, nil)
  new_on_art_less_1 = new_on_art(start_date, end_date, 0, 1)
  new_on_art_between_1_and_9 = new_on_art(start_date, end_date, 2, 9)
  new_on_art_btwn_10_14_female = new_on_art(start_date, end_date, 10, 14, 'F')
  new_on_art_btwn_10_14_male = new_on_art(start_date, end_date, 10, 14, 'M')
  new_on_art_less_15_19_female = new_on_art(start_date, end_date, 15, 19, 'F')
  new_on_art_less_15_19_male = new_on_art(start_date, end_date, 15, 19, 'M')
  new_on_art_less_20_24_female = new_on_art(start_date, end_date, 20, 24, 'F')
  new_on_art_less_20_24_male = new_on_art(start_date, end_date, 20, 24, 'M')
  new_on_art_less_25_49_female = new_on_art(start_date, end_date, 25, 49, 'F')
  new_on_art_less_25_49_male = new_on_art(start_date, end_date, 25, 49, 'M')
  new_on_art_less_more_than_50_female = new_on_art(start_date, end_date, 50, nil, 'F')
  new_on_art_less_more_than_50_male = new_on_art(start_date, end_date, 50, nil, 'M')
  new_on_art_less_15_female = new_on_art(start_date, end_date, 0, 14, 'F')
  new_on_art_less_15_male = new_on_art(start_date, end_date, 0, 14, 'M')
  new_on_art_more_15_female = new_on_art(start_date, end_date,15, nil, 'F')
  new_on_art_more_15_male = new_on_art(start_date, end_date,15, nil, 'M')

  puts "Receving ART (Total registered- Cumulative)........................................................................................."
  total_receiving_art_cumulative = receiving_art_cumulative(start_date, end_date, nil, nil)
  receiving_art_cumulative_less_1 = receiving_art_cumulative(start_date, end_date, 0, 1)
  receiving_art_cumulative_between_1_and_9 = receiving_art_cumulative(start_date, end_date, 2, 9)
  receiving_art_cumulative_btwn_10_14_female = receiving_art_cumulative(start_date, end_date, 10, 14, 'F')
  receiving_art_cumulative_btwn_10_14_male = receiving_art_cumulative(start_date, end_date, 10, 14, 'M')
  receiving_art_cumulative_less_15_19_female = receiving_art_cumulative(start_date, end_date, 15, 19, 'F')
  receiving_art_cumulative_less_15_19_male = receiving_art_cumulative(start_date, end_date, 15, 19, 'M')
  receiving_art_cumulative_less_20_24_female = receiving_art_cumulative(start_date, end_date, 20, 24, 'F')
  receiving_art_cumulative_less_20_24_male = receiving_art_cumulative(start_date, end_date, 20, 24, 'M')
  receiving_art_cumulative_less_25_49_female = receiving_art_cumulative(start_date, end_date, 25, 49, 'F')
  receiving_art_cumulative_less_25_49_male = receiving_art_cumulative(start_date, end_date, 25, 49, 'M')
  receiving_art_cumulative_less_more_than_50_female = receiving_art_cumulative(start_date, end_date, 50, nil, 'F')
  receiving_art_cumulative_less_more_than_50_male = receiving_art_cumulative(start_date, end_date, 50, nil, 'M')
  receiving_art_cumulative_less_15_female = receiving_art_cumulative(start_date, end_date, 0, 14, 'F')
  receiving_art_cumulative_less_15_male = receiving_art_cumulative(start_date, end_date, 0, 14, 'M')
  receiving_art_cumulative_more_15_female = receiving_art_cumulative(start_date, end_date,15, nil, 'F')
  receiving_art_cumulative_more_15_male = receiving_art_cumulative(start_date, end_date,15, nil, 'M')

  puts "New PLHIV with screened TB status(Newly registered).............................................."
  total_plhiv_screened_tb_status = plhiv_screened_tb_status(start_date, end_date, nil, nil)
  #plhiv_screened_tb_status_less_1 = plhiv_screened_tb_status(start_date, end_date, 0, 1)
  #plhiv_screened_tb_status_between_1_and_9 = plhiv_screened_tb_status(start_date, end_date, 2, 9)
  #plhiv_screened_tb_status_btwn_10_14_female = plhiv_screened_tb_status(start_date, end_date, 10, 14, 'F')
  #plhiv_screened_tb_status_btwn_10_14_male = plhiv_screened_tb_status(start_date, end_date, 10, 14, 'M')
  #plhiv_screened_tb_status_less_15_19_female = plhiv_screened_tb_status(start_date, end_date, 15, 19, 'F')
  #plhiv_screened_tb_status_less_15_19_male = plhiv_screened_tb_status(start_date, end_date, 15, 19, 'M')
  #plhiv_screened_tb_status_less_20_24_female = plhiv_screened_tb_status(start_date, end_date, 20, 24, 'F')
  #plhiv_screened_tb_status_less_20_24_male = plhiv_screened_tb_status(start_date, end_date, 20, 24, 'M')
  #plhiv_screened_tb_status_less_25_49_female = plhiv_screened_tb_status(start_date, end_date, 25, 49, 'F')
  #plhiv_screened_tb_status_less_25_49_male = plhiv_screened_tb_status(start_date, end_date, 25, 49, 'M')
  #plhiv_screened_tb_status_less_more_than_50_female = plhiv_screened_tb_status(start_date, end_date, 50, nil, 'F')
  #plhiv_screened_tb_status_less_more_than_50_male = plhiv_screened_tb_status(start_date, end_date, 50, nil, 'M')
  plhiv_screened_tb_status_less_15_female = plhiv_screened_tb_status(start_date, end_date, 0, 14, 'F')
  plhiv_screened_tb_status_less_15_male = plhiv_screened_tb_status(start_date, end_date, 0, 14, 'M')
  plhiv_screened_tb_status_more_15_female = plhiv_screened_tb_status(start_date, end_date,15, nil, 'F')
  plhiv_screened_tb_status_more_15_male = plhiv_screened_tb_status(start_date, end_date,15, nil, 'M')

  puts "Total alive and on ARVS at 12 months after initiation.............................................."
  total_alive_and_on_ARVS_at_12_months_after_initiation = alive_and_on_ARVS_at_12_months_after_initiation(start_date, end_date, nil, nil)
  alive_and_on_ARVS_at_12_months_after_initiation_less_1 = alive_and_on_ARVS_at_12_months_after_initiation(start_date, end_date, 0, 1)
  alive_and_on_ARVS_at_12_months_after_initiation_between_1_and_9 = alive_and_on_ARVS_at_12_months_after_initiation(start_date, end_date, 2, 9)
  alive_and_on_ARVS_at_12_months_after_initiation_btwn_10_14_female = alive_and_on_ARVS_at_12_months_after_initiation(start_date, end_date, 10, 14, 'F')
  alive_and_on_ARVS_at_12_months_after_initiation_btwn_10_14_male = alive_and_on_ARVS_at_12_months_after_initiation(start_date, end_date, 10, 14, 'M')
  alive_and_on_ARVS_at_12_months_after_initiation_less_15_19_female = alive_and_on_ARVS_at_12_months_after_initiation(start_date, end_date, 15, 19, 'F')
  alive_and_on_ARVS_at_12_months_after_initiation_less_15_19_male = alive_and_on_ARVS_at_12_months_after_initiation(start_date, end_date, 15, 19, 'M')
  alive_and_on_ARVS_at_12_months_after_initiation_less_20_24_female = alive_and_on_ARVS_at_12_months_after_initiation(start_date, end_date, 20, 24, 'F')
  alive_and_on_ARVS_at_12_months_after_initiation_less_20_24_male = alive_and_on_ARVS_at_12_months_after_initiation(start_date, end_date, 20, 24, 'M')
  alive_and_on_ARVS_at_12_months_after_initiation_less_25_49_female = alive_and_on_ARVS_at_12_months_after_initiation(start_date, end_date, 25, 49, 'F')
  alive_and_on_ARVS_at_12_months_after_initiation_less_25_49_male = alive_and_on_ARVS_at_12_months_after_initiation(start_date, end_date, 25, 49, 'M')
  alive_and_on_ARVS_at_12_months_after_initiation_less_more_than_50_female = alive_and_on_ARVS_at_12_months_after_initiation(start_date, end_date, 50, nil, 'F')
  alive_and_on_ARVS_at_12_months_after_initiation_less_more_than_50_male = alive_and_on_ARVS_at_12_months_after_initiation(start_date, end_date, 50, nil, 'M')
  alive_and_on_ARVS_at_12_months_after_initiation_less_15_female = alive_and_on_ARVS_at_12_months_after_initiation(start_date, end_date, 0, 14, 'F')
  alive_and_on_ARVS_at_12_months_after_initiation_less_15_male = alive_and_on_ARVS_at_12_months_after_initiation(start_date, end_date, 0, 14, 'M')
  alive_and_on_ARVS_at_12_months_after_initiation_more_15_female = alive_and_on_ARVS_at_12_months_after_initiation(start_date, end_date,15, nil, 'F')
  alive_and_on_ARVS_at_12_months_after_initiation_more_15_male = alive_and_on_ARVS_at_12_months_after_initiation(start_date, end_date,15, nil, 'M')

  puts "Total initiated in 12 months........................................................................."
  total_total_initiated_in_12_months = total_initiated_in_12_months(start_date, end_date, nil, nil)
  total_initiated_in_12_months_less_1 = total_initiated_in_12_months(start_date, end_date, 0, 1)
  total_initiated_in_12_months_between_1_and_9 = total_initiated_in_12_months(start_date, end_date, 2, 9)
  total_initiated_in_12_months_btwn_10_14_female = total_initiated_in_12_months(start_date, end_date, 10, 14, 'F')
  total_initiated_in_12_months_btwn_10_14_male = total_initiated_in_12_months(start_date, end_date, 10, 14, 'M')
  total_initiated_in_12_months_less_15_19_female = total_initiated_in_12_months(start_date, end_date, 15, 19, 'F')
  total_initiated_in_12_months_less_15_19_male = total_initiated_in_12_months(start_date, end_date, 15, 19, 'M')
  total_initiated_in_12_months_less_20_24_female = total_initiated_in_12_months(start_date, end_date, 20, 24, 'F')
  total_initiated_in_12_months_less_20_24_male = total_initiated_in_12_months(start_date, end_date, 20, 24, 'M')
  total_initiated_in_12_months_less_25_49_female = total_initiated_in_12_months(start_date, end_date, 25, 49, 'F')
  total_initiated_in_12_months_less_25_49_male = total_initiated_in_12_months(start_date, end_date, 25, 49, 'M')
  total_initiated_in_12_months_less_more_than_50_female = total_initiated_in_12_months(start_date, end_date, 50, nil, 'F')
  total_initiated_in_12_months_less_more_than_50_male = total_initiated_in_12_months(start_date, end_date, 50, nil, 'M')
  total_initiated_in_12_months_less_15_female = total_initiated_in_12_months(start_date, end_date, 0, 14, 'F')
  total_initiated_in_12_months_less_15_male = total_initiated_in_12_months(start_date, end_date, 0, 14, 'M')
  total_initiated_in_12_months_more_15_female = total_initiated_in_12_months(start_date, end_date,15, nil, 'F')
  total_initiated_in_12_months_more_15_male = total_initiated_in_12_months(start_date, end_date,15, nil, 'M')

  puts "PEADs with outcomes........................................................................."
  patients_alive_and_on_arvs_all_ages = receiving_art_cumulative(start_date, end_date)
  cumulative_children_less_15_years = receiving_art_cumulative(start_date, end_date, min_age = 0, max_age = 14)
  patients_alive_and_on_arvs_less_15_years = receiving_art_cumulative(start_date, end_date, min_age = 0, max_age = 15)
  patients_alive_and_on_arvs_less_bwtn_10_14_years = receiving_art_cumulative(start_date, end_date, min_age = 10, max_age = 14)
  patients_alive_and_on_arvs_only_14_years = receiving_art_cumulative(start_date, end_date, min_age = 14, max_age = 14)
  patients_alive_and_on_arvs_between_15_and_19_years = receiving_art_cumulative(start_date, end_date, min_age = 15, max_age = 19)
  patients_alive_and_on_arvs_more_than_20_years = receiving_art_cumulative(start_date, end_date, min_age = 20, max_age = nil)
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
    #$resultsOutput  << "total_plhiv_screened_tb_status: #{total_plhiv_screened_tb_status}\n plhiv_screened_tb_status_less_1: #{plhiv_screened_tb_status_less_1}\n plhiv_screened_tb_status_between_1_and_9: #{plhiv_screened_tb_status_between_1_and_9}\n plhiv_screened_tb_status_btwn_10_14_female: #{plhiv_screened_tb_status_btwn_10_14_female}\n plhiv_screened_tb_status_btwn_10_14_male: #{plhiv_screened_tb_status_btwn_10_14_male}\n plhiv_screened_tb_status_less_15_19_female: #{plhiv_screened_tb_status_less_15_19_female}\n plhiv_screened_tb_status_less_15_19_male: #{plhiv_screened_tb_status_less_15_19_male}\n plhiv_screened_tb_status_less_20_24_female: #{plhiv_screened_tb_status_less_20_24_female}\n plhiv_screened_tb_status_less_20_24_male: #{plhiv_screened_tb_status_less_20_24_male}\n plhiv_screened_tb_status_less_25_49_female: #{plhiv_screened_tb_status_less_25_49_female}\n plhiv_screened_tb_status_less_25_49_male: #{plhiv_screened_tb_status_less_25_49_male}\n plhiv_screened_tb_status_less_more_than_50_female: #{plhiv_screened_tb_status_less_more_than_50_female}\n plhiv_screened_tb_status_less_more_than_50_male: #{plhiv_screened_tb_status_less_more_than_50_male}\n plhiv_screened_tb_status_less_15_female: #{plhiv_screened_tb_status_less_15_female}\n plhiv_screened_tb_status_less_15_male: #{plhiv_screened_tb_status_less_15_male}\n plhiv_screened_tb_status_more_15_female: #{plhiv_screened_tb_status_more_15_female}\n plhiv_screened_tb_status_more_15_male: #{plhiv_screened_tb_status_more_15_male}\n"
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

def self.new_on_art(start_date, end_date, min_age = nil, max_age = nil, gender = [])
  if (max_age.blank? && min_age.blank?)
    condition = ""
  elsif (max_age.blank?)
    condition = "AND age_at_initiation >= #{min_age}"
  else
    condition = "AND age_at_initiation  BETWEEN #{min_age} and #{max_age}"
  end

  unless gender.blank?
    new_on_art = ActiveRecord::Base.connection.select_all <<EOF
      SELECT * FROM earliest_start_date
      WHERE date_enrolled BETWEEN '#{start_date}' AND '#{end_date}'
      #{condition}
      AND gender = '#{gender}';
EOF
  else
    new_on_art = ActiveRecord::Base.connection.select_all <<EOF
      SELECT * FROM earliest_start_date
      WHERE date_enrolled BETWEEN '#{start_date}' AND '#{end_date}'
      AND gender IN ('F', 'M')
      #{condition};
EOF
  end

  if new_on_art.blank?
    result = 0
  else
    result = new_on_art.count
  end
  return  result
end

def self.receiving_art_cumulative(start_date, end_date, min_age = nil, max_age = nil, gender = [])
  if (max_age.blank? && min_age.blank?)
    condition = ""
  elsif (max_age.blank?)
    condition = "AND age_at_initiation >= #{min_age}"
  else
    condition = "AND age_at_initiation  BETWEEN #{min_age} and #{max_age}"
  end

  unless gender.blank?
    receiving_art_cumulative = ActiveRecord::Base.connection.select_all <<EOF
      select * from earliest_start_date
      where date_enrolled <= '#{end_date}'
      AND gender = '#{gender}'
      #{condition};
EOF
  else
    receiving_art_cumulative = ActiveRecord::Base.connection.select_all <<EOF
      select * from earliest_start_date
      where date_enrolled <= '#{end_date}'
      AND gender IN ('F', 'M')
      #{condition};
EOF
  end

  if receiving_art_cumulative.blank?
    result = 0
  else
    result = receiving_art_cumulative.count
  end
  return  result
end

def self.plhiv_screened_tb_status(start_date, end_date, min_age = nil, max_age = nil, gender = [])
  if (max_age.blank? && min_age.blank?)
    condition = ""
  elsif (max_age.blank?)
    condition = "AND age_at_initiation >= #{min_age}"
  else
    condition = "AND age_at_initiation  BETWEEN #{min_age} and #{max_age}"
  end

  unless gender.blank?
    plhiv_screened_tb_status = ActiveRecord::Base.connection.select_all <<EOF
      select e.* from earliest_start_date e
       inner join obs o on o.person_id = e.patient_id and o.concept_id = 7459 and o.voided = 0
      where e.date_enrolled <= '#{end_date}'
      and o.value_coded in (7456, 7458)
      and e.gender = '#{gender}'
      and DATE(o.obs_datetime) < '#{end_date}'
      #{condition};
EOF
  else
    plhiv_screened_tb_status = ActiveRecord::Base.connection.select_all <<EOF
      select e.* from earliest_start_date e
       inner join obs o on o.person_id = e.patient_id and o.concept_id = 7459 and o.voided = 0
      where e.date_enrolled <= '#{end_date}'
      and o.value_coded in (7456, 7458)
      and e.gender in ('F','M')
      and DATE(o.obs_datetime) < '#{end_date}'
      #{condition};
EOF
  end

  if plhiv_screened_tb_status.blank?
    result = 0
  else
    result = plhiv_screened_tb_status.count
  end
  return  result
end

def self.alive_and_on_ARVS_at_12_months_after_initiation(start_date, end_date, min_age = nil, max_age = nil, gender = [])
  if (max_age.blank? && min_age.blank?)
    condition = ""
  elsif (max_age.blank?)
    condition = "AND age_at_initiation >= #{min_age}"
  else
    condition = "AND age_at_initiation  BETWEEN #{min_age} and #{max_age}"
  end

  unless gender.blank?
    alive_and_on_ARVS_at_12_months_after_initiation = ActiveRecord::Base.connection.select_all <<EOF
      select
          *,
          TIMESTAMPDIFF(month, date(date_enrolled), date('#{end_date}')) as period_on_art,
          patient_outcome(patient_id, '#{end_date}') as outcome
      from earliest_start_date
      where date_enrolled <= '#{end_date}' AND gender = '#{gender}'   #{condition}
      having period_on_art >= 12 and outcome <> 'Patient died';
EOF
  else
    alive_and_on_ARVS_at_12_months_after_initiation = ActiveRecord::Base.connection.select_all <<EOF
      select
          *,
          TIMESTAMPDIFF(month, date(date_enrolled), date('#{end_date}')) as period_on_art,
          patient_outcome(patient_id, '#{end_date}') as outcome
      from earliest_start_date
      where date_enrolled <= '#{end_date}' AND gender IN ('F', 'M')   #{condition}
      having period_on_art >= 12 and outcome <> 'Patient died';
EOF
  end
  if alive_and_on_ARVS_at_12_months_after_initiation.blank?
    result = 0
  else
    result = alive_and_on_ARVS_at_12_months_after_initiation.count
  end
  return  result
end

def self.total_initiated_in_12_months(start_date, end_date, min_age = nil, max_age = nil, gender = [])
  if (max_age.blank? && min_age.blank?)
    condition = ""
  elsif (max_age.blank?)
    condition = "AND age_at_initiation >= #{min_age}"
  else
    condition = "AND age_at_initiation  BETWEEN #{min_age} and #{max_age}"
  end

  unless gender.blank?
    patients_by_age_groups = ActiveRecord::Base.connection.select_all <<EOF
    select
        *,
        TIMESTAMPDIFF(month, date(date_enrolled), date('#{end_date}')) as period_on_art
    from earliest_start_date
    where date_enrolled <= '#{end_date}' AND gender = '#{gender}'   #{condition}
    having period_on_art >= 12;
EOF
  else
    patients_by_age_groups = ActiveRecord::Base.connection.select_all <<EOF
    select
        *,
        TIMESTAMPDIFF(month, date(date_enrolled), date('#{end_date}')) as period_on_art
    from earliest_start_date
    where date_enrolled <= '#{end_date}' AND gender IN ('F', 'M')   #{condition}
    having period_on_art >= 12;
EOF
  end
  if patients_by_age_groups.blank?
    result = 0
  else
    result = patients_by_age_groups.count
  end
  return  result
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
