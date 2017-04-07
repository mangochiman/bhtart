
require 'fastercsv'

Source_db = YAML.load(File.open(File.join(RAILS_ROOT, "config/database.yml"), "r"))['production']["database"]
CDCDataExtraction = 1

def start
  facility_name = GlobalProperty.find_by_sql("select property_value from global_property where property = 'current_health_center_name'").map(&:property_value).first

  start_date = "2016-01-01".to_date
  end_date = "2016-03-31".to_date
  puts "CDC ART data extraction (Additional)................................................................................................"

  puts "Receving ART (Total registered) Additional 2014....................................................................................."
  end_date_first = '2014-09-30'.to_date
  total_receiving_art_14_years_old_first = receiving_art_cumulative(start_date, end_date_first, 14, 14)
  total_receiving_art_less_1_year_first = receiving_art_cumulative(start_date, end_date_first, 0, 1)
  total_receiving_art_between_1_and_9_years_first = receiving_art_cumulative(start_date, end_date_first, 2, 9)
  total_receiving_art_10_14_years_first = receiving_art_cumulative(start_date, end_date_first, 10, 14)

  puts "Receving ART (Total registered) Additional 2015....................................................................................."
  end_date_second = '2015-09-30'.to_date
  total_receiving_art_14_years_old_second = receiving_art_cumulative(start_date, end_date_second, 14, 14)
  total_receiving_art_less_1_year_second = receiving_art_cumulative(start_date, end_date_second, 0, 1)
  total_receiving_art_between_1_and_9_years_second = receiving_art_cumulative(start_date, end_date_second, 2, 9)
  total_receiving_art_10_14_years_second = receiving_art_cumulative(start_date, end_date_second, 10, 14)

  puts "Receving ART (Total registered) Additional 2016....................................................................................."
  end_date_third = '2016-09-30'.to_date
  total_receiving_art_14_years_old_third = receiving_art_cumulative(start_date, end_date_third, 14, 14)
  total_receiving_art_less_1_year_third = receiving_art_cumulative(start_date, end_date_third, 0, 1)
  total_receiving_art_between_1_and_9_years_third = receiving_art_cumulative(start_date, end_date_third, 2, 9)
  total_receiving_art_10_14_years_third = receiving_art_cumulative(start_date, end_date_third, 10, 14)

  if CDCDataExtraction == 1
    file = "/home/deliwe/Desktop/cdc_data_extraction/cdc_art_data_extraction_" + "#{facility_name}" + ".csv"

    FasterCSV.open( file, 'w' ) do |csv|
      csv << ["facility_name","Category","CDC Indicator", "Total"]
      csv << ["#{facility_name}", "Receving ART (Total registered) Additional 2014", "total_receiving_art_14_years_old", "#{total_receiving_art_14_years_old_first}"]
      csv << ["#{facility_name}", "Receving ART (Total registered) Additional 2014", "total_receiving_art_less_1_year_old", "#{total_receiving_art_less_1_year_first}"]
      csv << ["#{facility_name}", "Receving ART (Total registered) Additional 2014", "total_receiving_art_between_1_and_9_years_old", "#{total_receiving_art_between_1_and_9_years_first}"]
      csv << ["#{facility_name}", "Receving ART (Total registered) Additional 2014", "total_receiving_art_10_14_years_old", "#{total_receiving_art_10_14_years_first}"]
      csv << ["","","", ""]
      csv << ["#{facility_name}", "Receving ART (Total registered) Additional 2015", "total_receiving_art_14_years_old", "#{total_receiving_art_14_years_old_second}"]
      csv << ["#{facility_name}", "Receving ART (Total registered) Additional 2015", "total_receiving_art_less_1_year_old", "#{total_receiving_art_less_1_year_second}"]
      csv << ["#{facility_name}", "Receving ART (Total registered) Additional 2015", "total_receiving_art_between_1_and_9_years_old", "#{total_receiving_art_between_1_and_9_years_second}"]
      csv << ["#{facility_name}", "Receving ART (Total registered) Additional 2015", "total_receiving_art_10_14_years_old", "#{total_receiving_art_10_14_years_second}"]
      csv << ["","","", ""]
      csv << ["#{facility_name}", "Receving ART (Total registered) Additional 2016", "total_receiving_art_14_years_old", "#{total_receiving_art_14_years_old_third}"]
      csv << ["#{facility_name}", "Receving ART (Total registered) Additional 2016", "total_receiving_art_less_1_year_old", "#{total_receiving_art_less_1_year_third}"]
      csv << ["#{facility_name}", "Receving ART (Total registered) Additional 2016", "total_receiving_art_between_1_and_9_years_old", "#{total_receiving_art_between_1_and_9_years_third}"]
      csv << ["#{facility_name}", "Receving ART (Total registered) Additional 2016", "total_receiving_art_10_14_years_old", "#{total_receiving_art_10_14_years_third}"]
    end
  end

  if CDCDataExtraction == 1
    #$resultsOutput.close()
  end
end

def self.receiving_art_cumulative(start_date, end_date, min_age = nil, max_age = nil, gender = [])
  if (max_age.blank? && min_age.blank?)
    condition = ""
  elsif (max_age.blank?)
    condition = "AND age_at_initiation >= #{min_age}"
  else
    condition = "AND age_at_initiation  BETWEEN #{min_age} and #{max_age}"
  end

  art_defaulters = ActiveRecord::Base.connection.select_all <<EOF
        SELECT p.person_id AS patient_id, current_defaulter(p.person_id, '#{end_date}') AS def
        FROM earliest_start_date e
         Inner JOIN person p on p.person_id = e.patient_id and p.voided  = 0
        WHERE p.dead = 0
        GROUP BY p.person_id
        HAVING def = 1 AND current_state_for_program(p.person_id, 1, '#{end_date}') NOT IN (6, 2, 3)
EOF

    patient_ids = []
    (art_defaulters || []).each do |patient|
      patient_ids << patient['patient_id'].to_i
    end

  unless gender.blank?
    receiving_art_cumulative = ActiveRecord::Base.connection.select_all <<EOF
      SELECT e.patient_id, current_state_for_program(e.patient_id, 1, '#{end_date}') AS state, date_enrolled, age_at_initiation, gender
      FROM earliest_start_date e
      WHERE date_enrolled <= '#{end_date}'
      AND e.patient_id NOT IN (#{patient_ids.join(',')})
      AND gender = '#{gender}'
      #{condition}
      GROUP BY e.patient_id
      HAVING state = 7;
EOF
  else
    receiving_art_cumulative = ActiveRecord::Base.connection.select_all <<EOF
      SELECT e.patient_id, current_state_for_program(e.patient_id, 1, '#{end_date}') AS state, date_enrolled, age_at_initiation, gender
      FROM earliest_start_date e
      WHERE date_enrolled <= '#{end_date}'
      AND e.patient_id NOT IN (#{patient_ids.join(',')})
      AND gender IN ('F','M')
      #{condition}
      GROUP BY e.patient_id
      HAVING state = 7;
EOF
  end

  if receiving_art_cumulative.blank?
    result = 0
  else
    result = receiving_art_cumulative.count
  end
  return  result
end

start
