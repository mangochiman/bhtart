
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
    $resultsOutput = File.open("./CDCDataExtractionAdditional_" + "#{facility_name}" + ".txt", "w")
    $resultsOutput  << "Total Receiong ART - Additional 2014............................................................................\n"
    $resultsOutput  << "  total_receiving_art_14_years_old_first: #{total_receiving_art_14_years_old_first}\n  total_receiving_art_less_1_year_first: #{total_receiving_art_less_1_year_first}\n  total_receiving_art_between_1_and_9_years_first: #{total_receiving_art_between_1_and_9_years_first}\n  total_receiving_art_10_14_years_first: #{total_receiving_art_10_14_years_first}\n "
    $resultsOutput  << "\nTotal Receiong ART - Additional 2015............................................................................\n"
    $resultsOutput  << "  total_receiving_art_14_years_old_second: #{total_receiving_art_14_years_old_second}\n  total_receiving_art_less_1_year_second: #{total_receiving_art_less_1_year_second}\n  total_receiving_art_between_1_and_9_years_second: #{total_receiving_art_between_1_and_9_years_second}\n  total_receiving_art_10_14_years_second: #{total_receiving_art_10_14_years_second}\n "
    $resultsOutput  << "\nTotal Receiong ART - Additional 2016.............................................................................\n"
    $resultsOutput  << "  total_receiving_art_14_years_old_third: #{total_receiving_art_14_years_old_third}\n  total_receiving_art_less_1_year_third: #{total_receiving_art_less_1_year_third}\n  total_receiving_art_between_1_and_9_years_third: #{total_receiving_art_between_1_and_9_years_third}\n  total_receiving_art_10_14_years_third: #{total_receiving_art_10_14_years_third}\n "
  end

  if CDCDataExtraction == 1
    $resultsOutput.close()
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

start
