=begin
    Author: mangochiman
    Purpose:
          1. Simplify the process of setting up the new ART
    Start Date: 26/January/2017
    End Date: ??/??/????
=end
# quit unless our script gets one command line arguments

def easy_art_setup
  environment = ARGV[0]
  if (ARGV.length != 1)
    puts "_____________________________________________________________"
    puts "Not  the right number of arguments. One argument is required"
    puts "Usage: script/runner script/new_art_setup.rb environment. The environment can be development or production\n"
    puts "_____________________________________________________________"
    exit
  end

  if  !((ARGV[0].downcase == 'development') || (ARGV[0].downcase == 'production'))
    puts "_____________________________________________________________"
    puts "The environment should be development or production."
    puts "_____________________________________________________________"
    exit
  end
  
  puts "==========================SCRIPT STARTED============================================="
  username = YAML::load_file('config/database.yml')[environment]['username']
  password = YAML::load_file('config/database.yml')[environment]['password']
  database = YAML::load_file('config/database.yml')[environment]['database']
  host = YAML::load_file('config/database.yml')[environment]['host']

  `mysql -h #{host} -u #{username} -p#{password} #{database} < db/openmrs_metadata_1_7.sql`
  `mysql -h #{host} -u #{username} -p#{password} #{database} < db/revised_regimens.sql`
  `rake db:migrate`

end

easy_art_setup