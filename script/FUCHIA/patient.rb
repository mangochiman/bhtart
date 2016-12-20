User.current = User.find_by_username('admin')
Parent_path = '/home/pachawo/Documents/msf/'
require 'fastercsv'

def start
  #returns a hash of references
  references = get_references
  FasterCSV.foreach("#{Parent_path}/TbPatient.csv", :quote_char => '"', :col_sep =>',', :row_sep =>:auto) do |row|
    names = row[9].split(' ')
    given_name = nil ; middle_name = nil ; family_name = nil

    (names || []).each_with_index do |name, i|
      next if name.blank?
      n = name.titleize.squish
      n = n.gsub('*','Unknown') 
      given_name = n if i == 0  
      if i == 1 and names.length > 2
	middle_name = n 
      else
        family_name = n if i == 1
      end
      family_name = n if i == 2
    end

    age_estimate = false
    gender = row[10].squish.to_i rescue 'Unknown'
    date_created = row[1] 
    date_created = get_proper_date(date_created)
    date_created = date_created.to_date.strftime("%Y-%m-%d 01:00:00") rescue Date.today.strftime("%Y-%m-%d 01:00:00")
    age_estimate_date_created = row[14] 
    date_of_death =get_proper_date(row[21]) unless row[21].blank?
    is_dead = row[20] rescue nil
    city_village = row[3] rescue nil
    occupation = row[4] rescue nil
    age = row[12]
    city_village = references[city_village]
    city_village = "Unknown" if city_village.blank?
    occupation = references[occupation]
    occupation = "Unknown" if occupation.blank?
    

    if row[11].blank?
      age_estimate = true
      if !age_estimate_date_created.blank? and !age.blank?
        age_estimate_date_created = get_proper_date(age_estimate_date_created)
        dob = Date.new(age_estimate_date_created.to_date.year - age.to_i, 7, 1)
     else
       dob = "1900-01-01"
     end
    else
      dob = get_proper_date(row[11])
    end

    unless gender == 'Unknown'
      gender = gender == 0 ? 'M' : 'F'
    end

    person = Person.find(row[0].to_i) rescue nil
    if person.blank?
      puts "Creating:#{row[0]} ................ "
      person = Person.new()
      person.birthdate_estimated = age_estimate
      person.birthdate = dob.to_date 
      person.dead = is_dead.to_i
      person.gender = gender unless gender == 'Unknown'
      person.death_date = date_of_death.to_date unless date_of_death.blank?
      person.date_created = date_created
      person.person_id = row[0].to_i
      person.save

     PersonName.create(:given_name => given_name, :family_name => family_name, :middle_name => middle_name, 
              :date_created => person.date_created, :person_id => person.id)
     PersonAddress.create(:person_id => person.id, :city_village => city_village, :date_created => person.date_created)
     PersonAttribute.create(:person_id => person.id, :value => occupation, :date_created => person.date_created, :person_attribute_type_id => 14)
     patient = Patient.new()
     patient.patient_id = person.id
     patient.date_created = person.date_created
     patient.save
     
   end
  end 
 
end

def get_proper_date (unfomatted_date)
  unfomatted_date = unfomatted_date.split("/") unless unfomatted_date.blank?
  year_of_birth = unfomatted_date[2].split(" ")
  year_of_birth = year_of_birth[0]
  current_year = Date.today.year.to_s

  if year_of_birth.to_i > current_year[-2..-1].to_i
    year = "19#{year_of_birth}"
  else
    year = "20#{year_of_birth}"
  end

  fomatted_date = "#{year}-#{unfomatted_date[0]}-#{unfomatted_date[1]}" 
end

def generate_date_of_birth(age, date_recorded)
  age = age.to_i
  date_of_birth = date_recorded.to_date - age.year
end

#function that loads csv file data into a hash
def get_references
  references_hash = {}
  FasterCSV.foreach("#{Parent_path}/TbReference.csv", :quote_char => '"', :col_sep =>',', :row_sep =>:auto) do |row|
    references_hash[row[0]] = row[6]
  end
  return references_hash
end
start
