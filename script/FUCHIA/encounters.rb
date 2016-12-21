User.current = User.find_by_username('admin')
Parent_path = '/home/pachawo/Documents/msf/'
require 'fastercsv'

def start
  references = get_references
  FasterCSV.foreach ("#{Parent_path}/TbPatientDrug.csv", :quote_char => '"', :col_sep => ',', :row_sep =>:auto) do |row|
    patient_id = row[3]
    clinic_registration(patient_id)
  end
end

#function that loads csv file data into a hash
def get_references
  references_hash = {}
  FasterCSV.foreach("#{Parent_path}/TbReference.csv", :quote_char => '"', :col_sep =>',', :row_sep =>:auto) do |row|
    references_hash[row[0]] = row[6]
  end
  return references_hash
end

def clinic_registration(patient_id)
  encounter_type_id = EncounterType.find_by_name("HIV CLINIC REGISTRATION").id
  encounter_id = Encounter.find_by_sql("SELECT encounter_id FROM encounter WHERE encounter_type = #{encounter_type_id} and patient_id = #{patient_id}").first.try(:encounter_id)
  concept_id = ConceptName.find_by_name("follow up agreement").concept_id
end
start
