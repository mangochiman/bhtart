User.current = User.find_by_username('admin')
Parent_path = '/home/pachawo/Documents/msf/'
require 'fastercsv'

def start
  references = get_references
  FasterCSV.foreach ("#{Parent_path}/TbPatientDrug.csv", :quote_char => '"', :col_sep => ',', :row_sep =>:auto) do |row|
    drug_name = references[row[4]]
    concept_id = 7754 #ever received arvs?
    value_coded = 1066 #default answer: no
    if drug_name.equal?("ARV")
      value_coded = 1065 #yes
    end
    puts ">>>> #{value_coded}"
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

start
