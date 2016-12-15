User.current = User.find_by_username('admin')
Parent_path = '/home/mwatha/Desktop/msf/'
require 'fastercsv'

def start
   FasterCSV.foreach("#{Parent_path}/TbPatient.csv", :quote_char => '"', :col_sep =>',', :row_sep =>:auto) do |row|
    names = row[9].split(' ')
    given_name = names[0].titleize.squish rescue nil
    family_name = names.last.titleize.squish rescue nil
    given_name = given_name.gsub('*','') unless given_name.blank?
    family_name = family_name.gsub('*','') unless family_name.blank?
    gender = row[10].squish.to_i rescue 'Unknown'
    
    unless gender == 'Unknown'
      gender = gender == 0 ? 'M' : 'F'
    end

    

    puts ">>> #{given_name} #{family_name} (#{gender})"


   end  
end


start
