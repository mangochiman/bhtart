# This file should contain all the record creation needed to seed the database with its default values.
# The data can then be loaded with the rake db:seed (or created alongside the db with db:setup).
#
# Examples:
#
#   cities = City.create([{ :name => 'Chicago' }, { :name => 'Copenhagen' }])
#   Major.create(:name => 'Daley', :city => cities.first)

# Validation rules for cohort report
require 'fastercsv'
require 'spreadsheet'
puts "Adding validation rules for cohort reports"
FasterCSV.foreach('db/validation_rules.csv',
  :col_sep => ',', :headers => :first_row) do |row|

  expr = row['expr'] || ''
  desc = row['desc'].to_s
  type_id = row['type_id']
  next if desc.blank?
  check = ValidationRule.find_by_desc(desc)

  if check.blank?
    ValidationRule.create :expr => expr.strip, :desc => desc, :type_id => type_id
  else

  end
end

def load_cms_drugs
  cms_drugs = Spreadsheet.open "#{Rails.root}/script/cms.xls"
  sheet1 = cms_drugs.worksheet 0

  ActiveRecord::Base.transaction do
    sheet1.each 2 do |row|
      drug_name = row[0]
      drug_code = row[1]
      drug_inventory_id = row[2]
      pack_size = drug_name.split(/[^\d]/).last
      next if drug_inventory_id.blank?
      drug_cms = DrugCms.new
      drug_cms.drug_inventory_id = drug_inventory_id
      drug_cms.name = drug_name
      drug_cms.code = drug_code
      drug_cms.pack_size = pack_size
      drug_cms.save
    end
  end
end

load_cms_drugs
