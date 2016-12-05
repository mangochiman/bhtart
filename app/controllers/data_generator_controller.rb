class DataGeneratorController < ApplicationController 
  
  def patients_without_any_encs
    render :text => DataGeneratorService.list_of_patients_without_any_encounters.to_json and return 
  end

end
