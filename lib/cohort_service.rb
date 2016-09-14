class CohortService 

 #Newly registered in quarter 
 attr_accessor :total_registered, :transfer_in, :initiated_on_art_first_time, :re_initiated_on_art,
               :pregnant_females_all_ages, :non_pregnant_females, :children_below_24_months_at_art_initiation,
               :children_24_months_14_years_at_art_initiation, :adults_at_art_initiation, :unknown_age,
               :presumed_severe_hiv_disease_in_infants, :confirmed_hiv_infection_in_infants_pcr,   
               :tb_within_the_last_two_years, :current_episode_of_tb, :kaposis_sarcoma,
               :presumed_severe_hiv_disease_in_infants, :confirmed_hiv_infection_in_infants_pcr,   
               :who_stage_two, :children_12_23_months, :breastfeeding_mothers, :pregnant_women,
               :who_stage_three, :who_stage_four, :unknown_other_reason_outside_guidelines, :all_males


 #Cumulative ever registered
 attr_accessor :cum_total_registered, :cum_transfer_in, :cum_initiated_on_art_first_time, :cum_re_initiated_on_art,
               :cum_pregnant_females_all_ages, :cum_non_pregnant_females, :cum_children_below_24_months_at_art_initiation,
               :cum_children_24_months_14_years_at_art_initiation, :cum_adults_at_art_initiation, :cum_unknown_age,
               :cum_presumed_severe_hiv_disease_in_infants, :cum_confirmed_hiv_infection_in_infants_pcr,   
               :cum_tb_within_the_last_two_years, :cum_current_episode_of_tb, :cum_kaposis_sarcoma,
               :cum_presumed_severe_hiv_disease_in_infants, :cum_confirmed_hiv_infection_in_infants_pcr,   
               :cum_who_stage_two, :cum_children_12_23_months, :cum_breastfeeding_mothers, :cum_pregnant_women,
               :cum_who_stage_three, :cum_who_stage_four, :cum_unknown_other_reason_outside_guidelines, :cum_all_males

  #Primary Outcomes (Cumulative)
  attr_accessor :total_alive_and_on_art, :died_within_the_1st_month_of_art_initiation,
                :died_within_the_2nd_month_of_art_initiation, :died_within_the_3rd_month_of_art_initiation,
                :died_after_the_3rd_month_of_art_initiation, :died_total, :defaulted, :stopped_art,
                :transfered_out, :unknown_outcome

  #Secondary Outcomes (Cumulative)
  attr_accessor :one_a, :one_p, :two_a, :two_p, :three_a, :three_p, :four_a, :four_p, :five_a, :six_a,
                :seven_a, :eight_a, :nine_p, :unknown_regimen

  #Adherence
  attr_accessor :patients_with_0_6_doses_missed_at_their_last_visit, :patients_with_7_plus_doses_missed_at_their_last_visit

  #Current TB status,any form of TB
  attr_accessor :tb_not_suspected, :tb_suspected, :tbconfirmed_currently_not_yet_on_tb_treatment,
                :tb_confirmed_on_tb_treatment, :unknown_tb_status


  
  


                



  
	def initialize(name)
		@name = name
	end
end
