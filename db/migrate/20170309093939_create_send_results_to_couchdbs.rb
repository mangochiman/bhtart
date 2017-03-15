class CreateSendResultsToCouchdbs < ActiveRecord::Migration
  def self.up
    create_table :send_results_to_couchdbs do |t|

      t.timestamps
    end
  end

  def self.down
    drop_table :send_results_to_couchdbs
  end
end
