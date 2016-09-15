class CreateGeneralSets < ActiveRecord::Migration
  def self.up
    create_table :general_sets do |t|

      t.timestamps
    end
  end

  def self.down
    drop_table :general_sets
  end
end
