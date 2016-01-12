class CreateDrugCms < ActiveRecord::Migration
  def self.up
    create_table :drug_cms, :id => false  do |t|
      t.integer :drug_inventory_id
      t.string :name
      t.string :code
      t.integer :pack_size
      t.integer :voided, :default => 0, :limit => 1
      t.integer :voided_by , :limit => 11
      t.datetime :date_voided
      t.string :void_reason, :limit => 225
      t.timestamps
    end
  end

  def self.down
    drop_table :drug_cms
  end
end
