class CreateSmartSlaveTests < ActiveRecord::Migration
  def self.up
    create_table :smart_slave_tests do |t|
      t.string :name
      t.timestamps
    end
  end

  def self.down
    drop_table :smart_slave_tests
  end
end
