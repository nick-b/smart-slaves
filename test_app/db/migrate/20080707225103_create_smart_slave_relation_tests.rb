class CreateSmartSlaveRelationTests < ActiveRecord::Migration
  def self.up
    create_table :smart_slave_relation_tests do |t|
      t.integer :smart_slave_test_id
      t.string :name
      t.timestamps
    end
  end

  def self.down
    drop_table :smart_slave_relation_tests
  end
end
