require File.dirname(__FILE__) + '/spec_helper'
require File.dirname(__FILE__) + '/../../init'

module SmartSlavesHelper
  def define_test_model(opts)
    SmartSlaveTest.class_eval do
      use_smart_slaves(opts)
    end
  end
  
  def create_fixture_record(klass, params)
    rec = klass.new(params)
    rec.id = params[:id]
    rec.save
  end
  
  def load_fixtures_data
    SmartSlaveTestOnSlave.connection.execute("DELETE FROM smart_slave_tests")
    create_fixture_record(SmartSlaveTestOnSlave, :name => 'slave_test1', :id => 1)
    create_fixture_record(SmartSlaveTestOnSlave, :name => 'slave_test2', :id => 2)
    create_fixture_record(SmartSlaveTestOnSlave, :name => 'slave_test3', :id => 3)

    SmartSlaveTest.connection.execute("DELETE FROM smart_slave_tests")
    create_fixture_record(SmartSlaveTest, :name => 'test1', :id => 1)
    create_fixture_record(SmartSlaveTest, :name => 'test2', :id => 2)
    create_fixture_record(SmartSlaveTest, :name => 'test3', :id => 3)
    create_fixture_record(SmartSlaveTest, :name => 'test4', :id => 4)
    create_fixture_record(SmartSlaveTest, :name => 'test5', :id => 5)
    create_fixture_record(SmartSlaveTest, :name => 'test6', :id => 6)
  end
end

describe SmartSlaves do
  include SmartSlavesHelper
  
  it "should raise an exception if no :slave_db parameter passed" do
    lambda {
      define_test_model
    }.should raise_error
  end
  
  it "should raise and exception if invalid :slave_db parameter passed" do
    lambda {
      define_test_model :slave_db => :crap
    }.should raise_error
  end

  it "should not raise and exception if all params are valid" do
    lambda {
      define_test_model :slave_db => :slave
    }.should_not raise_error
  end
end


describe SmartSlaves, "with use_smart_slaves" do
  extend SmartSlavesHelper
  define_test_model :slave_db => :slave

  load_fixtures_data

  before(:each) do
    puts "-----------------------------------------------------------------"
  end

  it "should retrieve and store in the cache our checkpoint id" do
    SmartSlaveTest.send(:checkpoint_value).should be_nil
    SmartSlaveTest.find(5).name.should == "test5"
    SmartSlaveTest.send(:checkpoint_value).should be(3)
  end
  
  it "should do finds on an appropriate servers (based on id)" do
    SmartSlaveTest.find(2).name.should == "slave_test2"
    SmartSlaveTest.find(3).name.should == "slave_test3"
    SmartSlaveTest.find(4).name.should == "test4"
    SmartSlaveTest.find(5).name.should == "test5"
  end
  
  it "should support conditioned find calls" do
    SmartSlaveTest.find(:first, :conditions => { :id => 2 }).should be_valid #.name.should == "slave_test2"
  end
  
  it "should run on the master when :on_master => true is in the options" do
    SmartSlaveTest.find(1, :on_master => true).name.should == "test1"
  end

  it "should run on the slave when :on_slave => true is in the options" do
    lambda { 
      SmartSlaveTest.find(5, :on_slave => true)
    }.should raise_error(ActiveRecord::RecordNotFound)
  end
end