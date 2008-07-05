require File.dirname(__FILE__) + '/spec_helper'
require File.dirname(__FILE__) + '/../../init'

module SmartSlavesHelper
  def define_test_model(opts)
    SmartSlaveTest.class_eval do
      use_smart_slaves(opts)
    end
  end
end

describe SmartSlaves do
  include SmartSlavesHelper
  
  it "should raise an exception if no :db parameter passed" do
    lambda {
      define_test_model
    }.should raise_error
  end
  
  it "should raise and exception if invalid :db parameter passed" do
    lambda {
      define_test_model :db => :crap
    }.should raise_error
  end

  it "should not raise and exception if all params are valid" do
    lambda {
      define_test_model :db => :slave
    }.should_not raise_error
  end
end


describe SmartSlaves, "with use_smart_slaves" do
  extend SmartSlavesHelper
  define_test_model :db => :slave
  
  before(:each) do
    puts "-----------------------------------------------------------------"
  end

  it "should retrieve and store in the cache our checkpoint id" do
    SmartSlaveTest.send(:checkpoint_value).should be_nil
    SmartSlaveTest.find(5)
    SmartSlaveTest.send(:checkpoint_value).should be(3)
  end  
end