require 'spec_helper'

describe Cassandra::Mapper::Utility::Config do
  subject do
    described_class.new do
      key :key1, :key2
      subkey :subkey1, :subkey2
      type :field, :integer
    end
  end

  its(:key)    { should == [:key1, :key2] }
  its(:subkey) { should == [:subkey1, :subkey2] }
  its(:types)  { should == { field: :integer }}
end
