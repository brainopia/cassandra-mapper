require 'spec_helper'

describe Cassandra::Mapper::Config do
  subject do
    described_class.new do
      key :field1, :field2
      types field: :integer
    end
  end

  its(:key)     { should == :field1 }
  its(:subkeys) { should == [:field2] }
  its(:types)   { should == { field: :integer }}
end
