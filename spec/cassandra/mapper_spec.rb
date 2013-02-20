require 'spec_helper'

describe Cassandra::Mapper do
  subject do
    described_class.new :test_mapper, table, &definition
  end

  before :each do
    unless subject.keyspace.column_families.keys.include? table.to_s
      subject.migrate
    end
  end

  context 'two keys' do
    let(:table) { :two_keys }
    let(:definition) do
      proc do
        key :field1, :field2
      end
    end

    let(:field1) { 1 }
    let(:field2) { 2 }
    let(:field3) { 3 }

    it '#insert only keys' do
      subject.insert field1: field1, field2: field2
      composite = Cassandra::Composite.new field1.to_s
      result = subject.keyspace.get table, field2.to_s,
        start: composite, finish: composite
      result.should be_empty
    end

    it '#inserts keys with data' do
      subject.insert field1: field1, field2: field2, field3: field3
      composite = Cassandra::Composite.new field1.to_s
      result = subject.keyspace.get table, field2.to_s,
        start: composite, finish: composite
      result.should == { 'field3' => field3 }
    end
  end
end
