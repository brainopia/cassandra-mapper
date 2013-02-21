require 'spec_helper'

describe Cassandra::Mapper do
  subject do
    described_class.new :test_mapper, table, &definition
  end

  before :each do
    unless subject.keyspace.column_families.keys.include? table.to_s
      subject.migrate
    end
    subject.keyspace.clear_keyspace!
  end

  context 'two keys' do
    let(:table) { :two_keys }
    let(:definition) do
      proc do
        key :field1, :field2
        types \
          field1: :integer,
          field2: :integer,
          field3: :integer
      end
    end

    let(:field1) { 1 }
    let(:field2) { 2 }
    let(:field3) { 3 }
    let(:keys) {{ field1: field1, field2: field2 }}

    it '#insert only keys' do
      subject.insert keys
      subject.one(keys).should == keys
    end

    it '#inserts keys with data' do
      payload = keys.merge(field3: field3)
      subject.insert payload
      subject.one(keys).should == payload
    end
  end
end
