require 'spec_helper'

describe Cassandra::Mapper do
  subject do
    described_class.new :test_mapper, table, &definition
  end

  before do
    subject.keyspace.drop_column_family table.to_s rescue nil
    subject.migrate
  end

  let(:table) { :common }

  context 'one subkey' do
    let(:definition) do
      proc do
        key :field1
        subkey :field2
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

    it 'only keys' do
      subject.insert keys
      subject.one(keys).should == keys
    end

    it 'with data' do
      payload = keys.merge(field3: field3)
      subject.insert payload
      subject.one(keys).should == payload
    end
  end

  context 'conversions' do
    let :definition do
      scope = self
      proc do
        key *scope.key
        subkey *scope.subkey
        types field: scope.type
      end
    end

    def self.converts(type, original, expected=original, &block)
      context "converts #{type}" do
        let(:original) { original }
        let(:expected) { expected }
        let(:compare)  { block or -> it { it }}
        it_behaves_like :type
      end
    end

    shared_examples_for :convertable do
      context 'default text type' do
        let(:type) { nil }
        converts 'integer', 2, '2'
        converts 'string', 'string'
      end

      context 'integer type' do
        let(:type) { :integer }
        converts 'integer', 2
        converts 'big integer', 1_000_000
        converts 'string', '32', 32
      end

      context 'boolean type' do
        let(:type) { :boolean }
        converts 'true', true
        converts 'false', false
      end

      context 'time' do
        let(:type) { :time }
        converts 'time', Time.now.round
        converts('date', Date.today) {|time| time.to_date }
      end

      context 'decimal' do
        let(:type) { :decimal }
        converts 'integer', 20
        converts 'float', 30.442
        converts 'decimal', BigDecimal('42.42')
      end

      context 'float' do
        let(:type) { :float }
        converts 'integer', 20
        converts 'float', 30.30
      end
    end

    shared_examples_for :uuid_convertable do
      context 'uuid' do
        let(:type) { :uuid }

        converts 'uuid', SimpleUUID::UUID.new
        converts('time', Time.now) {|uuid| uuid.to_time }
      end
    end

    shared_examples_for :type do
      before { subject.insert data }

      it 'should be correctly restored' do
        restored = subject.one(query)[:field]
        compare.(restored).should == expected
      end
    end

    context 'key' do
      let(:key)     { :field }
      let(:subkey)  {}
      let(:query)   {{ field: original }}
      let(:data)    {{ field: original, data: :dummy }}

      it_behaves_like :convertable
    end

    context 'subkeys' do
      let(:key)     { :key }
      let(:subkey)  { :field }
      let(:query)   {{ key: 42 }}
      let(:data)    {{ key: 42, field: original }}

      it_behaves_like :convertable
      it_behaves_like :uuid_convertable
    end

    context 'data' do
      let(:key)     { :key }
      let(:subkey)  {}
      let(:query)   {{ key: 42 }}
      let(:data)    {{ key: 42, field: original }}

      it_behaves_like :convertable
      it_behaves_like :uuid_convertable
    end
  end
end
