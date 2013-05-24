require 'spec_helper'

describe Cassandra::Mapper do
  subject do
    described_class.new :mapper, table, &definition
  end

  before do
    subject.keyspace.drop_column_family table.to_s rescue nil
    subject.migrate
  end

  let(:table) { :common }

  context 'one subkey' do
    let :definition do
      proc do
        key :field1
        subkey :field2
        type :field1, :integer
        type :field2, :integer
        type :field3, :integer
      end
    end

    context '.insert/.get' do
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
        subject.insert(payload).should == payload
        subject.one(keys).should == payload
      end

      it 'nil data field' do
        payload = keys.merge(field3: nil, field4: nil)
        converted_payload = subject.insert(payload)
        converted_payload.should == keys
        subject.one(keys).should == keys
      end
    end

    context 'various commands' do
      let(:first) {{ field1: 1, field2: 1, data: 'payload1' }}
      let(:second) {{ field1: 1, field2: 2, data: 'payload2' }}
      let(:data) {[ first, second ]}

      before { data.each &subject.method(:insert) }

      it '.each' do
        subject.to_enum.to_a.should == data
      end

      it '.delete' do
        subject.get(field1: 1).should have(2).items
        subject.remove(first)
        subject.get(field1: 1).should == [second]
        subject.remove(second)
        subject.get(field1: 1).should be_empty
      end
    end
  end

  context 'callbacks' do
    let :definition do
      proc do
        key :field1
        subkey :field2
        type :field2, :integer
      end
    end

    it '.before_insert' do
      subject.config.dsl.before_insert do |data|
        data[:field1] = data[:field2]
      end
      subject.insert field2: 2
      subject.one(field1: '2').should == { field1: '2', field2: 2 }
    end

    it '.after_insert' do
      subject.config.dsl.after_insert do |data|
        data.should == { field1: '1', field2: 2 }
      end
      subject.insert field1: 1, field2: '2'
    end

    it '.after_remove' do
      subject.config.dsl.after_remove do |data|
        data.should == { field1: '1', field2: 2 }
      end
      subject.insert field1: 1, field2: '2'
      subject.remove field1: 1, field2: '2'
    end
  end

  context 'conversions' do
    let :definition do
      scope = self
      proc do
        key *scope.key
        subkey *scope.subkey
        type :field, scope.type
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

        uuid = SimpleUUID::UUID.new
        converts 'uuid', uuid, uuid.to_time
        converts 'time', Time.now.round
      end
    end

    shared_examples_for :empty_string_convertable do
      context 'empty string for text type' do
        let(:type) { nil }
        converts '', ''
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
      it_behaves_like :empty_string_convertable
    end

    context 'data' do
      let(:key)     { :key }
      let(:subkey)  {}
      let(:query)   {{ key: 42 }}
      let(:data)    {{ key: 42, field: original }}

      it_behaves_like :convertable
      it_behaves_like :uuid_convertable
      it_behaves_like :empty_string_convertable
    end

    context 'complex key' do
      let(:key)     {[ :key, :field ]}
      let(:subkey)  {}
      let(:query)   {{ key: 42, field: original }}
      let(:data)    {{ key: 42, field: original, data: :dummy }}

      it_behaves_like :convertable
      it_behaves_like :empty_string_convertable
    end
  end
end
