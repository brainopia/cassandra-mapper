require 'spec_helper'

describe Cassandra::Mapper::Convert do
  context 'uuid' do
    let(:time) { Time.now.round }

    it '#to' do
      subject.to(:uuid, time).should have(16).bytes
    end

    it '#from' do
      uuid = subject.to(:uuid, time)
      subject.from(:uuid, uuid).should == time
    end
  end

  context 'integer' do
    let(:number) { 17 }
    let(:cassandra_format) { "\x00\x00\x00\x11" }

    it '#to' do
      subject.to(:integer, number).should == cassandra_format
    end

    it '#from' do
      subject.from(:integer, cassandra_format) == number
    end
  end
end
