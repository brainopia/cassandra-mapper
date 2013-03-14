require 'spec_helper'

describe Cassandra::Mapper::StoreInstances do
  let(:subject) { Class.new.tap {|it| it.extend described_class }}

  it 'should initialize instances with array' do
    subject.instances.should == []
  end

  it 'should track new instances' do
    object1 = subject.new
    object2 = subject.new

    subject.instances.should == [object1, object2]
  end
end
