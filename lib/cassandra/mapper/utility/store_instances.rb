module Cassandra::Mapper::StoreInstances
  def self.extended(klass)
    klass.instance_variable_set :@instances, []
  end

  attr_reader :instances

  def new(*)
    super.tap {|it| @instances << it }
  end
end
