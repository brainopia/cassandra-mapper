require 'cassandra/mapper'
require 'yaml'

Cassandra::Mapper.schema = { keyspaces: [ :mapper ]}
Cassandra::Mapper.env    = :test
Cassandra::Mapper.auto_migrate

RSpec.configure do |config|
  config.before do
    Cassandra.new('mapper_test').clear_keyspace!
  end
end
