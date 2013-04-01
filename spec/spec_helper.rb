require 'cassandra/mapper'
require 'yaml'

Cassandra::Mapper.schema = { keyspaces: [ :mapper ]}
Cassandra::Mapper.env    = :test

begin
  Cassandra::Mapper.migrate
rescue CassandraThrift::InvalidRequestException
  puts 'Using existing keyspace'
end
