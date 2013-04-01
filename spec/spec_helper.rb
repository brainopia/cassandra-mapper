require 'cassandra/mapper'
require 'yaml'

Cassandra::Mapper.schema = { test: { mapper: {}}}
Cassandra::Mapper.env    = :test

begin
  Cassandra::Mapper.migrate
rescue CassandraThrift::InvalidRequestException
  puts 'Using existing keyspace'
end
