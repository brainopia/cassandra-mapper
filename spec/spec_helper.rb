require 'cassandra/mapper'
require 'yaml'

begin
  schema = File.expand_path File.join __FILE__, '../schema.yml'
  Cassandra::Mapper.migrate 'test', YAML.load_file(schema)
rescue CassandraThrift::InvalidRequestException
  puts 'Using existing keyspace'
end
