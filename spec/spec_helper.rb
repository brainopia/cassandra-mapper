require 'cassandra/mapper'
require 'yaml'

Cassandra::Mapper.schema = { keyspaces: [ :mapper ]}
Cassandra::Mapper.env    = :test
Cassandra::Mapper.force_migrate
