require 'cassandra/mapper'

TEST_KEYSPACE = 'test_mapper'

# create testing keyspace if needed
cassandra = Cassandra.new('system')
unless cassandra.keyspaces.include? TEST_KEYSPACE
  cassandra.add_keyspace Cassandra::Keyspace.new \
    name: TEST_KEYSPACE,
    strategy_options: { 'replication_factor' => '1' },
    cf_defs: []
end
