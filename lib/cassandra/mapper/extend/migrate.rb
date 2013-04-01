class Cassandra::Mapper
  def self.migrate
    cassandra = Cassandra.new('system')
    schema[:keyspaces].each do |name|
      options  = schema.fetch(env, {}).fetch(name, {})
      options  = Utility.stringify_keys options
      strategy = options.delete('strategy') || 'SimpleStrategy'
      options['replication_factor'] = options.fetch('replication_factor', 1).to_s

      cassandra.add_keyspace Cassandra::Keyspace.new \
        name:             "#{name}_#{env}",
        strategy_class:   strategy,
        strategy_options: options,
        cf_defs:          []
    end
  end

  def migrate
    subkey_types = config.subkey.map do |it|
      Convert.type config.types[it]
    end

    # field subkey
    subkey_types.push Convert::TEXT_TYPE

    keyspace.add_column_family Cassandra::ColumnFamily.new \
      keyspace:        keyspace.keyspace,
      name:            table,
      comparator_type: "CompositeType(#{subkey_types.join ','})"
  end
end
