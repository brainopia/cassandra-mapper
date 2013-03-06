class Cassandra::Mapper
  def self.migrate(env, schema)
    cassandra = Cassandra.new('system')
    schema[env].each do |name, options|
      options ||= {}
      strategy = options.delete('strategy') || 'SimpleStrategy'
      options['replication_factor'] = options.fetch('replication_factor', 1).to_s

      cassandra.add_keyspace Cassandra::Keyspace.new \
        name:             name,
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
