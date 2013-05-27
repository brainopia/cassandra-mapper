class Cassandra::Mapper
  MigrateError = Class.new StandardError

  class << self
    def force_migrate
      auto_migrate_keyspaces
      instances.each(&:force_migrate)
      @force_migrate_cf = true
    end

    def auto_migrate
      auto_migrate_keyspaces
      instances.each(&:auto_migrate)
      @auto_migrate_cf = true
    end

    def new(*)
      super.tap do |it|
        it.auto_migrate  if @auto_migrate_cf
        it.force_migrate if @force_migrate_cf
      end
    end

    def auto_migrate_keyspaces
      system = Cassandra.new('system')
      keyspaces = system.send(:client).describe_keyspaces

      keyspaces_schema.each do |keyspace|
        found = keyspaces.find {|it| it.name == keyspace.name }
        if found
          unless schema_match? found, keyspace
            raise MigrateError, "#{keyspace.name} exists and not matches schema"
          end
        else
          system.add_keyspace keyspace
        end
      end
    end

    def keyspaces_schema
      schema[:keyspaces].map do |name|
        options  = schema.fetch(env, {}).fetch(name, {})
        options  = Utility::Hash.stringify_keys options
        strategy = options.delete('strategy') || 'SimpleStrategy'
        options['replication_factor'] = options.fetch('replication_factor', 1).to_s

        Cassandra::Keyspace.new \
          name:             "#{name}_#{env}",
          strategy_class:   strategy,
          strategy_options: options,
          cf_defs:          []
      end
    end

    def schema_match?(actual, blueprint)
      actual.strategy_class.include?(blueprint.strategy_class) and
      actual.strategy_options == blueprint.strategy_options
    end
  end

  def force_migrate
    migrate do
      keyspace.drop_column_family cf_schema.name
      keyspace.add_column_family cf_schema
    end
  end

  def auto_migrate
    migrate do
      raise MigrateError, <<-ERROR
        #{actual.name} exists and not matches comparator.
        actual: #{comparator}
        expected: #{blueprint.comparator_type}
      ERROR
    end
  end

  def migrate
    blueprint = cf_schema
    actual = keyspace.column_families[blueprint.name]
    if actual
      comparator = actual.comparator_type.gsub('org.apache.cassandra.db.marshal.', '')
      yield unless comparator == blueprint.comparator_type
    else
      keyspace.add_column_family blueprint
    end
  end

  def cf_schema
    subkey_types = config.subkey.map do |it|
      Convert.type config.types[it]
    end

    # field subkey
    subkey_types.push Convert::TEXT_TYPE

    Cassandra::ColumnFamily.new \
      keyspace:        keyspace.keyspace,
      name:            table,
      comparator_type: "CompositeType(#{subkey_types.join ','})"
  end
end
