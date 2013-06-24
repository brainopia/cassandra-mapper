class Cassandra::Mapper
  def self.schema
    @@schema
  end

  def self.schema=(schema)
    @@schema = Utility::Hash.symbolize_keys schema
  end

  def self.env
    @@env
  end

  def self.env=(env)
    @@env = env.to_sym
  end

  def self.keyspaces
    schema[:keyspaces].map do |name|
      Cassandra.new "#{name}_#{env}", server
    end
  end

  def self.clear!
    keyspaces.each(&:clear_keyspace!)
  end

  def env
    @@env
  end
end
