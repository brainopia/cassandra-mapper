class Cassandra::Mapper
  def self.schema
    @@schema
  end

  def self.schema=(schema)
    @@schema = Utility.symbolize_keys schema
  end

  def self.env
    @@env
  end

  def self.env=(env)
    @@env = env.to_sym
  end

  def env
    @@env
  end
end
