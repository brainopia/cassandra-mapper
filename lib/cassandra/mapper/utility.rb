module Cassandra::Mapper::Utility
  extend self

  def symbolize_keys(hash)
    hash.keys.each do |key|
      hash[key.to_sym] = hash.delete key
    end
    hash
  end
end
