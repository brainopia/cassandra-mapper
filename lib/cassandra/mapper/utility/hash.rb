module Cassandra::Mapper::Utility
  module Hash
    extend self

    def symbolize_keys(hash)
      map_hash_key hash, &:to_sym
    end

    def stringify_keys(hash)
      map_hash_key hash, &:to_s
    end

    private

    def map_hash_key(hash)
      hash.keys.each do |key|
        hash[yield key] = hash.delete key
      end
      hash
    end
  end
end
