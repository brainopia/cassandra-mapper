class Cassandra::Mapper
  FULL_RING = 2**64
  HALF_RING = 2**63

  def token_for(data)
    hash = MurmurHash3::V128.str_hash raw_key_for(data)
    token = (hash[0] << 32) + hash[1]

    if token > HALF_RING
      FULL_RING - token
    else
      token
    end
  end

  def raw_key_for(data)
    request = Data::Request.new config, data
    request.packed_keys
  end
end
