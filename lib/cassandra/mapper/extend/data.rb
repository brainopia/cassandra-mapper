class Cassandra::Mapper
  FULL_RING = 2**64
  HALF_RING = 2**63

  def token_for(data)
    token_for_raw raw_key_for(data)
  end

  def token_for_raw(data)
    # TODO: normalize for Long.minimal
    hash = MurmurHash3::V128.str_hash data
    token = (hash[1] << 32) + hash[0]

    if token > HALF_RING
      token - FULL_RING
    else
      token
    end
  end

  def raw_key_for(data)
    request = Data::Request.new config, data
    request.packed_keys
  end
end
