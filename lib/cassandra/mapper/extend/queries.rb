class Cassandra::Mapper
  BATCH_SIZE = 500

  def insert(hash)
    data = Data::Insert.new config, hash
    keyspace.insert table, data.packed_keys, data.columns
    data.return!
  end

  def remove(hash)
    data = Data::Remove.new config, hash
    keyspace.remove table, data.packed_keys, data.columns
    data.return!
  end

  def get(query, slice={})
    request = Data::Request.new config, query
    columns, drop_last = columns_for request, slice

    response = Data::Response.new config, request.keys, columns
    records  = response.unpack

    records.pop if drop_last
    records
  end

  def one(keys, filter={})
    get(keys, filter).first
  end

  def each(&block)
    keyspace.each_key table do |packed_keys|
      keys = unpack_keys packed_keys
      get(keys).each &block
    end
  end

  def all
    to_enum.to_a
  end

  private

  def columns_for(request, filter)
    count = filter.delete(:count)
    batch = filter.delete(:batch_size) || count || BATCH_SIZE

    columns   = {}
    drop_last = false

   loop do
      result      = columns_get request, filter, batch
      result_size = result.size

      columns.merge! result

      break if result_size < batch

      if count
        if result_size >= count
          drop_last = true
          break
        else
          count -= result_size
        end
      end

      filter[:start] = { slice: :after, subkey: result.keys.last }
    end

    return columns, drop_last
  end

  def columns_get(request, filter, batch)
    query = request.query(filter.dup).merge! count: batch
    keyspace.get(table, request.packed_keys, query) || {}
  end

  def unpack_keys(packed_keys)
    keys = packed_keys.split Data::Request::KEY_SEPARATOR
    keys = Hash[config.key.zip(keys)]
    keys.each do |field, value|
      keys[field] = Convert.from config.type(field), value
    end
  end
end
