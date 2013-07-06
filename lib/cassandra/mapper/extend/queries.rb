class Cassandra::Mapper
  BATCH_SIZE   = 500
  MAX_ONE_SIZE = 15

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
    buffer  = [] unless block_given?

    columns_for request, slice do |batch|
      response = Data::Response.new config, request.keys, batch
      records  = response.unpack
      buffer ? buffer.concat(records) : yield(records)
    end

    buffer
  end

  def one(keys, filter={})
    get(keys, { count: MAX_ONE_SIZE }.merge(filter)).first
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
    last_record = {}

    loop do
      result      = columns_get request, filter, batch
      result_size = result.size

      result      = last_record.merge! result
      records     = result.group_by {|composite, _| composite[0..-2] }

      if result_size >= batch
        last_record = Hash[records.delete records.keys.last]
      end

      yield records

      if result_size < batch
        break
      end

      if count
        if result_size >= count
          break
        else
          count -= result_size
        end
      end

      filter[:start] = { slice: :after, subkey: last_record.keys.last }
    end
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
