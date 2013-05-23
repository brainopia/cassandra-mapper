class Cassandra::Mapper::Data
  class Response
    attr_reader :config, :key_values, :columns

    def initialize(config, key_values, columns)
      @config     = config
      @key_values = key_values
      @columns    = columns
    end

    def keys
      Hash[config.key.zip key_values]
    end

    def subkeys(values)
      Hash[config.subkey.zip values]
    end

    def unpack
      return [] if columns.empty?
      records = columns.group_by {|composite, _| composite[0..-2] }
      records.map do |subkey_values, fields|
        record = keys.merge subkeys(subkey_values)
        fields.each do |composite, value|
          field = composite[-1]
          record[field.to_sym] = value unless field.empty?
        end
        convert! record
      end
    end

    private

    def convert!(data)
      data.each do |field, value|
        data[field] = Cassandra::Mapper::Convert.from config.types[field], value
      end
    end
  end
end
