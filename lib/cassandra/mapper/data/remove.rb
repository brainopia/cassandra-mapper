class Cassandra::Mapper::Data
  class Remove < Insert
    def convert!(data)
      config.before_remove.each {|it| it.call data }
      super
    end

    def columns
      super.keys unless subkeys.empty? and data.empty?
    end

    def return!
      converted.tap do |data|
        config.after_remove.each {|it| it.call data }
      end
    end
  end
end
