module Cassandra::Mapper::DelegateKeys
  def delegate_keys(target, *keys)
    keys.each do |key|
      class_eval <<-RUBY
        def #{key}
          #{target}[:#{key}]
        end
      RUBY
    end
  end
end