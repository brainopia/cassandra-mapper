module Cassandra::Mapper::Utility
  class Config
    extend DelegateKeys
    delegate_keys 'dsl.options', :key, :subkey, :types, :before_insert,
                                 :after_insert, :after_remove, :before_remove

    attr_reader :dsl

    def initialize(&block)
      @dsl = DSL.new &block
    end

    class DSL
      attr_reader :options

      def initialize(&block)
        @options = {
          types: {},
          before_insert: [],
          after_insert: [],
          after_remove: [],
          before_remove: []
        }
        instance_eval &block
      end

      def key(*fields)
        @options[:key] = fields
      end

      def subkey(*fields)
        @options[:subkey] = fields
      end

      def type(field, type)
        @options[:types][field] = type
      end

      def before_insert(&block)
        @options[:before_insert].push block
      end

      def before_remove(&block)
        @options[:before_remove].push block
      end

      def after_insert(&block)
        @options[:after_insert].push block
      end

      def after_remove(&block)
        @options[:after_remove].push block
      end
    end
  end
end
