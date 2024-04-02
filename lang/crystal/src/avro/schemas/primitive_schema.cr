require "./abstract_schema"

module Avro::Schemas
  # Valid primitive types are in PRIMITIVE_TYPES.
  class PrimitiveSchema < AbstractSchema
    def initialize(type : String, logical_type : String? = nil)
      if PRIMITIVE_TYPES.includes?(type)
        super(type, logical_type)
      else
        raise AvroError.new("#{type} is not a valid primitive type.")
      end
    end

    def to_avro(names = nil?)
      hsh = super
      hsh.size == 1 ? type : hsh
    end

    def match_schema?(schema)
      return type_sym == schema.type_sym
      # TODO: eventually this could handle schema promotion for primitive schemas too
    end
  end
end
