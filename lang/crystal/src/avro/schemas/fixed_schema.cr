require "./named_schema"

module Avro::Schemas
    class FixedSchema < NamedSchema
        getter :size, :precision, :scale
  
        def initialize(name : String, space : String?, size : Int64, names : Hash(String, Avro::Schemas::AbstractSchema)? = nil, logical_type : String? = nil, aliases : Array(String)? = nil, precision : Int32? = nil, scale : Int32? = nil)
          # Ensure valid cto args
          unless size.is_a?(Int)
            raise AvroError.new("Fixed Schema requires a valid integer for size property.")
          end
          super("fixed", name, space, names, nil, logical_type, aliases)
          @size = size
          @precision = precision
          @scale = scale
        end
  
        def to_avro(names = Set.new)
          avro = super
          return avro if avro.is_a?(String)
  
          avro["size"] = size
          avro["precision"] = precision if precision
          avro["scale"] = scale if scale
          avro
        end
  
        def match_schema?(schema)
          return true if super && size == schema.size
  
          if logical_type == DECIMAL_LOGICAL_TYPE && schema.logical_type == DECIMAL_LOGICAL_TYPE
            return precision == schema.precision && (scale || 0) == (schema.scale || 0)
          end
  
          false
        end
      end

end