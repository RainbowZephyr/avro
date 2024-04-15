require "./primitive_schema"

module Avro::Schemas
  class BytesSchema < PrimitiveSchema
    ERROR_INVALID_SCALE       = "Scale must be greater than or equal to 0"
    ERROR_INVALID_PRECISION   = "Precision must be positive"
    ERROR_PRECISION_TOO_SMALL = "Precision must be greater than scale"

    getter :precision, :scale
    @precision : Int32?
    @scale : Int32?

    def initialize(type : String, logical_type : String? = nil, precision : Int32? = nil, scale : Int32? = nil)
      super(type, logical_type)

      @precision = precision if precision
      @scale = scale if scale

      validate_decimal! if logical_type == DECIMAL_LOGICAL_TYPE
    end

    def to_avro(names = nil?)
      avro = super
      return avro if avro.is_a?(String)

      avro["precision"] = precision if precision
      avro["scale"] = scale if scale
      avro
    end

    def match_schema?(schema)
      return true if super

      if logical_type == DECIMAL_LOGICAL_TYPE && schema.logical_type == DECIMAL_LOGICAL_TYPE
        return precision == schema.precision && (scale || 0) == (schema.scale || 0)
      end

      false
    end

    private def validate_decimal!
      if !@precision.nil?
        raise Avro::SchemaParseError.new(ERROR_INVALID_PRECISION) unless @precision.as(Int32).positive?
      end

      if !@scale.nil?
        raise Avro::SchemaParseError.new(ERROR_INVALID_SCALE) if @scale.as(Int32).negative?
      end

      if !@precision.nil? && !@scale.nil?
        raise Avro::SchemaParseError.new(ERROR_PRECISION_TOO_SMALL) if @precision.as(Int32) < @scale.as(Int32)
      end
    end
  end
end
