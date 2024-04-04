require "./abstract_schema"
require "../schema_validator"
module Avro::Schemas
  class Field < AbstractSchema
    getter :field_type, :name, :default, :order, :doc, :aliases
    @field_type : Avro::Schemas::AbstractSchema
    @name : String

    def initialize(type : String, name : String, default : String = "no_default", order : String? = nil, names : Hash(String, Avro::Schemas::AbstractSchema) = nil, namespace : String? = nil, doc : String? = nil, aliases : Array(String)? = nil)
      super(type, nil)
      @field_type = subparse(type, names, namespace)
      @name = name
      @default = default
      @order = order
      @doc = doc
      @aliases = aliases
      @type_adapter = nil
      validate_aliases! if aliases
      validate_default! if default? && !Avro.disable_field_default_validation
    end

    def default?
      @default != :no_default
    end

    def to_avro(names = Set.new)
      {"name" => name, "type" => type.to_avro(names)}.tap do |avro|
        avro["default"] = default if default?
        avro["order"] = order if order
        avro["doc"] = doc if doc
      end
    end

    def alias_names
      @alias_names ||= aliases.to_a
    end

    private def validate_default!
      type_for_default : Avro::Schemas::AbstractSchema
      if type == "union"
        type_for_default = @field_type.as(Avro::Schemas::UnionSchema).schemas.first
      else
        type_for_default = @field_type
      end

      case type_for_default.logical_type
      when DECIMAL_LOGICAL_TYPE
        # https://avro.apache.org/docs/1.11.1/specification/#schema-record
        # Default values for bytes and fixed fields are JSON strings, where Unicode code points 0-255 are mapped to unsigned 8-bit byte values 0-255
        options = Avro::SchemaValidator::DEFAULT_VALIDATION_OPTIONS.dup
        options[:encoded] = true
        Avro::SchemaValidator.validate!(type_for_default, default, options)
      else
        Avro::SchemaValidator.validate!(type_for_default, default)
      end
    rescue e : Avro::SchemaValidator::ValidationError
      raise Avro::SchemaParseError.new("Error validating default for #{name}: #{e.message}")
    end
  end
end
