require "set"
require "json"
require "./schemas/*"
# require "./*"
require "./errors/*"

module Avro
  abstract class Schema
    def self.parse(json_string : String)
      real_parse(JSON.parse(json_string), {} of String => Avro::Schemas::AbstractSchema)
    end

    def self.real_parse(json_obj : JSON::Any, names : Hash(String, Avro::Schemas::AbstractSchema), default_namespace : String? = nil) : Avro::Schemas::AbstractSchema
      # begin
      # if json_obj.is_a?(Hash)
      if !json_obj.as_h?.nil?
        hash = json_obj.as_h
        type : String = String.from_json(hash["type"].to_json)
        logical_type : String? = String.from_json(hash.fetch("logicalType", "nil").to_json)
        raise SchemaParseError.new("No 'type' property: #{json_obj}") if type.nil?

        unless Avro::Schemas::AbstractSchema::VALID_TYPES.includes?(type)
          raise SchemaParseError.new("Unknown type: #{type}")
        end

        precision : Int32?
        scale : Int32?
        size : Int32?
        namespace : String?
        aliases : Array(String)?
        symbols : Array(String)?
        doc : String?

        # type_sym = type.to_sym
        if Avro::Schemas::AbstractSchema::PRIMITIVE_TYPES.includes?(type)
          case type
          when "bytes"
            precision = Int32.from_json(hash.fetch("precision", "nil").to_json)
            scale = Int32.from_json(hash.fetch("scale", "nil").to_json)
            return Avro::Schemas::BytesSchema.new(type, logical_type, precision, scale)
          else
            return Avro::Schemas::PrimitiveSchema.new(type, logical_type)
          end
        elsif Avro::Schemas::AbstractSchema::NAMED_TYPES.includes?(type)
          name = hash["name"].to_s
          if !Avro.disable_schema_name_validation && name !~ Avro::Schemas::AbstractSchema::NAME_REGEX
            raise SchemaParseError.new("Name #{name} is invalid for type #{type}!")
          end

          namespace = hash.includes?("namespace") ? String.from_json(hash["namespace"].to_json) : default_namespace
          aliases = hash.has_key?("aliases") ? Array(String).from_json(hash["aliases"].to_json) : nil
          fields : Array(JSON::Any)
          # debugger
          case type
          when "fixed"
            size = Int32.from_json(hash.fetch("size", "nil").to_json)
            precision = Int32.from_json(hash.fetch("precision", "nil").to_json)
            scale = Int32.from_json(hash.fetch("scale", "nil").to_json)
            return Avro::Schemas::FixedSchema.new(name, namespace, size, names, logical_type, aliases, precision, scale)
          when "enum"
            symbols = Array(String).from_json(hash.fetch("symbols", "null").to_json)
            doc = hash["doc"].to_s
            default = hash["default"]
            namespace = "" if namespace.nil?
            return Avro::Schemas::EnumSchema.new(name, namespace.as(String), symbols, names, doc, default, aliases)
          when "record", "error"
            fields = Array(JSON::Any).from_json(hash["fields"].to_json)
            doc = String.from_json(hash.fetch("doc", "null").to_json)
            # debugger
            namespace = "" if namespace.nil?
            return Avro::Schemas::RecordSchema.new(name, namespace.as(String), fields, names, type, doc, aliases)
          else
            raise SchemaParseError.new("Unknown named type: #{type}")
          end
        else
          case type
          when "array"
            items = String.from_json(hash["items"].to_json)
            return Avro::Schemas::ArraySchema.new(items, names, default_namespace)
          when "map"
            return Avro::Schemas::MapSchema.new(hash["values"], names, default_namespace)
          else
            raise SchemaParseError.new("Unknown Valid Type: #{type}")
          end
        end
        # rescue e : TypeCastError
        #   puts "!!!!!!!!!!!!!!!!!! base #{e}"
        #   raise e
        # end

        # begin
      elsif !json_obj.as_a?.nil?
        array = json_obj.as_a
        return Avro::Schemas::UnionSchema.new(array, names, default_namespace)
        # rescue e : TypeCastError
        # puts "!!!!!!!!!!!!!!!!!! #{e}"
        # end

      elsif Avro::Schemas::AbstractSchema::PRIMITIVE_TYPES.includes?(json_obj)
        # if Avro::Schemas::AbstractSchema::PRIMITIVE_TYPES.includes?(json_obj)
        type = String.from_json(json_obj.to_json)
        return Avro::Schemas::PrimitiveSchema.new(type)
      else
        raise UnknownSchemaError.new(json_obj.to_s, default_namespace.to_s)
      end
    end

    def self.validate(expected_schema, logical_datum, options = Avro::Schemas::AbstractSchema::DEFAULT_VALIDATE_OPTIONS)
      SchemaValidator.validate!(expected_schema, logical_datum, options)
      true
    rescue SchemaValidator::ValidationError
      false
    end
  end
end
