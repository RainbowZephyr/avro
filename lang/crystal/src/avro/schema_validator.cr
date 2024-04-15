module Avro
  class SchemaValidator
    ROOT_IDENTIFIER = "."
    PATH_SEPARATOR = "."
    INT_RANGE = (Avro::Schemas::AbstractSchema::INT_MIN_VALUE..Avro::Schemas::AbstractSchema::INT_MAX_VALUE)
    LONG_RANGE = (Avro::Schemas::AbstractSchema::LONG_MIN_VALUE..Avro::Schemas::AbstractSchema::LONG_MAX_VALUE)
    COMPLEX_TYPES = [:array, :error, :map, :record, :request]
    BOOLEAN_VALUES = [true, false]
    DEFAULT_VALIDATION_OPTIONS = { :recursive => true, :encoded => false, :fail_on_extra_fields => false }
    RECURSIVE_SIMPLE_VALIDATION_OPTIONS = { :encoded => true }
    RUBY_CLASS_TO_AVRO_TYPE = {
      Nil => "null",
      String => "string",
      Float64 => "float",
      Hash => "record"
    }

    class Result
      getter :errors

      @errors : Array(String)

      def initialize(@errors = Array(String).new())
      end

      def <<(error)
        @errors << error
      end

      def add_error(path, message)
        self << "at #{path} #{message}"
      end

      def failure?
        !@errors.nil? && @errors.any?
      end

      def to_s
        failure? ? @errors.join("\n") : ""
      end

      # def errors
      #   # Use less memory for success results by lazily creating the errors array
      #   @errors ||= [] of String
      # end
    end

    class ValidationError < Exception
      getter result 
      
      @result : Result

      def initialize(result = Result.new)
        @result = result
        super(@result.to_s)
      end

      def to_s
        @result.to_s
      end
    end

    # TypeMismatchError = Class.new(ValidationError)

      def self.validate!(expected_schema, logical_datum, options = DEFAULT_VALIDATION_OPTIONS)
        result = Result.new
        if options.fetch(:recursive, true)
          validate_recursive(expected_schema, logical_datum, ROOT_IDENTIFIER, result, options)
        else
          validate_simple(expected_schema, logical_datum, ROOT_IDENTIFIER, result, options)
        end
        raise ValidationError.new(result) if result.failure?
        result
      end

      private def self.validate_recursive(expected_schema, logical_datum, path, result, options : Hash(Symbol, Bool))
        datum = resolve_datum(expected_schema, logical_datum, options[:encoded])

        validate_simple(expected_schema, datum, path, result, RECURSIVE_SIMPLE_VALIDATION_OPTIONS)

        case expected_schema.type
        when "array"
          validate_array(expected_schema, datum, path, result, options)
        when "map"
          validate_map(expected_schema, datum, path, result, options)
        when "union"
          validate_union(expected_schema, datum, path, result, options)
        when "record", "error", "request"
          raise ValidationError.new() unless datum.is_a?(Hash)
          expected_schema.as(Avro::Schemas::RecordSchema).fields.each do |field|
            deeper_path = deeper_path_for_hash(field.name, path)
            # TODO: fix this to not depend on symbol
            # nested_value = datum.key?(field.name) ? datum[field.name] : datum[field.name.to_sym]
            nested_value = datum.key?(field.name) ? datum[field.name] : datum[field.name]
            validate_recursive(field.type, nested_value, deeper_path, result, options)
          end
          if options[:fail_on_extra_fields]
            datum_fields = datum.keys.map(&.to_s)
            schema_fields = expected_schema.as(Avro::Schemas::RecordSchema).fields.map(&.name)
            (datum_fields - schema_fields).each do |extra_field|
              result.add_error(path, "extra field '#{extra_field}' - not in schema")
            end
          end
        end
      rescue e : ValidationError
        result.add_error(path, "expected type #{expected_schema.type}, got #{actual_value_message(datum)}")
      end

      private def self.validate_simple(expected_schema, logical_datum, path, result, options)
        datum = resolve_datum(expected_schema, logical_datum, options[:encoded])
        validate_type(expected_schema)

        case expected_schema.type
        when "null"
          raise ValidationError.new() unless datum.nil?
        when "boolean"
          raise ValidationError.new() unless BOOLEAN_VALUES.includes?(datum)
        when "string", "bytes"
          raise ValidationError.new() unless datum.is_a?(String)
        when "int"
          raise ValidationError.new() unless datum.is_a?(Int32) || datum.is_a?(Int64)
          result.add_error(path, "out of bound value #{datum}") unless INT_RANGE.covers?(datum)
        when "long"
          raise ValidationError.new() unless datum.is_a?(Int64)
          result.add_error(path, "out of bound value #{datum}") unless LONG_RANGE.covers?(datum)
        when "float", "double"
          raise ValidationError.new() unless datum.is_a?(Float64) || datum.is_a?(Int32) || datum.is_a?(Int64) || datum.is_a?(BigDecimal)
        when "fixed"
          if datum.is_a? String
            result.add_error(path, fixed_string_message(expected_schema.as(Avro::Schemas::FixedSchema).size, datum)) unless datum.bytesize == expected_schema.as(Avro::Schemas::FixedSchema).size
          else
            result.add_error(path, "expected fixed with size #{expected_schema.as(Avro::Schemas::FixedSchema).size}, got #{actual_value_message(datum)}")
          end
        when "enum"
          result.add_error(path, enum_message(expected_schema.as(Avro::Schemas::EnumSchema).symbols, datum)) unless expected_schema.as(Avro::Schemas::EnumSchema).symbols.includes?(datum)
        end
      rescue e : ValidationError
        result.add_error(path, "expected type #{expected_schema.type}, got #{actual_value_message(datum)}")
      end

      private def self.resolve_datum(expected_schema, logical_datum, encoded)
        if encoded
          logical_datum
        else
          expected_schema.type_adapter.encode(logical_datum) rescue nil
        end
      end

      private def self.validate_type(expected_schema)
        unless Avro::Schemas::AbstractSchema::VALID_TYPES.includes?(expected_schema.type)
          raise "Unexpected schema type #{expected_schema.type} #{expected_schema.inspect}"
        end
      end

      private def self.fixed_string_message(size, datum)
        "expected fixed with size #{size}, got \"#{datum}\" with size #{datum.bytesize}"
      end

      private def self.enum_message(symbols, datum)
        "expected enum with values #{symbols}, got #{actual_value_message(datum)}"
      end

      private def self.validate_array(expected_schema, datum, path, result, options)
        raise ValidationError.new() unless datum.is_a?(Array)
        datum.each_with_index do |d, i|
          validate_recursive(expected_schema.items, d, "#{path}[#{i}]", result, options)
        end
      end

      private def self.validate_map(expected_schema, datum, path, result, options)
        raise ValidationError.new() unless datum.is_a?(Hash)
        datum.keys.each do |k|
          result.add_error(path, "unexpected key type '#{ruby_to_avro_type(k.class)}' in map") unless k.is_a?(String)
        end
        datum.each do |k, v|
          deeper_path = deeper_path_for_hash(k, path)
          validate_recursive(expected_schema.values, v, deeper_path, result, options)
        end
      end

      private def self.validate_union(expected_schema, datum, path, result, options)
        if expected_schema.as(Avro::Schemas::UnionSchema).schemas.size == 1
          validate_recursive(expected_schema.as(Avro::Schemas::UnionSchema).schemas.first, datum, path, result, options)
          return
        end
        failures = Array(Hash(Symbol, Avro::SchemaValidator::Result | String)).new
        compatible_type = first_compatible_type(datum, expected_schema, path, failures, options)
        return unless compatible_type.nil?

        complex_type_failed = failures.find { |r| COMPLEX_TYPES.includes?(r[:type]) }
        if complex_type_failed
          complex_type_failed[:result].as(Avro::SchemaValidator::Result).errors.each { |error| result << error }
        else
          types = expected_schema.as(Avro::Schemas::UnionSchema).schemas.map { |s| "'#{s.type}'" }.join(", ")
          result.add_error(path, "expected union of [#{types}], got #{actual_value_message(datum)}")
        end
      end

      private def self.first_compatible_type(datum, expected_schema, path, failures, options = Hash(Symbol, Boolean))
        expected_schema.as(Avro::Schemas::UnionSchema).schemas.find do |schema|
          # Avoid expensive validation if we're just validating a nil
          next datum.nil? if schema.type == "null"

          result = Result.new
          validate_recursive(schema, datum, path, result, options)
          failures << { :type => schema.type, :result => result } if result.failure?
          !result.failure?
        end
      end

      private def self.deeper_path_for_hash(sub_key, path)
        deeper_path = "#{path}#{PATH_SEPARATOR}#{sub_key}"
        deeper_path = deeper_path.squeeze(PATH_SEPARATOR)
        # deeper_path.freeze
      end

      private def self.actual_value_message(value)
        avro_type = if value.is_a?(Int32) || value.is_a?(Int64)
                      ruby_integer_to_avro_type(value)
                    else
                      ruby_to_avro_type(value.class)
                    end
        if value.nil?
          avro_type
        else
          "#{avro_type} with value #{value.inspect}"
        end
      end

      private def self.ruby_to_avro_type(ruby_class)
        RUBY_CLASS_TO_AVRO_TYPE.fetch(ruby_class, ruby_class.to_s)
      end

      private def self.ruby_integer_to_avro_type(value)
        INT_RANGE.covers?(value) ? "int" : "long"
      end
    end
end
