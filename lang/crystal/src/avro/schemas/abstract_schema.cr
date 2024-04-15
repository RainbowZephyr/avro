require "../logical_types"

module Avro::Schemas
  abstract class AbstractSchema
    PRIMITIVE_TYPES = Set.new(%w[null boolean string bytes int long float double])
    NAMED_TYPES     = Set.new(%w[fixed enum record error])
    VALID_TYPES     = PRIMITIVE_TYPES + NAMED_TYPES + Set.new(%w[array map union request])

    # PRIMITIVE_TYPES_SYM = Set.new(PRIMITIVE_TYPES.map(&.to_sym))
    # NAMED_TYPES_SYM = Set.new(NAMED_TYPES.map(&.to_sym))
    # VALID_TYPES_SYM = Set.new(VALID_TYPES.map(&.to_sym))

    NAME_REGEX = /^([A-Za-z_][A-Za-z0-9_]*)(\.([A-Za-z_][A-Za-z0-9_]*))*$/

    INT_MIN_VALUE  = Int32::MIN
    INT_MAX_VALUE  = Int32::MAX
    LONG_MIN_VALUE = Int64::MIN
    LONG_MAX_VALUE = Int64::MAX

    DEFAULT_VALIDATE_OPTIONS = {recursive: true, encoded: false}
    DECIMAL_LOGICAL_TYPE     = "decimal"

    getter :type, :logical_type

    # @logical_type : Hash(String, String)
      @type_adapter : Avro::LogicalTypes::LogicalTypeWithSchema?

    def initialize(@type : String, @logical_type : String? = nil)
      # @type_sym = type.is_a?(Symbol) ? type : type.to_sym
      @logical_type = logical_type
      @type_adapter = nil
    end

    # def type
    #   @type_sym.to_s
    # end

    def type_adapter : Avro::LogicalTypes::LogicalTypeWithSchema?
      @type_adapter ||= Avro::LogicalTypes.type_adapter(type, logical_type, self) || Avro::LogicalTypes::Identity.new(self)
    end

    def md5_fingerprint
      parsing_form = SchemaNormalization.to_parsing_form(self)
      Digest::MD5.hexdigest(parsing_form).to_i(16)
    end

    def sha256_fingerprint
      parsing_form = SchemaNormalization.to_parsing_form(self)
      Digest::SHA256.hexdigest(parsing_form).to_i(16)
    end

    CRC_EMPTY = 0xc15d213aa4d7a795u64

    @@fp_table = nil?

    def init_fp_table
      @@fp_table = Array.new(256)
      256.times do |i|
        fp = i
        8.times do
          fp = (fp >> 1) ^ (CRC_EMPTY & -(fp & 1))
        end
        @@fp_table[i] = fp
      end
    end

    def crc_64_avro_fingerprint
      parsing_form = Avro::SchemaNormalization.to_parsing_form(self)
      data_bytes = parsing_form.unpack("C*")

      init_fp_table unless @@fp_table

      fp = CRC_EMPTY
      data_bytes.each do |b|
        fp = (fp >> 8) ^ @@fp_table[(fp ^ b) & 0xff]
      end
      fp
    end

    SINGLE_OBJECT_MAGIC_NUMBER = [0xC3, 0x01].freeze

    def single_object_encoding_header
      [SINGLE_OBJECT_MAGIC_NUMBER, single_object_schema_fingerprint].flatten
    end

    def single_object_schema_fingerprint
      working = crc_64_avro_fingerprint
      bytes = Array.new(8)
      8.times do |i|
        bytes[i] = (working & 0xff)
        working = working >> 8
      end
      bytes
    end

    def read?(writers_schema)
      SchemaCompatibility.can_read?(writers_schema, self)
    end

    def be_read?(other_schema)
      other_schema.read?(self)
    end

    def mutual_read?(other_schema)
      SchemaCompatibility.mutual_read?(other_schema, self)
    end

    def ==(other)
      other.is_a?(Schema) && type_sym == other.type_sym
    end

    def hash(_seen = nil?)
      type_sym.hash
    end
    
    def subparse(json_obj : (JSON::Any | String), names : Hash(String, Avro::Schemas::AbstractSchema)? = nil, namespace : String? = nil) : Avro::Schemas::AbstractSchema
      if json_obj.is_a?(String) && names.nil?
        fullname = Name.make_fullname(json_obj, namespace)
        return names[fullname] if names.includes?(fullname)
      end

      json = JSON.parse(json_obj.to_json)
      begin
        Schema.real_parse(json, names, namespace)
      rescue e
        raise e if e.is_a?(SchemaParseError)
        raise SchemaParseError.new("Sub-schema for #{self.class.name} not a valid Avro schema. Bad schema: #{json_obj}")
      end
    end

    # def subparse(json_obj : String, names : Hash(String, Avro::Schemas::AbstractSchema)? = nil, namespace : String? = nil) : Avro::Schemas::AbstractSchema?
    #   if names
    #     fullname = Name.make_fullname(json_obj, namespace)
    #     return names[fullname] if names.includes?(fullname)
    #   end
    # end

    # def subparse(json_obj : JSON::Any, names : Hash(String, Avro::Schemas::AbstractSchema)? = nil, namespace : String? = nil) : Avro::Schemas::AbstractSchema?
    #   begin
    #     Schema.real_parse(json_obj, names, namespace)
    #   rescue e
    #     raise e if e.is_a?(SchemaParseError)
    #     raise SchemaParseError.new("Sub-schema for #{self.class.name} not a valid Avro schema. Bad schema: #{json_obj}")
    #   end
    # end

    def to_avro(_names = nil?)
      props = {"type" => type}
      props["logicalType"] = logical_type if logical_type
      props
    end

    def to_s
      MultiJson.dump(to_avro)
    end

    protected def validate_aliases!
      unless aliases.nil? ||
             (aliases.is_a?(Array) && aliases.as(Array(String)).all? { |a| a.is_a?(String) })
        raise Avro::SchemaParseError.new("Invalid aliases value #{aliases.inspect} for #{type} #{name}. Must be an array of strings.")
      end
    end
  end
end
