require "json"
require "set"
require "digest/md5"
# require "net/http"
# require "stringio"
require "compress/zlib"

require "./avro/avro_error"

module Avro
  VERSION = File.read("#{__DIR__}/avro/VERSION.txt").freeze

  @@disable_enum_symbol_validation : Bool  = false
  @@disable_field_default_validation : Bool = false
  @@disable_schema_name_validation : Bool = false
  @@disable_schema_name_validation : Bool = false


  class AvroTypeError < Avro::AvroError
    def initialize(schm = nil, datum = nil, msg = nil)
      msg ||= "Not a #{schm}: #{datum}"
      super(msg)
    end
  end

  # class << self

  def self.disable_enum_symbol_validation
    @@disable_enum_symbol_validation ||= ENV.fetch("AVRO_DISABLE_ENUM_SYMBOL_VALIDATION", "") != ""
  end

  def self.disable_enum_symbol_validation=(value)
    @@disable_enum_symbol_validation = value
  end

  def self.disable_field_default_validation
    @@disable_field_default_validation ||= ENV.fetch("AVRO_DISABLE_FIELD_DEFAULT_VALIDATION", "") != ""
  end

  def self.disable_field_default_validation=(value)
    @@disable_field_default_validation = value
  end

  def self.disable_schema_name_validation
    @@disable_schema_name_validation ||= ENV.fetch("AVRO_DISABLE_SCHEMA_NAME_VALIDATION", "") != ""
  end

  def self.disable_schema_name_validation=(value)
    @@disable_schema_name_validation = value
  end
  # end
end

require "./avro/schema"
# require "./avro/io"
# require "./avro/data_file"
# require "./avro/protocol"
# require "./avro/ipc"
# require "./avro/schema_normalization"
# require "./avro/schema_validator"
# require "./avro/schema_compatibility"
