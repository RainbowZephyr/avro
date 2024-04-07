# -*- coding: utf-8 -*-
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
# https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

require "time"
require "big"

module Avro
  module LogicalTypes
    class LogicalTypeWithSchema
      property :schema 
      @schema : (Avro::Schemas::BytesSchema | Avro::Schemas::FixedSchema)

      def initialize(@schema)
        raise ArgumentError.new("schema is required") if @schema.nil?
      end

      def encode(datum)
        raise NotImplementedError.new("Encoder not implemented for `#{datum}`")
      end

      def decode(datum)
        raise NotImplementedError.new("Decoder not implemented for `#{datum}`")
      end
    end

    class BytesDecimal < LogicalTypeWithSchema
      ERROR_INSUFFICIENT_PRECISION = "Precision is too small"
      ERROR_ROUNDING_NECESSARY = "Rounding necessary"
      ERROR_VALUE_MUST_BE_NUMERIC = "value must be numeric"

      PACK_UNSIGNED_CHARS = "C*"
      TEN = BigInt.new(10)

      property :precision 
      property :scale 
      property :factor 
      @precision : Int32 = 0
      @scale : Int32 = 0
      @factor : BigInt = BigInt.new(0)

      def initialize(@schema)
        super

        if @schema.is_a?(Avro::Schemas::BytesSchema)
          sch = @schema.as(Avro::Schemas::BytesSchema) 
          if(!sch.scale.nil? && !sch.precision.nil?)
            @scale = sch.scale.as(Int32)
            @precision = sch.precision.as(Int32)
            @factor = TEN ** @scale
          end
        elsif @schema.is_a?(Avro::Schemas::FixedSchema)
          sch = @schema.as(Avro::Schemas::FixedSchema) 
          if(!sch.scale.nil? && !sch.precision.nil?)
            @scale = sch.scale.as(Int32)
            @precision = sch.precision.as(Int32)
            @factor = TEN ** @scale
          end
        else 
          raise ArgumentError.new("Cannot have scale in any schema beyond Bytes and Fixed")
        end

      end

      def encode(value)
        raise ArgumentError.new(ERROR_VALUE_MUST_BE_NUMERIC) unless value.is_a?(Number)

        # to_byte_array(unscaled_value(BigDecimal.new(value))).pack(PACK_UNSIGNED_CHARS)
        bytes = to_byte_array(unscaled_value(BigDecimal.new(value)))
        String.new(bytes.map(&.to_u8).to_unsafe)
      end

      def decode(stream)
        from_byte_array(stream).to_d / @factor
      end

      private def from_byte_array(stream)
        bytes = stream.bytes
        positive = bytes.first[7] == 0
        total = 0

        bytes.each_with_index do |value, ix|
          total += (positive ? value : (value ^ 0xff)) << (bytes.length - ix - 1) * 8
        end

        positive ? total : -(total + 1)
      end

      # private def to_byte_array(number : Int32)
      #   result = Array(Int32).new()

      #   loop do
      #     result.unshift(number & 0xff)
      #     number >>= 8

      #     break if (number == 0 || number == -1) && (result[0...7] == number & 0x80_u8)
      #   end

      #   result
      # end

      private def to_byte_array(number : Int)
        result = [] of Int32
      
        loop do
          result.unshift(number & 0xff_u8)
          number >>= 8
      
          break if (number == 0 || number == -1) && (result.first? && (result.first & 0x80_u8) == (number & 0x80_u8))
        end
      
        result
      end

      private def unscaled_value(decimal)
        # TODO: Crystal does not support those features
        # details = decimal.split
        # length = details[1].size

        # fractional_part = length - details[3]
        # raise RangeError.new(ERROR_ROUNDING_NECESSARY) if fractional_part > scale

        # if length > precision || (length - fractional_part) > (precision - scale)
        #   raise RangeError.new(ERROR_INSUFFICIENT_PRECISION)
        # end

        (decimal * @factor).to_i
      end
    end

    class IntDate < LogicalTypeWithSchema
      EPOCH_START = Time.utc(1970, 1, 1)

      def encode(date)
        return date.to_i if date.is_a?(Number) 
        string_date = date.as(String)
         (Time.parse_utc(string_date, "%F") - EPOCH_START).to_i
      end

      def decode(int)
        EPOCH_START + int
      end
    end

    class TimestampMillis < LogicalTypeWithSchema
      SUBUNITS_PER_SECOND = 1000

      def encode(value)
        return value.to_i if value.is_a?(Number)
          # time = value.to_utc
          # time.to_i * SUBUNITS_PER_SECOND + time.usec / SUBUNITS_PER_SECOND
        string_value = value.as(String)
          Time.parse_iso8601(string_value).to_unix_ms
      end

      def decode(int)
        s, ms = int.divmod(SUBUNITS_PER_SECOND)
        Time.at(s, ms, :millisecond).utc
      end
    end

    class TimestampMicros < LogicalTypeWithSchema
      SUBUNITS_PER_SECOND = 1_000_000

      def encode(value)
        value.is_a?(Number) ? value.to_i : begin
        string_value = value.as(String)
          time = Time.parse_iso8601(string_value)
          time.to_unix * SUBUNITS_PER_SECOND + (time.nanosecond / 1000.0).round.to_i
        end
      end

      def decode(int)
        s, us = int.divmod(SUBUNITS_PER_SECOND)
        Time.at(s, us, :microsecond).utc
      end
    end

    class TimestampNanos < LogicalTypeWithSchema
      SUBUNITS_PER_SECOND = 1_000_000_000

      def encode(value)
        return value.to_i if value.is_a?(Number)
        string_value = value.as(String)
        Time.parse_iso8601(string_value).to_unix_ns
      end

      def decode(int)
        s, ns = int.divmod(SUBUNITS_PER_SECOND)
        Time.at(s, ns, :nanosecond).utc
      end
    end

    class Identity < LogicalTypeWithSchema
      def encode(datum)
        datum
      end

      def decode(datum)
        datum
      end
    end

    TYPES = {
      "bytes" => {
        "decimal" => BytesDecimal,
      },
      "int" => {
        "date" => IntDate,
      },
      "long" => {
        "timestamp-millis" => TimestampMillis,
        "timestamp-micros" => TimestampMicros,
        "timestamp-nanos" => TimestampNanos,
      },
    }

    def self.type_adapter(type : String, logical_type : String?, schema : Avro::Schemas::AbstractSchema? = nil) : (Avro::LogicalTypes::LogicalTypeWithSchema)?
      return unless logical_type

      adapter = TYPES.fetch(type, Hash(String, Avro::LogicalTypes::LogicalTypeWithSchema.class).new).fetch(logical_type, Identity)
      # return adapter.is_a?(Class) ? adapter.new(schema) : adapter
      return adapter.new(schema)
    end
  end
end
