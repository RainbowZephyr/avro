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
      property schema : Avro::Schema

      def initialize(@schema)
        raise ArgumentError.new("schema is required") if @schema.nil?
      end

      def encode(datum)
        raise NotImplementedError.new
      end

      def decode(datum)
        raise NotImplementedError.new
      end
    end

    class BytesDecimal < LogicalTypeWithSchema
      ERROR_INSUFFICIENT_PRECISION = "Precision is too small"
      ERROR_ROUNDING_NECESSARY = "Rounding necessary"
      ERROR_VALUE_MUST_BE_NUMERIC = "value must be numeric"

      PACK_UNSIGNED_CHARS = "C*"
      TEN = BigInt.new(10)

      property precision : Int32
      property scale : Int32
      property factor : BigInt

      def initialize(@schema)
        super

        @scale = @schema.scale.to_i
        @precision = @schema.precision.to_i
        @factor = TEN ** @scale
      end

      def encode(value)
        raise ArgumentError.new(ERROR_VALUE_MUST_BE_NUMERIC) unless value.is_a?(Numeric)

        to_byte_array(unscaled_value(BigDecimal.new(value)))
          .pack(PACK_UNSIGNED_CHARS).freeze
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

      private def to_byte_array(number)
        result = [] of UInt8

        loop do
          result.unshift(number & 0xff)
          number >>= 8

          break if (number == 0 || number == -1) && (result.first[7] == number[7])
        end

        result
      end

      private def unscaled_value(decimal)
        details = decimal.split
        length = details[1].size

        fractional_part = length - details[3]
        raise RangeError.new(ERROR_ROUNDING_NECESSARY) if fractional_part > scale

        if length > precision || (length - fractional_part) > (precision - scale)
          raise RangeError.new(ERROR_INSUFFICIENT_PRECISION)
        end

        (decimal * @factor).to_i
      end
    end

    module IntDate
      EPOCH_START = Time.utc(1970, 1, 1).to_unix

      def self.encode(date)
        date.is_a?(Numeric) ? date.to_i : (date - EPOCH_START).to_i
      end

      def self.decode(int)
        EPOCH_START + int
      end
    end

    module TimestampMillis
      SUBUNITS_PER_SECOND = 1000

      def self.encode(value)
        value.is_a?(Numeric) ? value.to_i : begin
          time = value.to_time
          time.to_i * SUBUNITS_PER_SECOND + time.usec / SUBUNITS_PER_SECOND
        end
      end

      def self.decode(int)
        s, ms = int.divmod(SUBUNITS_PER_SECOND)
        Time.at(s, ms, :millisecond).utc
      end
    end

    module TimestampMicros
      SUBUNITS_PER_SECOND = 1_000_000

      def self.encode(value)
        value.is_a?(Numeric) ? value.to_i : begin
          time = value.to_time
          time.to_i * SUBUNITS_PER_SECOND + time.usec
        end
      end

      def self.decode(int)
        s, us = int.divmod(SUBUNITS_PER_SECOND)
        Time.at(s, us, :microsecond).utc
      end
    end

    module TimestampNanos
      SUBUNITS_PER_SECOND = 1_000_000_000

      def self.encode(value)
        value.is_a?(Numeric) ? value.to_i : begin
          time = value.to_time
          time.to_i * SUBUNITS_PER_SECOND + time.nsec
        end
      end

      def self.decode(int)
        s, ns = int.divmod(SUBUNITS_PER_SECOND)
        Time.at(s, ns, :nanosecond).utc
      end
    end

    module Identity
      def self.encode(datum)
        datum
      end

      def self.decode(datum)
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

    def self.type_adapter(type, logical_type, schema = nil) : Avro::LogicalTypes
      return unless logical_type

      adapter = TYPES.fetch(type, Hash(String, LogicalTypes).new).fetch(logical_type, Identity)
      adapter.is_a?(Class) ? adapter.new(schema) : adapter
    end
  end
end
