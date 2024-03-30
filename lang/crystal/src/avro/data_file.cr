require "openssl"
require "json"
require "compress/zlib"
require "./io"
# require "snappy"
# require "zstd"

module Avro
  module DataFile
    VERSION = 1
    MAGIC = "Obj" + VERSION.to_s
    MAGIC_SIZE = MAGIC.size
    SYNC_SIZE = 16
    SYNC_INTERVAL = 4000 * SYNC_SIZE
    META_SCHEMA = JSON.parse("{'type': 'map', 'values': 'bytes'}")
    VALID_ENCODINGS = ["binary"].freeze

    class DataFileError < Exception; end

    def self.open(file_path, mode='r', schema=nil, codec=nil)
      schema = Avro::Schema.parse(schema) if schema
      case mode
      when 'w'
        unless schema
          raise DataFileError.new("Writing an Avro file requires a schema.")
        end
        io = open_writer(File.open(file_path, "wb"), schema, codec)
      when 'r'
        io = open_reader(File.open(file_path, "rb"), schema)
      else
        raise DataFileError.new("Only modes 'r' and 'w' allowed. You gave #{mode.inspect}.")
      end

      yield io if block_given?
      io
    ensure
      io.close if block_given? && io
    end

    def self.codecs
      @codecs
    end

    def self.register_codec(codec)
      @codecs ||= {} of String => Class
      codec = codec.new unless codec.respond_to?(:codec_name) && codec.is_a?(Class)
      @codecs[codec.codec_name.to_s] = codec
    end

    def self.get_codec(codec)
      codec ||= "null"
      if codec.respond_to?(:compress) && codec.respond_to?(:decompress)
        codec
      elsif codec.is_a?(Class)
        codec.new
      elsif @codecs.include?(codec.to_s)
        @codecs[codec.to_s]
      else
        raise DataFileError.new("Unknown codec: #{codec.inspect}")
      end
    end

    private def self.open_writer(file, schema, codec=nil)
        writer = Avro::IO::DatumWriter.new(schema)
        Avro::DataFile::Writer.new(file, writer, schema, codec)
      end

      private def self.open_reader(file, schema)
        reader = Avro::IO::DatumReader.new(nil, schema)
        Avro::DataFile::Reader.new(file, reader)
      end


    class Writer
      def self.generate_sync_marker
        OpenSSL::Random.random_bytes(16)
      end

      attr_reader :writer, :encoder, :datum_writer, :buffer_writer, :buffer_encoder, :sync_marker, :meta, :codec
      attr_accessor :block_count

      def initialize(writer, datum_writer, writers_schema=nil, codec=nil, meta={} of String => Object)
        @writer = writer
        @encoder = IO::BinaryEncoder.new(@writer)
        @datum_writer = datum_writer
        @meta = meta
        @buffer_writer = IO::Memory.new
        @buffer_encoder = IO::BinaryEncoder.new(@buffer_writer)
        @block_count = 0

        if writers_schema
          @sync_marker = Writer.generate_sync_marker
          @codec = DataFile.get_codec(codec)
          @meta["avro.codec"] = @codec.codec_name.to_s
          @meta["avro.schema"] = writers_schema.to_s
          datum_writer.writers_schema = writers_schema
          write_header
        else
          dfr = Reader.new(writer, Avro::IO::DatumReader.new)

          @sync_marker = dfr.sync_marker
          @meta["avro.codec"] = dfr.meta["avro.codec"]
          @codec = DataFile.get_codec(meta["avro.codec"])

          schema_from_file = dfr.meta["avro.schema"]
          @meta["avro.schema"] = schema_from_file
          datum_writer.writers_schema = Schema.parse(schema_from_file)

          writer.seek(0,2)
        end
      end

      def <<(datum)
        datum_writer.write(datum, buffer_encoder)
        self.block_count += 1

        if buffer_writer.tell >= SYNC_INTERVAL
          write_block
        end
      end

      def sync
        write_block
        writer.tell
      end

      def flush
        write_block
        writer.flush
      end

      def close
        flush
        writer.close
      end

      private def write_header
        writer.write(MAGIC)
        datum_writer.write_data(META_SCHEMA, meta, encoder)
        writer.write(sync_marker)
      end

      private def write_block
        if block_count > 0
          encoder.write_long(block_count)
          to_write = codec.compress(buffer_writer.to_s)
          encoder.write_long(to_write.size)

          writer.write(to_write)
          writer.write(sync_marker)

          buffer_writer.clear
          self.block_count = 0
        end
      end
    end

    class Reader
      include Enumerable(IO::BinaryDecoder)

      attr_reader :reader, :decoder
      attr_reader :block_decoder
      attr_reader :datum_reader, :sync_marker, :meta, :file_length, :codec
      attr_accessor :block_count

      def initialize(reader, datum_reader)
        @reader = reader
        @decoder = IO::BinaryDecoder.new(reader)
        @datum_reader = datum_reader

        read_header

        @codec = DataFile.get_codec(meta["avro.codec"])

        @block_count = 0
        datum_reader.writers_schema = Schema.parse meta["avro.schema"]
      end

      def each
        loop do
          if block_count == 0
            case
            when eof?; break
            when skip_sync
              break if eof?
              read_block_header
            else
              read_block_header
            end
          end

          datum = datum_reader.read(block_decoder)
          self.block_count -= 1
          yield(datum)
        end
      end

      def eof?; reader.eof?; end

      def close
        reader.close
      end

      
      private def read_header
        reader.seek(0, 0)

        magic_in_file = reader.read(MAGIC_SIZE)
        if magic_in_file.size < MAGIC_SIZE
          msg = "Not an Avro data file: shorter than the Avro magic block"
          raise DataFileError.new(msg)
        elsif magic_in_file != MAGIC
          msg = "Not an Avro data file: #{magic_in_file.inspect} doesn't match #{MAGIC.inspect}"
          raise DataFileError.new(msg)
        end

        @meta = datum_reader.read_data(META_SCHEMA, META_SCHEMA, decoder)
        @sync_marker = reader.read(SYNC_SIZE)
      end

      private def read_block_header
        self.block_count = decoder.read_long
        block_bytes = decoder.read_long
        data = codec.decompress(reader.read(block_bytes))
        @block_decoder = IO::BinaryDecoder.new(IO::Memory.new(data))
      end

      private def skip_sync
        proposed_sync_marker = reader.read(SYNC_SIZE)
        if proposed_sync_marker != sync_marker
          reader.seek(-SYNC_SIZE, 1)
          false
        else
          true
        end
      end
    end

    class NullCodec
      def codec_name; "null"; end
      def decompress(data); data; end
      def compress(data); data; end
    end

    class DeflateCodec
      attr_reader :level

      def initialize(level=Zlib::DEFAULT_COMPRESSION)
        @level = level
      end

      def codec_name; "deflate"; end

      def decompress(compressed)
        zstream = Zlib::Inflate.new(-Zlib::MAX_WBITS)
        data = zstream.inflate(compressed)
        data << zstream.finish
      ensure
        zstream.close
      end

      def compress(data)
        zstream = Zlib::Deflate.new(level, -Zlib::MAX_WBITS)
        compressed = zstream.deflate(data)
        compressed << zstream.finish
      ensure
        zstream.close
      end
    end

    # class SnappyCodec
    #   def codec_name; "snappy"; end

    #   def decompress(data)
    #     crc32 = data.slice(-4..-1).unpack("N").first
    #     uncompressed = Snappy.inflate(data.slice(0..-5))

    #     if crc32 == Zlib.crc32(uncompressed)
    #       uncompressed
    #     else
    #       Snappy.inflate(data)
    #     end
    #   rescue Snappy::Error
    #     Snappy.inflate(data)
    #   end

    #   def compress(data)
    #     crc32 = Zlib.crc32(data)
    #     compressed = Snappy.deflate(data)
    #     [compressed, crc32].pack("a*N")
    #   end
    # end

    class ZstandardCodec
      def codec_name; "zstandard"; end

      def decompress(data)
        contxt = Zstd::Decompress::Context.new
        buffer = contxt.decompress data
        buffer
      end

      def compress(data)
        contxt = Zstd::Compress::Context.new()
        buffer = contxt.compress data
        buffer
      end
    end

    @codecs = {
      "null" => NullCodec.new,
      "deflate" => DeflateCodec.new,
      # "snappy" => SnappyCodec.new,
      "zstandard" => ZstandardCodec.new
    }

    VALID_CODECS = @codecs.keys
  end
end

