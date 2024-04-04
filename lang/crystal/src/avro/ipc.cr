require "socket"
require "net/http"
require "json"

module Avro
  module IPC
    class AvroRemoteError < Avro::AvroError; end

    HANDSHAKE_REQUEST_SCHEMA = Avro::Schema.parse <<-JSON
    {
      "type": "record",
      "name": "HandshakeRequest", "namespace":"org.apache.avro.ipc",
      "fields": [
        {"name": "clientHash",
         "type": {"type": "fixed", "name": "MD5", "size": 16}},
        {"name": "clientProtocol", "type": ["null", "string"]},
        {"name": "serverHash", "type": "MD5"},
        {"name": "meta", "type": ["null", {"type": "map", "values": "bytes"}]}
      ]
    }
    JSON

    HANDSHAKE_RESPONSE_SCHEMA = Avro::Schema.parse <<-JSON
    {
      "type": "record",
      "name": "HandshakeResponse", "namespace": "org.apache.avro.ipc",
      "fields": [
        {"name": "match",
         "type": {"type": "enum", "name": "HandshakeMatch",
                  "symbols": ["BOTH", "CLIENT", "NONE"]}},
        {"name": "serverProtocol", "type": ["null", "string"]},
        {"name": "serverHash",
         "type": ["null", {"type": "fixed", "name": "MD5", "size": 16}]},
        {"name": "meta",
         "type": ["null", {"type": "map", "values": "bytes"}]}
      ]
    }
    JSON

    HANDSHAKE_REQUESTOR_WRITER = Avro::IO::DatumWriter.new(HANDSHAKE_REQUEST_SCHEMA)
    HANDSHAKE_REQUESTOR_READER = Avro::IO::DatumReader.new(HANDSHAKE_RESPONSE_SCHEMA)
    HANDSHAKE_RESPONDER_WRITER = Avro::IO::DatumWriter.new(HANDSHAKE_RESPONSE_SCHEMA)
    HANDSHAKE_RESPONDER_READER = Avro::IO::DatumReader.new(HANDSHAKE_REQUEST_SCHEMA)

    META_SCHEMA = Avro::Schema.parse('{"type": "map", "values": "bytes"}')
    META_WRITER = Avro::IO::DatumWriter.new(META_SCHEMA)
    META_READER = Avro::IO::DatumReader.new(META_SCHEMA)

    SYSTEM_ERROR_SCHEMA = Avro::Schema.parse('["string"]')

    REMOTE_HASHES = {} of String => Avro::Protocol
    REMOTE_PROTOCOLS = {} of String => Avro::Protocol

    BUFFER_HEADER_LENGTH = 4
    BUFFER_SIZE = 8192

    class AvroRemoteException < Avro::AvroError; end
    class ConnectionClosedException < Avro::AvroError; end

    class Requestor
      include Avro::IPC

      property local_protocol : Avro::Protocol
      property transport : Avro::IPC::SocketTransport
      property remote_protocol : Avro::Protocol | Nil
      property remote_hash : String | Nil
      property send_protocol : Bool | Nil

      def initialize(@local_protocol : Avro::Protocol, @transport : Avro::IPC::SocketTransport)
        @remote_protocol = nil
        @remote_hash = nil
        @send_protocol = nil
      end

      def remote_protocol=(new_remote_protocol : Avro::Protocol)
        @remote_protocol = new_remote_protocol
        REMOTE_PROTOCOLS[@transport.remote_name] = remote_protocol
      end

      def remote_hash=(new_remote_hash : String)
        @remote_hash = new_remote_hash
        REMOTE_HASHES[@transport.remote_name] = remote_hash
      end

      def request(message_name : String, request_datum)
        buffer_writer = IO::Memory.new
        buffer_encoder = Avro::IO::BinaryEncoder.new(buffer_writer)
        write_handshake_request(buffer_encoder)
        write_call_request(message_name, request_datum, buffer_encoder)

        call_request = buffer_writer.to_s
        call_response = @transport.transceive(call_request)

        buffer_decoder = Avro::IO::BinaryDecoder.new(IO::Memory.new(call_response))
        if read_handshake_response(buffer_decoder)
          read_call_response(message_name, buffer_decoder)
        else
          request(message_name, request_datum)
        end
      end

      private def write_handshake_request(encoder : Avro::IO::BinaryEncoder)
        local_hash = local_protocol.md5
        remote_name = transport.remote_name
        remote_hash = REMOTE_HASHES[remote_name]
        unless remote_hash
          remote_hash = local_hash
          self.remote_protocol = local_protocol
        end
        request_datum = {
          "clientHash" => local_hash,
          "serverHash" => remote_hash
        }
        if send_protocol
          request_datum["clientProtocol"] = local_protocol.to_s
        end
        HANDSHAKE_REQUESTOR_WRITER.write(request_datum, encoder)
      end

      private def write_call_request(message_name : String, request_datum, encoder : Avro::IO::BinaryEncoder)
        request_metadata = {}
        META_WRITER.write(request_metadata, encoder)

        message = local_protocol.messages[message_name]
        unless message
          raise AvroError.new("Unknown message: #{message_name}")
        end
        encoder.write_string(message.name)

        write_request(message.request, request_datum, encoder)
      end

      private def write_request(request_schema, request_datum, encoder : Avro::IO::BinaryEncoder)
        datum_writer = Avro::IO::DatumWriter.new(request_schema)
        datum_writer.write(request_datum, encoder)
      end

      private def read_handshake_response(decoder : Avro::IO::BinaryDecoder) : Bool
        handshake_response = HANDSHAKE_REQUESTOR_READER.read(decoder)
        we_have_matching_schema = false

        case handshake_response["match"]
        when "BOTH"
          self.send_protocol = false
          we_have_matching_schema = true
        when "CLIENT"
          raise AvroError.new("Handshake failure. match == CLIENT") if send_protocol
          self.remote_protocol = Avro::Protocol.parse(handshake_response["serverProtocol"])
          self.remote_hash = handshake_response["serverHash"]
          self.send_protocol = false
          we_have_matching_schema = true
        when "NONE"
          raise AvroError.new("Handshake failure. match == NONE") if send_protocol
          self.remote_protocol = Avro::Protocol.parse(handshake_response["serverProtocol"])
          self.remote_hash = handshake_response["serverHash"]
          self.send_protocol = true
        else
          raise AvroError.new("Unexpected match: #{match}")
        end

        return we_have_matching_schema
      end

      private def read_call_response(message_name : String, decoder : Avro::IO::BinaryDecoder)
        _response_metadata = META_READER.read(decoder)

        remote_message_schema = remote_protocol.messages[message_name]
        raise AvroError.new("Unknown remote message: #{message_name}") unless remote_message_schema

        local_message_schema = local_protocol.messages[message_name]
        unless local_message_schema
          raise AvroError.new("Unknown local
 message: #{message_name}")
        end

        if !decoder.read_boolean
          writers_schema = remote_message_schema.response
          readers_schema = local_message_schema.response
          read_response(writers_schema, readers_schema, decoder)
        else
          writers_schema = remote_message_schema.errors || SYSTEM_ERROR_SCHEMA
          readers_schema = local_message_schema.errors || SYSTEM_ERROR_SCHEMA
          raise read_error(writers_schema, readers_schema, decoder)
        end
      end

      private def read_response(writers_schema, readers_schema, decoder : Avro::IO::BinaryDecoder)
        datum_reader = Avro::IO::DatumReader.new(writers_schema, readers_schema)
        datum_reader.read(decoder)
      end

      private def read_error(writers_schema, readers_schema, decoder : Avro::IO::BinaryDecoder)
        datum_reader = Avro::IO::DatumReader.new(writers_schema, readers_schema)
        AvroRemoteError.new(datum_reader.read(decoder))
      end
    end

    class Responder
      include Avro::IPC

      property local_protocol : Avro::Protocol
      property local_hash : String
      property protocol_cache : Hash(String, Avro::Protocol)

      def initialize(@local_protocol : Avro::Protocol)
        @local_hash = self.local_protocol.md5
        @protocol_cache = { local_hash => local_protocol }
      end

      def respond(call_request : String, transport : Avro::IPC::SocketTransport? = nil)
        buffer_decoder = Avro::IO::BinaryDecoder.new(IO::Memory.new(call_request))
        buffer_writer = IO::Memory.new
        buffer_encoder = Avro::IO::BinaryEncoder.new(buffer_writer)
        error = nil
        response_metadata = {}

        begin
          remote_protocol = process_handshake(buffer_decoder, buffer_encoder, transport)

          unless remote_protocol
            return buffer_writer.to_s
          end

          _request_metadata = META_READER.read(buffer_decoder)
          remote_message_name = buffer_decoder.read_string

          remote_message = remote_protocol.messages[remote_message_name]
          unless remote_message
            raise AvroError.new("Unknown remote message: #{remote_message_name}")
          end

          local_message = local_protocol.messages[remote_message_name]
          unless local_message
            raise AvroError.new("Unknown local message: #{remote_message_name}")
          end

          writers_schema = remote_message.request
          readers_schema = local_message.request
          request = read_request(writers_schema, readers_schema, buffer_decoder)

          begin
            response = call(local_message, request)
          rescue AvroRemoteError => e
            error = e
          rescue Exception => e
            error = AvroRemoteError.new(e.to_s)
          end

          META_WRITER.write(response_metadata, buffer_encoder)
          buffer_encoder.write_boolean(!!error)

          if error.nil?
            writers_schema = local_message.response
            write_response(writers_schema, response, buffer_encoder)
          else
            writers_schema = local_message.errors || SYSTEM_ERROR_SCHEMA
            write_error(writers_schema, error, buffer_encoder)
          end
        rescue Avro::AvroError => e
          error = AvroRemoteException.new(e.to_s)
          buffer_encoder = Avro::IO::BinaryEncoder.new(IO::Memory.new)
          META_WRITER.write(response_metadata, buffer_encoder)
          buffer_encoder.write_boolean(true)
          write_error(SYSTEM_ERROR_SCHEMA, error, buffer_encoder)
        end

        buffer_writer.to_s
      end

      private def process_handshake(decoder : Avro::IO::BinaryDecoder, encoder : Avro::IO::BinaryEncoder, connection : Avro::IPC::SocketTransport? = nil) : Avro::Protocol?
        if connection && connection.is_connected?
          return connection.protocol
        end

        handshake_request = HANDSHAKE_RESPONDER_READER.read(decoder)
        handshake_response = {}

        client_hash = handshake_request["clientHash"]
        client_protocol = handshake_request["clientProtocol"]
        remote_protocol = protocol_cache[client_hash]

        if !remote_protocol && client_protocol
          remote_protocol = Avro::Protocol.parse(client_protocol)
          protocol_cache[client_hash] = remote_protocol
        end

        server_hash = handshake_request["serverHash"]

        if local_hash == server_hash
          if !remote_protocol
            handshake_response["match"] = "NONE"
          else
            handshake_response["match"] = "BOTH"
          end
        else
          if !remote_protocol
            handshake_response["match"] = "NONE"
          else
            handshake_response["match"] = "CLIENT"
          end
        end

        if handshake_response["match"] != "BOTH"
          handshake_response["serverProtocol"] = local_protocol.to_s
          handshake_response["serverHash"] = local_hash
        end

        HANDSHAKE_RESPONDER_WRITER.write(handshake_response, encoder)

        if connection && handshake_response["match"] != "NONE"
          connection.protocol = remote_protocol
        end

        remote_protocol
      end

      private def call(local_message, request)
        raise NotImplementedError.new
      end

      private def read_request(writers_schema, readers_schema, decoder)
        datum_reader = Avro::IO::DatumReader.new(writers_schema, readers_schema)
        datum_reader.read(decoder)
      end

      private def write_response(writers_schema, response_datum, encoder)
        datum_writer = Avro::IO::DatumWriter.new(writers_schema)
        datum_writer.write(response_datum, encoder)
      end

      private def write_error(writers_schema, error_exception, encoder)
        datum_writer = Avro::IO::DatumWriter.new(writers_schema)
        datum_writer.write(error_exception.to_s, encoder)
      end
    end

    class SocketTransport
      include Avro::IPC

      property sock : Socket
      property remote_name : String
      property protocol : Avro::Protocol | Nil

      def initialize(@sock : Socket)
        @protocol = nil
      end

      def is_connected? : Bool
        !!@protocol
      end

      def transceive(request : String) : String
        write_framed_message(request)
        read_framed_message
      end

      private def read_framed_message : String
        message = [] of String
        loop do
          buffer = IO::Memory.new
          buffer_length = read_buffer_length
          if buffer_length == 0
            return message.join
          end
          while buffer.tell < buffer_length
            chunk = sock.read(buffer_length - buffer.tell)
            if chunk == ""
              raise ConnectionClosedException.new("Socket read 0 bytes.")
            end
            buffer.write(chunk)
          end
          message << buffer.to_s
        end
      end

      private def write_framed_message(message : String)
        message_length = message.bytesize
        total_bytes_sent = 0
        while message_length - total_bytes_sent > 0
          if message_length - total_bytes_sent > BUFFER_SIZE
            buffer_length = BUFFER_SIZE
          else
            buffer_length = message_length - total_bytes_sent
          end
          write_buffer(message[total_bytes_sent,buffer_length])
          total_bytes_sent += buffer_length
        end
        write_buffer_length(0)
      end

      private def write_buffer(chunk)
        buffer_length = chunk.bytesize
        write_buffer_length(buffer_length)
        total_bytes_sent = 0
        while total_bytes_sent < buffer_length
          bytes_sent = self.sock.write(chunk[total_bytes_sent..-1])
          if bytes_sent == 0
            raise ConnectionClosedException.new("Socket sent 0 bytes.")
          end
          total_bytes_sent += bytes_sent
        end
      end

      private def write_buffer_length(n)
        bytes_sent = sock.write([n].pack("N"))
        if bytes_sent == 0
          raise ConnectionClosedException.new("socket sent 0 bytes")
        end
      end

      private def read_buffer_length
        read = sock.read(BUFFER_HEADER_LENGTH)
        if read == "" || read == nil
          raise ConnectionClosedException.new("Socket read 0 bytes.")
        end
        read.unpack("N")[0]
      end

      def close
        sock.close
      end
    end

    class ConnectionClosedError < Exception; end

    class FramedWriter
      property writer : IO::Memory

      def initialize(@writer : IO::Memory)
      end

      def write_framed_message(message : String)
        message_size = message.bytesize
        total_bytes_sent = 0
        while message_size - total_bytes_sent > 0
          if message_size - total_bytes_sent > BUFFER_SIZE
            buffer_size = BUFFER_SIZE
          else
            buffer_size = message_size - total_bytes_sent
          end
          write_buffer(message[total_bytes_sent, buffer_size])
          total_bytes_sent += buffer_size
        end
        write_buffer_size(0)
      end

      def to_s : String
        writer.to_s
      end

      private def write_buffer(chunk)
        buffer_size = chunk.bytesize
        write_buffer_size(buffer_size)
        writer << chunk
      end

      private def write_buffer_size(n)
        writer.write([n].pack("N"))
      end
    end

    class FramedReader
      property reader : IO::Memory

      def initialize(@reader : IO::Memory)
      end

      def read_framed_message : String
        message = [] of String
        loop do
          buffer = String.new("", encoding: "BINARY")
          buffer_size = read_buffer_size

          return message.join if buffer_size == 0

          while buffer.bytesize < buffer_size
            chunk = reader.read(buffer_size - buffer.bytesize)
            chunk_error?(chunk)
            buffer << chunk
          end
          message << buffer
        end
      end

      private def read_buffer_size
        header = reader.read(BUFFER_HEADER_LENGTH)
        chunk_error?(header)
        header.unpack("N")[0]
      end

      private def chunk_error?(chunk)
        raise ConnectionClosedError.new("Reader read 0 bytes") if chunk == ""
      end
    end

    class HTTPTransceiver
      include Avro::IPC

      property remote_name : String
      property host : String
      property port : Int32

      def initialize(@host : String, @port : Int32)
        @remote_name = "#{host}:#{port}"
        @conn = Net::HTTP.start(host, port)
      end

      def transceive(message : String) : String
        writer = FramedWriter.new(IO::Memory.new)
        writer.write_framed_message(message)
        resp = @conn.post("/", writer.to_s, { "Content-Type" => "avro/binary" })
        FramedReader.new(IO::Memory.new(resp.body)).read_framed_message
      end
    end
  end
end
