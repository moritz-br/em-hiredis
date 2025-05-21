require 'hiredis/reader'

module EventMachine::Hiredis
  class Connection < EM::Connection
    include EventMachine::Hiredis::EventEmitter

    # ssl_options are mandatory and assumed to be valid for an SSL connection (e.g. include :ca_file)
    def initialize(host, port, ssl_options)
      super()
      @host, @port = host, port
      @ssl_options = ssl_options # Assumed to be valid and for an SSL connection
      @name = "[em-hiredis #{@host}"
      @name << (@port ? ":#{@port}" : "")
      @name << " (SSL Mandatory)"
      @name << "]"

      @tls_established = false
      @reader = nil
    end

    def reconnect(host, port)
      super
      @host, @port = host, port
    end

    def post_init
      # SSL is mandatory. @ssl_options should be pre-validated by BaseClient.
      # BaseClient ensures @ssl_options[:ca_file] exists and we map it to cert_chain_file for EM
      # @ssl_options[:verify_peer] defaults to true if not set.
      EM::Hiredis.logger.debug { "#{to_s} Attempting to start TLS with params: #{@ssl_options.inspect}" }
      
      # Map the ssl_options keys to what EventMachine's start_tls expects
      tls_params_for_em = {}
      tls_params_for_em[:cert_chain_file] = @ssl_options[:ca_file] if @ssl_options[:ca_file]
      tls_params_for_em[:verify_peer] = @ssl_options.fetch(:verify_peer, true)
      
      EM::Hiredis.logger.debug { "#{to_s} Mapping SSL options to EM parameters: #{tls_params_for_em.inspect}" }
      start_tls(tls_params_for_em)
    end

    def ssl_handshake_completed
      EM::Hiredis.logger.debug { "#{to_s} SSL handshake completed." }
      @tls_established = true
      complete_connection_setup
    end

    def complete_connection_setup
      return if @reader # Already set up

      # SSL is mandatory. If TLS is not established here, it's a failure.
      unless @tls_established
        EM::Hiredis.logger.error { "#{to_s} SSL handshake did not complete. Aborting connection setup." }
        close_connection(true) # Force close, BaseClient will handle reconnect/failure.
        return
      end

      EM::Hiredis.logger.debug { "#{to_s} Completing connection setup after TLS." }
      @reader = ::Hiredis::Reader.new
      emit(:connected) # Raw TLS connection is ready
    end

    def receive_data(data)
      # Only process data if TLS is established (SSL is mandatory)
      return unless @tls_established && @reader

      @reader.feed(data)
      until (reply = @reader.gets) == false
        emit(:message, reply)
      end
    end

    def unbind
      emit(:closed)
    end

    def send_command(command, args)
      send_data(command_string(command, *args)) # Renamed internal helper
    end

    def to_s
      @name
    end

    protected

    COMMAND_DELIMITER = "\r\n"

    # Renamed from `command` to `command_string` to avoid conflict if any existed.
    def command_string(*args)
      cmd_parts = []
      cmd_parts << "*#{args.size}"

      args.each do |arg|
        arg_str = arg.to_s
        cmd_parts << "$#{string_size arg_str}"
        cmd_parts << arg_str
      end

      cmd_parts.join(COMMAND_DELIMITER) + COMMAND_DELIMITER
    end

    if "".respond_to?(:bytesize)
      def string_size(string)
        string.to_s.bytesize
      end
    else
      def string_size(string)
        string.to_s.size
      end
    end
  end
end
