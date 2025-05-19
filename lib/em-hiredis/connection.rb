require 'hiredis/reader'

module EventMachine::Hiredis
  class Connection < EM::Connection
    include EventMachine::Hiredis::EventEmitter

    def initialize(host, port, ssl_options)
      super()
      @host, @port = host, port
      @ssl_options = ssl_options
      @name = "[em-hiredis #{@host}"
      @name << (@port ? ":#{@port}" : "")
      @name << " (SSL)"
      @name << "]"

      @tls_established = false
      @reader = nil
    end

    def reconnect(host, port)
      super
      @host, @port = host, port
    end

    def post_init
      tls_params = @ssl_options.is_a?(Hash) ? @ssl_options : {}
      start_tls(tls_params)
    end

    def ssl_handshake_completed
      @tls_established = true
      complete_connection_setup
    end

    def complete_connection_setup
      return unless @tls_established && @reader.nil?

      @reader = ::Hiredis::Reader.new
      emit(:connected)
    end

    def receive_data(data)
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
      send_data(command(command, *args))
    end

    def to_s
      @name
    end

    protected

    COMMAND_DELIMITER = "\r\n"

    def command(*args)
      command = []
      command << "*#{args.size}"

      args.each do |arg|
        arg = arg.to_s
        command << "$#{string_size arg}"
        command << arg
      end

      command.join(COMMAND_DELIMITER) + COMMAND_DELIMITER
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
