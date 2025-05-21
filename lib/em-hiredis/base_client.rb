require 'uri'
require 'eventmachine' # Ensure EventMachine is available for EM::DefaultDeferrable

module EventMachine::Hiredis
  # Emits the following events
  #
  # * :connected - on successful connection or reconnection
  # * :reconnected - on successful reconnection
  # * :disconnected - no longer connected, when previously in connected state
  # * :reconnect_failed(failure_number) - a reconnect attempt failed
  #     This event is passed number of failures so far (1,2,3...)
  # * :monitor
  #
  class BaseClient
    include EventEmitter
    # We will manage our own top-level deferrable for connection establishment
    # include EM::Deferrable # Removing this, connect will return its own deferrable

    attr_reader :original_host, :original_port, :original_password, :original_db # Retain for potential reference or URI parsing
    attr_reader :ssl_options, :sentinel_params
    attr_reader :current_target_host, :current_target_port

    DEFAULT_MAX_CONNECT_RETRIES = 5
    DEFAULT_RECONNECT_DELAY = 1.0

    # sentinel_params: { sentinels: [{host: 'h', port: p, password: 'pw'}, ...], master_name: 'name', sentinel_password: 'global_sentinel_password_if_any'}
    # ssl_options: { ca_file: 'path', verify_peer: true } # verify_peer defaults to true if not specified.
    def initialize(host = 'localhost', port = 6379, password = nil, db = nil, ssl_options = {}, sentinel_params = {})
      # Store initial host/port primarily for reference; actual target comes from Sentinel.
      @direct_connection_mode = (sentinel_params == :_direct_connect_)

      unless ssl_options.is_a?(Hash) && ssl_options[:ca_file]
        raise ArgumentError, "ssl_options must be a Hash and include :ca_file for mandatory SSL."
      end
      @ssl_options = deep_copy(ssl_options)
      @ssl_options[:verify_peer] = true unless @ssl_options.key?(:verify_peer)

      if @direct_connection_mode
        EM::Hiredis.logger.debug { "[EM::Hiredis::BaseClient] Initializing in Direct Connection Mode for #{host}:#{port}" }
        @current_target_host = host
        @current_target_port = port
        @password = password
        @db = db || 0
        @sentinel_params = {} # Not used in direct mode
      else
        # Standard Sentinel-based initialization
        @original_host = host 
        @original_port = port 
        @original_password = password # This is the password for the Redis master
        @original_db = db || 0       # This is the DB for the Redis master

        unless sentinel_params.is_a?(Hash) && 
               sentinel_params[:sentinels].is_a?(Array) && !sentinel_params[:sentinels].empty? && 
               sentinel_params[:master_name].is_a?(String) && !sentinel_params[:master_name].empty?
          raise ArgumentError, "sentinel_params must include a non-empty :sentinels array and a :master_name string."
        end
        @sentinel_params = deep_copy(sentinel_params)
        @current_sentinel_index = 0

        # Use the direct password and db params for the master connection (resolved via Sentinel)
        @password = @original_password 
        @db = @original_db

        @current_target_host = nil # Will be resolved by Sentinel
        @current_target_port = nil # Will be resolved by Sentinel
      end
      
      # URI parsing block removed as configuration is direct and fixed.
      
      @defs = []
      @command_queue = []
      @reconnect_tries = 0
      @reconnect_timer = nil
      @connection_active = false
      @fully_connected = false
      @is_connecting = false
      @permanent_failure = false
      @connection_establishment_deferrable = nil
    end

    def connect
      return @connection_establishment_deferrable if @is_connecting && @connection_establishment_deferrable
      raise Error, "Already connected or in a permanent failure state." if @fully_connected || @permanent_failure

      @is_connecting = true
      @permanent_failure = false
      @reconnect_tries = 0
      @connection_establishment_deferrable = EM::DefaultDeferrable.new
      if @direct_connection_mode
        # In direct mode, max retries and delay don't apply in the same way as Sentinel discovery.
        # The connection attempt is more straightforward. Timeout can be shorter.
        # For a single direct attempt, perhaps a simple, shorter timeout or rely on EM's default.
        # Let's use a fixed timeout for direct connections for now.
        @connection_establishment_deferrable.timeout(10) # E.g., 10 seconds for a direct connection attempt
        EM::Hiredis.logger.debug { "#{_log_prefix} Direct connection mode: attempting to establish raw connection."}
        # SSL options for direct connection are simply @ssl_options
        _establish_raw_connection(@current_target_host, @current_target_port, @ssl_options)
      else
        # Sentinel-based connection with retries
        timeout_duration = (DEFAULT_MAX_CONNECT_RETRIES * DEFAULT_RECONNECT_DELAY) + 10 
        @connection_establishment_deferrable.timeout(timeout_duration) if timeout_duration > 0
        _initiate_connection_attempt
      end
      @connection_establishment_deferrable
    end
    
    def connected?
      @fully_connected
    end

    def close_connection
      EM::Hiredis.logger.debug { "#{_log_prefix} close_connection called." }
      @is_connecting = false 
      @fully_connected = false 
      _cancel_reconnect_timer
      if @connection
        @connection.close_connection_after_writing
        @connection = nil
      end
    end

    private

    def _log_prefix
      prefix = "[EM::Hiredis::BaseClient"
      if @direct_connection_mode
        prefix << " Direct: #{@current_target_host}:#{@current_target_port}"
      else
        prefix << " Sentinel: #{@sentinel_params[:master_name]}"
        prefix << " Target: #{@current_target_host}:#{@current_target_port}" if @current_target_host
      end
      prefix << "]"
      prefix
    end
    
    def deep_copy(obj)
      Marshal.load(Marshal.dump(obj))
    rescue TypeError 
      obj.dup
    end

    def _initiate_connection_attempt
      EM::Hiredis.logger.debug { "#{_log_prefix} Initiating connection attempt (Try #{@reconnect_tries + 1}/#{DEFAULT_MAX_CONNECT_RETRIES})." }
      if @permanent_failure
        EM::Hiredis.logger.warn { "#{_log_prefix} In permanent failure state. Aborting connection attempt."}
        if @connection_establishment_deferrable && !@connection_establishment_deferrable.instance_variable_get(:@called)
          @connection_establishment_deferrable.fail(Error.new("Connection permanently failed."))
        end
        return
      end
      _connect_via_sentinel
    end

    def _connect_via_sentinel(sentinel_index_offset = 0) 
      sentinels = @sentinel_params.fetch(:sentinels, []) 
      @current_sentinel_index = (@current_sentinel_index + sentinel_index_offset) % sentinels.size 
      sentinel_conf = sentinels[@current_sentinel_index]
      sentinel_host = sentinel_conf[:host]
      sentinel_port = sentinel_conf[:port]
      sentinel_password = sentinel_conf[:password] || @sentinel_params[:sentinel_password]
      EM::Hiredis.logger.info { "#{_log_prefix} Connecting to Sentinel (SSL) #{sentinel_host}:#{sentinel_port} to find master '#{@sentinel_params[:master_name]}'." }
      # Instantiate the temporary client for Sentinel communication in direct connection mode.
      # It uses the main @ssl_options for its connection to the Sentinel server.
      # The db (0) and sentinel_params (:_direct_connect_) are specific to this internal usage.
      temp_sentinel_client = self.class.new(sentinel_host, sentinel_port, sentinel_password, 0, @ssl_options, :_direct_connect_)
      sentinel_conn_df = temp_sentinel_client.connect
      sentinel_conn_df.callback do
        EM::Hiredis.logger.debug { "#{_log_prefix} Connected to Sentinel #{sentinel_host}:#{sentinel_port}." }
        temp_sentinel_client.sentinel("get-master-addr-by-name", @sentinel_params[:master_name]).callback do |master_addr|
          temp_sentinel_client.close_connection
          if master_addr && master_addr.is_a?(Array) && master_addr.size == 2
            @current_target_host = master_addr[0]
            @current_target_port = master_addr[1].to_i
            EM::Hiredis.logger.info { "#{_log_prefix} Sentinel resolved master '#{@sentinel_params[:master_name]}' to #{@current_target_host}:#{@current_target_port}." }
            _establish_raw_connection(@current_target_host, @current_target_port, @ssl_options) 
          else
            EM::Hiredis.logger.error { "#{_log_prefix} Sentinel #{sentinel_host} failed to provide master address for '#{@sentinel_params[:master_name]}': #{master_addr.inspect}" }
            _handle_connection_failure("Failed to get master from Sentinel #{sentinel_host}.")
          end
        end.errback do |err|
          temp_sentinel_client.close_connection
          EM::Hiredis.logger.error { "#{_log_prefix} Sentinel #{sentinel_host} command error: #{err.inspect}" }
          _handle_connection_failure("Sentinel command error from #{sentinel_host}: #{err}.")
        end
      end
      sentinel_conn_df.errback do |err|
        EM::Hiredis.logger.error { "#{_log_prefix} Failed to connect to Sentinel #{sentinel_host}:#{sentinel_port}: #{err.inspect}" }
        _handle_connection_failure("Failed to connect to Sentinel #{sentinel_host}: #{err}.")
      end
    end

    def _establish_raw_connection(host, port, ssl_opts_to_use)
      EM::Hiredis.logger.debug { "#{_log_prefix} Establishing raw SSL connection to #{host}:#{port}" }
      begin
        @connection = EM.connect(host, port, Connection, host, port, ssl_opts_to_use) 
      rescue => e
        EM::Hiredis.logger.error { "#{_log_prefix} EM.connect raised an exception for #{host}:#{port}: #{e.message}" }
        _handle_connection_failure("EM.connect failed: #{e.message}")
        return
      end
      @connection_active = true
      @connection.on(:connected) do 
        EM::Hiredis.logger.info { "#{_log_prefix} Raw SSL connection established to #{@current_target_host}:#{@current_target_port} (TLS up)." }
        @connection_active = true
        _perform_redis_handshake
      end
      @connection.on(:closed) do 
        EM::Hiredis.logger.info { "#{_log_prefix} Raw SSL connection closed to #{@current_target_host}:#{@current_target_port}."}
        @connection_active = false
        @fully_connected = false
        @connection = nil 
        @defs.each { |d| d.fail(Error.new("Redis disconnected")) unless d.instance_variable_get(:@called) }
        @defs = []
        emit(:disconnected) 
        _handle_connection_failure("Connection closed")
      end
      
      # Simplified RuntimeError handling for messages from Connection
      @connection.on(:message) do |reply|
        if RuntimeError === reply
          if !@defs.empty?
            # This was an error reply to a pending command
            deferred = @defs.shift
            error = RedisError.new(reply.message) 
            error.redis_error = reply 
            deferred.fail(error) if deferred && !deferred.instance_variable_get(:@called)
          else
            # A RuntimeError with no pending command is unexpected/out-of-sync
            EM::Hiredis.logger.error {"#{_log_prefix} Unexpected Redis error with no pending command: #{reply.inspect}"}
          end
        else
          # Normal reply (not a RuntimeError), pass to general reply handler
          _handle_reply(reply)
        end
      end
    end

    def _perform_redis_handshake
      handshake_sequence = EM::Queue.new
      if @password && !@password.empty?
        handshake_sequence.push(proc {
          df = EM::DefaultDeferrable.new
          auth_df = method_missing(:auth, @password)
          auth_df.callback { df.succeed }
          auth_df.errback { |err| df.fail(["Auth failed", err]) }
          df
        })
      end
      if @db != 0
        handshake_sequence.push(proc {
          df = EM::DefaultDeferrable.new
          select_df = method_missing(:select, @db)
          select_df.callback { df.succeed }
          select_df.errback { |err| df.fail(["Select DB failed", err]) }
          df
        })
      end
      process_handshake = proc do |_unused_op_df_result|
        if handshake_sequence.empty?
          EM::Hiredis.logger.info { "#{_log_prefix} Redis handshake successful (Auth/Select DB)." }
          @fully_connected = true
          @is_connecting = false
          @reconnect_tries = 0 
          @command_queue.each do |queued_df, command, args|
            actual_df = method_missing(command, *args) 
            actual_df.callback(&queued_df.method(:succeed))
            actual_df.errback(&queued_df.method(:fail))
          end
          @command_queue.clear
          emit(:connected) 
          if @connection_establishment_deferrable && !@connection_establishment_deferrable.instance_variable_get(:@called)
            @connection_establishment_deferrable.succeed(self)
          end
        else 
          op_proc = handshake_sequence.pop
          current_op_df = op_proc.call
          current_op_df.callback(&process_handshake) 
          current_op_df.errback do |err_info|
            reason, err = err_info.is_a?(Array) ? err_info : [err_info.to_s, err_info]
            EM::Hiredis.logger.error { "#{_log_prefix} Redis handshake failed: #{reason} - #{err.inspect}" }
            @connection.close_connection if @connection 
          end
        end
      end
      handshake_sequence.pop(&process_handshake)
    end

    def _handle_reply(reply)
      if @defs.empty?
        is_currently_monitoring = instance_variable_get(:@monitoring) == true

        if is_currently_monitoring
          emit(:monitor, reply) 
        elsif self.is_a?(PubsubClient) && reply.is_a?(Array) && PubsubClient::PUBSUB_MESSAGES.include?(reply[0].to_s)
          EM::Hiredis.logger.debug {"#{_log_prefix} PubSub-like message received by BaseClient for PubSub client instance (should be rare): #{reply.inspect}"}
        else
           EM::Hiredis.logger.debug {"#{_log_prefix} Received reply with no pending deferrable (not monitoring, not pubsub): #{reply.inspect}"}
        end
      else
        deferred = @defs.shift
        deferred.succeed(reply) if deferred && !deferred.instance_variable_get(:@called)
      end
    end
    
    def _cancel_reconnect_timer
      if @reconnect_timer
        EM.cancel_timer(@reconnect_timer)
        @reconnect_timer = nil
      end
    end

    def _handle_connection_failure(reason_str)
      EM::Hiredis.logger.warn { "#{_log_prefix} Connection failure: #{reason_str}. Current tries: #{@reconnect_tries}."}
      _cancel_reconnect_timer 
      @fully_connected = false 

      # If this is a direct_connection_mode client (e.g., a temporary client for Sentinel communication),
      # a connection failure should immediately fail its top-level deferrable and not attempt Sentinel-based reconnections.
      if @direct_connection_mode
        EM::Hiredis.logger.warn { "#{_log_prefix} Direct connection failed. Failing establish deferrable." }
        @permanent_failure = true # Mark as failed to prevent any other actions
        @is_connecting = false    # No longer trying to connect
        if @connection_establishment_deferrable && !@connection_establishment_deferrable.instance_variable_get(:@called)
          @connection_establishment_deferrable.fail(Error.new("Direct connection failed: #{reason_str}"))
        end
        # For a direct_connection_mode client, we also need to fail any pending commands in @defs or @command_queue
        # as there will be no further connection attempts for this instance.
        @defs.each { |d| d.fail(Error.new("Direct connection failed: #{reason_str}")) unless d.instance_variable_get(:@called) }
        @defs.clear
        @command_queue.each do |df, _, _|
          df.fail(Error.new("Direct connection failed: #{reason_str}")) unless df.instance_variable_get(:@called)
        end
        @command_queue.clear
        return # Do not proceed with Sentinel retry logic for direct mode clients
      end

      if @permanent_failure
         EM::Hiredis.logger.error { "#{_log_prefix} In permanent failure state. Not retrying."}
         if @connection_establishment_deferrable && !@connection_establishment_deferrable.instance_variable_get(:@called)
            @connection_establishment_deferrable.fail(Error.new("Connection permanently failed: #{reason_str}"))
         end
         return
      end

      @reconnect_tries += 1
      if @reconnect_tries <= DEFAULT_MAX_CONNECT_RETRIES
        delay = DEFAULT_RECONNECT_DELAY
        EM::Hiredis.logger.info { "#{_log_prefix} Scheduling reconnect attempt #{@reconnect_tries}/#{DEFAULT_MAX_CONNECT_RETRIES} in #{delay.round(2)}s. Reason: #{reason_str}" }
        emit(:reconnect_scheduled, @reconnect_tries, delay) 
        @reconnect_timer = EM.add_timer(delay) do
          @reconnect_timer = nil
          @current_sentinel_index = (@current_sentinel_index + 1) % @sentinel_params.fetch(:sentinels, [{host: 'fallback'}]).size 
          _initiate_connection_attempt
        end
      else
        EM::Hiredis.logger.error { "#{_log_prefix} Max reconnect attempts (#{DEFAULT_MAX_CONNECT_RETRIES}) reached. Marking as permanent failure. Last reason: #{reason_str}" }
        @permanent_failure = true
        @is_connecting = false
        emit(:failed_permanently, reason_str) 
        if @connection_establishment_deferrable && !@connection_establishment_deferrable.instance_variable_get(:@called)
          @connection_establishment_deferrable.fail(Error.new("Failed to connect after #{DEFAULT_MAX_CONNECT_RETRIES} retries: #{reason_str}"))
        end
        @command_queue.each do |df, _, _|
          df.fail(Error.new("Redis connection permanently failed")) unless df.instance_variable_get(:@called)
        end
        @command_queue.clear
      end
    end

    # Public command interface
    def method_missing(command_name, *args, &block)
      df = EM::DefaultDeferrable.new
      df.callback(&block) if block_given?
      if @permanent_failure
        df.fail(Error.new("Redis connection in permanent failure state"))
      elsif @fully_connected && @connection 
        begin
          @connection.send_command(command_name.to_s.upcase, args)
          @defs.push(df)
        rescue => e 
          EM::Hiredis.logger.error {"#{_log_prefix} Error sending command #{command_name} while connected: #{e.message}"}
          df.fail(e)
          @connection.close_connection if @connection
        end
      elsif @is_connecting || !@fully_connected 
         EM::Hiredis.logger.debug {"#{_log_prefix} Queuing command: #{command_name}"}
        @command_queue << [df, command_name, args]
      else
        df.fail(Error.new("Redis client not connected and not attempting to connect. Call #connect first."))
      end
      df
    end

    # Specific commands can be defined for clarity or custom processing
    def auth(password_to_auth, &block)
      @password = password_to_auth 
      method_missing(:auth, @password, &block)
    end

    def select(db_to_select, &block)
      @db = db_to_select.to_i
      method_missing(:select, @db, &block)
    end
    
    def sentinel(*args, &block)
      method_missing(:sentinel, *args, &block)
    end
    
    def send_command_direct(command_name, *args)
      if @fully_connected && @connection
        @connection.send_command(command_name.to_s.upcase, args)
        true
      else
        false
      end
    end

  end
end
