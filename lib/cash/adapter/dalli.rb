require 'dalli'

module Cash
  module Adapter
    class Dalli

      def initialize(repository, options = {})
        @repository = repository
        @logger = options[:logger]
        @default_ttl = options[:default_ttl] || raise(":default_ttl is a required option")
      end
      
      def add(key, value, ttl=nil, raw=false)
        wrap(key, not_stored) do
          logger.debug("Dalli add: #{key.inspect}") if debug_logger?
          @repository.add(key,value, ttl || @default_ttl, :raw => raw)
          logger.debug("Dalli hit: #{key.inspect}") if debug_logger?
          stored
        end
      end
      
      def get(key, raw=false)
        wrap(key) do
          logger.debug("Dalli get: #{key.inspect}") if debug_logger?
          value = wrap(key) { @repository.get(key, :raw => raw) }
          logger.debug("Dalli hit: #{key.inspect}") if debug_logger?
          value
        end
      end
      
      def get_multi(*keys)
        wrap(keys, {}) do
          begin
            keys.flatten!
            logger.debug("Dalli get_multi: #{keys.inspect}") if debug_logger?
            values = @repository.get_multi(keys)
            logger.debug("Dalli hit: #{keys.inspect}") if debug_logger?
            values
          rescue TypeError
            log_error($!) if logger
            keys.each { |key| delete(key) }
            logger.debug("Dalli deleted: #{keys.inspect}") if debug_logger?
            {}
          end
        end
      end
      
      def set(key, value, ttl=nil, raw=false)
        wrap(key, not_stored) do
          logger.debug("Dalli set: #{key.inspect}") if debug_logger?
          @repository.set(key, value, ttl || @default_ttl, :raw => raw)
          logger.debug("Dalli hit: #{key.inspect}") if debug_logger?
          stored
        end
      end
      
      def delete(key)
        wrap(key, not_found) do
          logger.debug("Dalli delete: #{key.inspect}") if debug_logger?
          @repository.delete(key)
          logger.debug("Dalli hit: #{key.inspect}") if debug_logger?
          deleted
        end
      end
      
      def get_server_for_key(key)
        wrap(key) { @repository.send(:ring).server_by_key(key) }
      end

      def incr(key, value = 1)
        wrap(key) { @repository.incr(key, value) }
      end

      def decr(key, value = 1)
        wrap(key) { @repository.decr(key, value) }
      end
      
      def flush_all
        @repository.flush
      end
 
      def exception_classes
        [::Dalli::NetworkError, ::Dalli::DalliError, ::Dalli::RingError]
      end

      private

      def stored
        "STORED\r\n"
      end

      def deleted
        "DELETED\r\n"
      end

      def not_stored
        "NOT_STORED\r\n"
      end

      def not_found
        "NOT_FOUND\r\n"
      end

      def logger
        @logger
      end

      def debug_logger?
        logger && logger.respond_to?(:debug?) && logger.debug?
      end

      def wrap(key, error_value = nil, options = {})
        yield
      rescue *exception_classes
        log_error($!) if logger
        raise if options[:reraise_error]
        error_value
      end

      def log_error(err)
        logger.error("Dalli ERROR, #{err.class}: #{err}") if logger
      end
    end
  end
end
