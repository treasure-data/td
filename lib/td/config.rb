module TreasureData
  class ConfigError < StandardError
  end

  class ConfigNotFoundError < ConfigError
  end

  class ConfigParseError < ConfigError
  end

  class Config
    # class variables
    @@path = ENV['TREASURE_DATA_CONFIG_PATH'] || ENV['TD_CONFIG_PATH'] || File.join(ENV['HOME'], '.td', 'td.conf')
    @@global_config_entries = nil

    def self.read(path=Config.path, create=false)
      ConfigFile.new(path)
    end

    def self.configfile_entries
      @@global_config_entries ||= ConfigFile.new(Config.path)
    end

    class OptionEntry
      attr_reader :name

      def initialize(name, envvar_names = [], configfile_entry_name = nil, &default_getter)
        @name = name
        @value = nil
        @envvar_names = envvar_names
        @configfile_entry_name = configfile_entry_name
        @default_getter = default_getter
        @command_line_override = false
      end

      def override?
        @command_line_override
      end

      def get
        return @value if @value
        @envvar_names.each do |envvar|
          value = ENV[envvar]
          return value if value && value != ""
        end
        if @configfile_entry_name && value = Config.configfile_entries[@configfile_entry_name]
          return value
        end
        if @default_getter
          return @default_getter.call
        end
        nil
      end

      def set(value)
        @value = value
        @command_line_override = true
      end
    end

    API_ENDPOINT_PATTERN = /\Aapi(-(?:staging|development))?(-[a-z0-9]+)?\.(connect\.)?(eu01\.)?treasuredata\.(com|co\.jp)\z/io

    def self.endpoint_hostname(endpoint)
      (endpoint || 'https://api.treasuredata.com').sub(%r[https?://], '').sub(%r!\:[0-9]+\z!, '')
    end

    def self.endpoint_variant(endpoint_type, api_endpoint)
      if API_ENDPOINT_PATTERN =~ endpoint_hostname(api_endpoint)
        case endpoint_type
        when :import
          "https://api#{$1}-import#{$2}.#{$3}#{$4}treasuredata.#{$5}"
        when :workflow
          "https://api#{$1}-workflow#{$2}.#{$3}#{$4}treasuredata.#{$5}"
        else
          raise ConfigError, "Invalid endpoint type '#{endpoint_type}'"
        end
      else
        raise ConfigError, "#{endpoint_type.to_s.capitalize} is not supported for '#{self.endpoint}'"
      end
    end

    @@apikey = OptionEntry.new(:apikey, ['TREASURE_DATA_API_KEY', 'TD_API_KEY'], 'account.apikey')
    @@endpoint = OptionEntry.new(:endpoint, ['TREASURE_DATA_API_SERVER', 'TD_API_SERVER'], 'account.endpoint')
    @@import_endpoint = OptionEntry.new(:import_endpoint) do
      endpoint_variant(:import, @@endpoint.get)
    end
    @@workflow_endpoint = OptionEntry.new(:workflow_endpoint) do
      endpoint_variant(:workflow, @@endpoint.get)
    end
    @@secure = true
    @@retry_post_requests = false

    def self.path
      @@path
    end

    def self.path=(path)
      @@path = path
      @@global_config_entries = nil
    end

    def self.secure
      @@secure
    end

    def self.secure=(secure)
      @@secure = secure
    end

    def self.retry_post_requests
      @@retry_post_requests
    end

    def self.retry_post_requests=(retry_post_requests)
      @@retry_post_requests = retry_post_requests
    end

    def self.apikey
      @@apikey.get
    end

    def self.apikey=(apikey)
      @@apikey.set(apikey)
    end

    def self.cl_apikey
      @@apikey.override?
    end

    def self.endpoint
      @@endpoint.get
    end

    def self.endpoint=(endpoint)
      @@endpoint.set(endpoint)
    end

    def self.cl_endpoint
      @@endpoint.override?
    end

    def self.import_endpoint
      @@import_endpoint.get
    end

    def self.import_endpoint=(endpoint)
      @@import_endpoint.set(endpoint)
    end

    def self.workflow_endpoint
      @@workflow_endpoint.get
    end

    def self.workflow_endpoint=(endpoint)
      @@workflow_endpoint.set(endpoint)
    end

    # renders the apikey and endpoint options as a string for the helper commands
    def self.cl_options_string
      string = ""
      string += "-k #{@@apikey.get} " if @@apikey.override?
      string += "-e #{@@endpoint.get} " if @@endpoint.override?
      string += "--import-endpoint #{@@import_endpoint.get} " if @@import_endpoint.override?
      string += "--workflow-endpoint #{@@workflow_endpoint.get}" if @@workflow_endpoint.override?
      string
    end

    class ConfigFile
      def initialize(path=nil)
        @path = path
        @conf = {}   # section.key = val
        read if path
      end

      def [](cate_key)
        @conf[cate_key]
      end

      def []=(cate_key, val)
        @conf[cate_key] = val
      end

      def delete(cate_key)
        @conf.delete(cate_key)
      end

      def read(path=@path)
        @path = path
        begin
          data = File.read(@path)
        rescue
          e = ConfigNotFoundError.new($!.to_s)
          e.set_backtrace($!.backtrace)
          raise e
        end

        section = ""

        data.each_line {|line|
          line.strip!
          case line
          when /^#/
            next
          when /\[(.+)\]/
            section = $~[1]
          when /^(\w+)\s*=\s*(.+?)\s*$/
            key = $~[1]
            val = $~[2]
            @conf["#{section}.#{key}"] = val
          else
            raise ConfigParseError, "invalid config line '#{line}' at #{@path}"
          end
        }

        self
      end

      def save(path=@path||Config.path)
        @path = path
        write
      end

      private
      def write
        require 'fileutils'
        FileUtils.mkdir_p File.dirname(@path)
        File.open(@path, "w") {|f|
          @conf.keys.map {|cate_key|
            cate_key.split('.', 2)
          }.zip(@conf.values).group_by {|(section,key), val|
            section
          }.each {|section,cate_key_vals|
            f.puts "[#{section}]"
            cate_key_vals.each {|(section,key), val|
              f.puts "  #{key} = #{val}"
            }
          }
        }
      end
    end
  end # class Config
end # module TreasureData
