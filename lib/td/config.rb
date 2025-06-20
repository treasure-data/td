
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
  @@apikey = ENV['TREASURE_DATA_API_KEY'] || ENV['TD_API_KEY']
  @@apikey = nil if @@apikey == ""
  @@cl_apikey = false # flag to indicate whether an apikey has been provided through the command-line
  @@endpoint = ENV['TREASURE_DATA_API_SERVER'] || ENV['TD_API_SERVER']
  @@endpoint = nil if @@endpoint == ""
  @@cl_endpoint = false # flag to indicate whether an endpoint has been provided through the command-line
  @@import_endpoint = ENV['TREASURE_DATA_API_IMPORT_SERVER'] || ENV['TD_API_IMPORT_SERVER']
  @@import_endpoint = nil if @@endpoint == ""
  @@cl_import_endpoint = false # flag to indicate whether an endpoint has been provided through the command-line option
  @@secure = true
  @@retry_post_requests = false
  @@ssl_option = ENV['TD_SSL_VERIFY'] || ENV['TD_SSL_CA_FILE'] || true
  @@cl_ssl_option = false # flag to indicate whether ssl option has been provided through the command-line

  def initialize
    @path = nil
    @conf = {}   # section.key = val
  end

  def self.read(path=Config.path, create=false)
    new.read(path)
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


  def self.path
    @@path
  end

  def self.path=(path)
    @@path = path
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
    @@apikey || Config.read['account.apikey']
  end

  def self.apikey=(apikey)
    @@apikey = apikey
  end

  def self.cl_apikey
    @@cl_apikey
  end

  def self.cl_apikey=(flag)
    @@cl_apikey = flag
  end


  def self.endpoint
    endpoint = @@endpoint || Config.read['account.endpoint']
    endpoint.sub(/(\/)+$/, '') if endpoint
  end

  def self.endpoint=(endpoint)
    @@endpoint = endpoint
  end

  def self.endpoint_domain
    (self.endpoint || 'api.treasuredata.com').sub(%r[https?://], '')
  end

  def self.cl_endpoint
    @@cl_endpoint
  end

  def self.cl_endpoint=(flag)
    @@cl_endpoint = flag
  end

  def self.import_endpoint
    endpoint = @@import_endpoint || Config.read['account.import_endpoint']
    endpoint.sub(/(\/)+$/, '') if endpoint
  end

  def self.import_endpoint=(endpoint)
    @@import_endpoint = endpoint
  end

  def self.cl_import_endpoint
    @@cl_import_endpoint
  end

  def self.cl_import_endpoint=(flag)
    @@cl_import_endpoint = flag
  end

  def self.workflow_endpoint
    case self.endpoint_domain
    when /\Aapi(-(?:staging|development))?(-[a-z0-9]+)?\.(connect\.)?((?:eu01|ap02|ap03)\.)?treasuredata\.(com|co\.jp)\z/i
      "https://api#{$1}-workflow#{$2}.#{$3}#{$4}treasuredata.#{$5}"
    else
      raise ConfigError, "Workflow is not supported for '#{self.endpoint}'"
    end
  end

  def self.ssl_option
    if @@cl_ssl_option
      return @@ssl_option
    end

    begin
      conf = read
      if conf['ssl.verify']
        return conf['ssl.verify'].downcase == 'false' ? false : conf['ssl.verify']
      elsif conf['ssl.ca_file']
        return conf['ssl.ca_file']
      end
    rescue ConfigNotFoundError
    end

    return @@ssl_option
  end

  def self.ssl_option=(option)
    @@ssl_option = option
  end

  def self.cl_ssl_option
    @@cl_ssl_option
  end

  def self.cl_ssl_option=(flag)
    @@cl_ssl_option = flag
  end

  # Compatibility methods for existing code
  def self.ssl_verify
    option = ssl_option
    return false if option == false || (option.is_a?(String) && option.downcase == 'false')
    return true if option == true || option.nil?
    return true # if it's a file path, verification is enabled
  end

  def self.ssl_verify=(verify)
    self.ssl_option = verify
  end

  def self.ssl_ca_file
    option = ssl_option
    return option if option.is_a?(String) && option.downcase != 'false' && File.exist?(option)
    return nil
  end

  def self.ssl_ca_file=(ca_file)
    self.ssl_option = ca_file
  end

  def self.cl_ssl_verify
    @@cl_ssl_option
  end

  def self.cl_ssl_verify=(flag)
    @@cl_ssl_option = flag
  end

  def self.cl_ssl_ca_file
    @@cl_ssl_option
  end

  def self.cl_ssl_ca_file=(flag)
    @@cl_ssl_option = flag
  end

  # renders the apikey and endpoint options as a string for the helper commands
  def self.cl_options_string
    require 'shellwords'
    
    string = ""
    string += "-k #{@@apikey} " if @@cl_apikey
    string += "-e #{@@endpoint} " if @@cl_endpoint
    string += "--import-endpoint #{@@import_endpoint} " if @@cl_import_endpoint
    
    # Handle simplified SSL option
    if @@cl_ssl_option && @@ssl_option
      if @@ssl_option == false || (@@ssl_option.is_a?(String) && @@ssl_option.downcase == 'false')
        string += "--ssl-verify false "
      elsif @@ssl_option.is_a?(String)
        string += "--ssl-verify #{Shellwords.escape(@@ssl_option)} "
      end
    end
    
    string
  end

end # class Config
end # module TreasureData
