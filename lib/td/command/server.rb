require 'uri'

module TreasureData
module Command

  def server_status(op)
    op.cmd_parse

    puts Client.server_status
  end

  def server_endpoint(op)
    endpoint = op.cmd_parse

    uri = URI.parse(endpoint)
    unless uri.kind_of?(URI::HTTP) || uri.kind_of?(URI::HTTPS)
      raise ParameterConfigurationError,
            "API server endpoint URL must use 'http' or 'https' protocol. Example format: 'https://api.treasuredata.com'"
    end

    if !(md = /(\d{1,3}).(\d{1,3}).(\d{1,3}).(\d{1,3})/.match(uri.host)).nil? # IP address
      md[1..-1].each { |v|
        if v.to_i < 0 || v.to_i > 255
          raise ParameterConfigurationError,
                "API server IP address must a 4 integers tuple, with every integer in the [0,255] range. Example format: 'https://1.2.3.4'"
        end
      }
    else # host name validation
      unless uri.host =~ /\.treasure\-?data\.com$/
        raise ParameterConfigurationError,
              "API server endpoint URL must end with '.treasuredata.com' or '.treasure-data.com'. Example format: 'https://api.treasuredata.com'"
      end
      unless uri.host =~ /[\d\w\.]+\.treasure\-?data\.com$/
        raise ParameterConfigurationError,
              "API server endpoint URL must have prefix before '.treasuredata.com' or '.treasure-data.com'. Example format: 'https://api.treasuredata.com'."
      end
    end

    if Config.cl_endpoint and endpoint != Config.endpoint
      raise ParameterConfigurationError,
            "You specified the API server endpoint in the command options as well (-e / --endpoint option) but it does not match the value provided to the 'server:endpoint' command. Please remove the option or ensure the endpoints URLs match each other."
    end

    conf = nil
    begin
      conf = Config.read
    rescue ConfigError
      conf = Config.new
    end
    conf["account.endpoint"] = endpoint
    conf.save
  end

end
end

