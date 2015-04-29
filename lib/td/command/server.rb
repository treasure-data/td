require 'uri'

module TreasureData
module Command

  def server_status(op)
    op.cmd_parse

    $stdout.puts Client.server_status
  end

  def server_endpoint(op)
    endpoint = op.cmd_parse

    if Config.cl_endpoint and endpoint != Config.endpoint
      raise ParameterConfigurationError,
            "You specified the API server endpoint in the command options as well (-e / --endpoint " +
            "option) but it does not match the value provided to the 'server:endpoint' command. " +
            "Please remove the option or ensure the endpoints URLs match each other."
    end

    Command.validate_api_endpoint(endpoint)
    Command.test_api_endpoint(endpoint)

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

