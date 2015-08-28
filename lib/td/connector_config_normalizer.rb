module TreasureData
  class ConnectorConfigNormalizer
    def initialize(config)
      @config = config
    end

    def normalized_config
      case
      when @config['in']
        {
          'in'      => @config['in'],
          'out'     => @config['out']  || {},
          'exec'    => @config['exec'] || {},
          'filters' => @config['filters'] || []
        }
      when @config['config']
        if @config.size != 1
          raise "Setting #{(@config.keys - ['config']).inspect} keys in a configuration file is not supported. Please set options to the command line argument."
        end

        self.class.new(@config['config']).normalized_config
      else
        {
          'in'   => @config,
          'out'  => {},
          'exec' => {},
          'filters' => []
        }
      end
    end
  end
end
