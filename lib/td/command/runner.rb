module TreasureData
module Command

class Runner
  def initialize
    @config_path = nil
    @apikey = nil
    @endpoint = nil
    @prog_name = nil
    @insecure = false
  end

  attr_accessor :apikey, :endpoint, :config_path, :prog_name, :insecure

  def run(argv=ARGV)
    require 'td/version'
    require 'td/compat_core'
    require 'optparse'

    $prog = @prog_name || File.basename($0)

    op = OptionParser.new
    op.version = TOOLBELT_VERSION
    op.banner = <<EOF
usage: #{$prog} [options] COMMAND [args]

options:
EOF

    op.summary_indent = "  "

    (class << self;self;end).module_eval do
      define_method(:usage) do |errmsg|
        require 'td/command/list'
        $stdout.puts op.to_s
        $stdout.puts ""
        $stdout.puts <<EOF
Basic commands:

  db             # create/delete/list databases
  table          # create/delete/list/import/export/tail tables
  query          # issue a query
  job            # show/kill/list jobs
  import         # manage bulk import sessions (Java based fast processing)
  bulk_import    # manage bulk import sessions (Old Ruby-based implementation)
  result         # create/delete/list result URLs
  sched          # create/delete/list schedules that run a query periodically
  schema         # create/delete/modify schemas of tables
  connector      # manage connectors
  workflow       # manage workflows

Additional commands:

  status         # show scheds, jobs, tables and results
  apikey         # show/set API key
  server         # show status of the Treasure Data server
  sample         # create a sample log file
  help           # show help messages

Type 'td help COMMAND' for more information on a specific command.
EOF
        if errmsg
          $stdout.puts "Error: #{errmsg}"
          return 1
        else
          return 0
        end
      end
    end

    # there local vars are loaded with the values of the options below
    # here they are preloaded with the defaults
    config_path = @config_path
    apikey = @apikey
    endpoint = @endpoint
    insecure = nil
    $verbose = false
    #$debug = false
    retry_post_requests = false

    op.on('-c', '--config PATH', "path to the configuration file (default: ~/.td/td.conf)") {|s|
      config_path = s
    }

    op.on('-k', '--apikey KEY', "use this API key instead of reading the config file") {|s|
      apikey = s
    }

    op.on('-e', '--endpoint API_SERVER', "specify the URL for API server to use (default: https://api.treasuredata.com)." ,
                                         "  The URL must contain a scheme (http:// or https:// prefix) to be valid.",
                                         "  Valid IPv4 addresses are accepted as well in place of the host name.") {|e|
      require 'td/command/common'
      Command.validate_api_endpoint(e)
      endpoint = e
    }

    op.on('--insecure', "Insecure access: disable SSL (enabled by default)") {|b|
      insecure = true
    }

    op.on('-v', '--verbose', "verbose mode", TrueClass) {|b|
      $verbose = b
    }

    #op.on('-d', '--debug', "debug mode", TrueClass) {|b|
    #	$debug = b
    #}

    op.on('-h', '--help', "show help") {
      return usage nil
    }

    op.on('-r', '--retry-post-requests', "retry on failed post requests.",
                                         "Warning: can cause resource duplication, such as duplicated job submissions.",
                                         TrueClass) {|b|
      retry_post_requests = b
    }

    op.on('--version', "show version") {
      $stdout.puts op.version
      return 0
    }

    begin
      op.order!(argv)
      return usage nil if argv.empty?
      cmd = argv.shift

      # NOTE: these information are loaded from by each command through
      #       'TreasureData::Command::get_client' from 'lib/td/command/common.rb'
      require 'td/config'
      if config_path
        Config.path = config_path
      end
      if apikey
        Config.apikey = apikey
        Config.cl_apikey = true
      end
      if endpoint
        Config.endpoint = endpoint
        Config.cl_endpoint = true
      end
      if insecure
        Config.secure = false
      end
      if retry_post_requests
        Config.retry_post_requests = true
      end
    rescue
      return usage $!.to_s
    end

    require 'td/command/list'
    if defined?(Encoding)
      #Encoding.default_internal = 'UTF-8' if Encoding.respond_to?(:default_internal)
      Encoding.default_external = 'UTF-8' if Encoding.respond_to?(:default_external)
    end

    method, cmd_req_connectivity = Command::List.get_method(cmd)
    unless method
      $stderr.puts "'#{cmd}' is not a td command. Run '#{$prog}' to show the list."
      Command::List.show_guess(cmd)
      return 1
    end

    status = nil
    begin
      # test the connectivity with the API endpoint
      if cmd_req_connectivity && Config.cl_endpoint
        Command.test_api_endpoint(Config.endpoint)
      end
      status = method.call(argv)
    rescue ConfigError
      $stderr.puts "TreasureData account is not configured yet."
      $stderr.puts "Run '#{$prog} account' first."
    rescue => e
      # known exceptions are rendered as simple error messages unless the
      # TD_TOOLBELT_DEBUG variable is set or the -v / --verbose option is used.
      # List of known exceptions:
      #   => ParameterConfigurationError
      #   => BulkImportExecutionError
      #   => UpUpdateError
      #   => ImportError
      require 'td/client/api'
      #   => APIError
      #   => ForbiddenError
      #   => NotFoundError
      #   => AuthError
      if ![ParameterConfigurationError, BulkImportExecutionError, UpdateError, ImportError,
            APIError, ForbiddenError, NotFoundError, AuthError, AlreadyExistsError, WorkflowError].include?(e.class) ||
         !ENV['TD_TOOLBELT_DEBUG'].nil? || $verbose
        show_backtrace "Error #{$!.class}: backtrace:", $!.backtrace
      end

      if $!.respond_to?(:api_backtrace) && $!.api_backtrace
        show_backtrace "Error backtrace from server:", $!.api_backtrace.split("\n")
      end

      $stdout.print "Error: "
      if [ForbiddenError, NotFoundError, AuthError].include?(e.class)
        $stdout.print "#{e.class} - "
      end
      $stdout.puts $!.to_s

      require 'socket'
      if e.is_a?(::SocketError)
        $stderr.puts <<EOS

Network dependent error occurred.
If you want to use td command through a proxy,
please set HTTP_PROXY environment variable (e.g. export HTTP_PROXY="host:port")
EOS
      end
      return 1
    end
    return (status.is_a? Integer) ? status : 0
  end

  private

  def show_backtrace(message, backtrace)
    $stderr.puts message
    backtrace.each {|bt|
      $stderr.puts "  #{bt}"
    }
    $stdout.puts ""
  end
end # class Runner

end # module Command
end # module TreasureData
