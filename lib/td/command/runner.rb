
module TreasureData
module Command


class Runner
  def initialize
    @config_path = nil
    @apikey = nil
    @prog_name = nil
    @secure = true
  end

  attr_accessor :apikey, :config_path, :prog_name, :secure

  def run(argv=ARGV)
    require 'td/version'
    require 'td/compat_core'
    require 'optparse'

    $prog = @prog_name || File.basename($0)

    op = OptionParser.new
    op.version = TreasureData::VERSION
    op.banner = <<EOF
usage: #{$prog} [options] COMMAND [args]

options:
EOF

    op.summary_indent = "  "

    (class << self;self;end).module_eval do
      define_method(:usage) do |errmsg|
        require 'td/command/list'
        puts op.to_s
        puts ""
        puts <<EOF
Basic commands:

  db             # create/delete/list databases
  table          # create/delete/list/import/export/tail tables
  query          # issue a query
  job            # show/kill/list jobs
  bulk_import    # manage bulk import sessions
  result         # create/delete/list result URLs

Additional commands:

  sched          # create/delete/list schedules that run a query periodically
  schema         # create/delete/modify schemas of tables
  status         # show scheds, jobs, tables and results
  apikey         # show/set API key
  server         # show status of the Treasure Data server
  sample         # create a sample log file
  help           # show help messages

Type 'td help COMMAND' for more information on a specific command.
EOF
        if errmsg
          puts "error: #{errmsg}"
          exit 1
        else
          exit 0
        end
      end
    end

    config_path = @config_path
    apikey = @apikey
    insecure = nil
    $verbose = false
    #$debug = false

    op.on('-c', '--config PATH', "path to config file (~/.td/td.conf)") {|s|
      config_path = s
    }

    op.on('-k', '--apikey KEY', "use this API key instead of reading the config file") {|s|
      apikey = s
    }

    op.on('--insecure', "Insecure access: disable SSL") { |b|
      insecure = true
    }

    op.on('-v', '--verbose', "verbose mode", TrueClass) {|b|
      $verbose = b
    }

    #op.on('-d', '--debug', "debug mode", TrueClass) {|b|
    #	$debug = b
    #}

    op.on('-h', '--help', "show help") {
      usage nil
    }

    begin
      op.order!(argv)
      usage nil if argv.empty?
      cmd = argv.shift

      require 'td/config'
      if config_path
        TreasureData::Config.path = config_path
      end
      if apikey
        TreasureData::Config.apikey = apikey
      end
      if insecure
        TreasureData::Config.secure = false
      end
    rescue
      usage $!.to_s
    end

    require 'td/command/list'
    if defined?(Encoding)
      #Encoding.default_internal = 'UTF-8' if Encoding.respond_to?(:default_internal)
      Encoding.default_external = 'UTF-8' if Encoding.respond_to?(:default_external)
    end

    method = TreasureData::Command::List.get_method(cmd)
    unless method
      $stderr.puts "'#{cmd}' is not a td command. Run '#{$prog}' to show the list."
      TreasureData::Command::List.show_guess(cmd)
      exit 1
    end

    begin
      method.call(argv)
    rescue TreasureData::ConfigError
      $stderr.puts "TreasureData account is not configured yet."
      $stderr.puts "Run '#{$prog} account' first."
    rescue
      $stderr.puts "error #{$!.class}: backtrace:"
      $!.backtrace.each {|b|
        $stderr.puts "  #{b}"
      }
      puts ""
      puts $!
    end
  end
end


end
end

