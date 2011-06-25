
module TRD
module Command
  private
  def cmd_opt(name, *args)
    if args.last.to_s =~ /_$/
      args.push args.pop.to_s[0..-2]+'...'
      multi = true
    end

    req_args, opt_args = args.partition {|a| a.to_s !~ /\?$/ }
    opt_args = opt_args.map {|a| a.to_s[0..-2].to_sym }
    args = req_args + opt_args

    args_line = req_args.map {|a| "<#{a}>" }
    args_line.concat opt_args.map {|a| "[#{a}]" }
    args_line = args_line.join(' ')

    description = List.get_description(name)

    op = OptionParser.new
    op.summary_indent = "  "
    op.banner = <<EOF
usage: #{$prog} #{name} #{args_line}

description:
#{description.split("\n").map {|l| "  #{l}" }.join("\n")}
EOF

    (class<<op;self;end).module_eval do
      define_method(:cmd_usage) do |msg|
        puts op.to_s
        puts ""
        puts "error: #{msg}" if msg
        exit 1
      end

      define_method(:cmd_parse) do
        begin
          parse!(ARGV)
          if ARGV.length < req_args.length - opt_args.length ||
              (!multi && ARGV.length > args.length)
            cmd_usage nil
          end
          if ARGV.length <= 1
            ARGV[0]
          else
            ARGV
          end
        rescue
          cmd_usage $!
        end
      end

    end

    op
  end

  def cmd_config
    require 'trd/config'
    Config.read($TRD_CONFIG_PATH)
  end

  def cmd_api(conf)
    apikey = conf['account.apikey']
    unless apikey
      raise ConfigError, "Account is not configured."
    end
    require 'trd/api'
    api = API.new(apikey)
  end

  def cmd_render_table(rows, *opts)
    require 'hirb'
    Hirb::Helpers::Table.render(rows, *opts)
  end

  def cmd_render_tree(nodes, *opts)
    require 'hirb'
    Hirb::Helpers::Tree.render(nodes, *opts)
  end

  def cmd_debug_error(ex)
    if $verbose
      $stderr.puts "error: #{$!.class}: #{$!.to_s}"
      $!.backtrace.each {|b|
        $stderr.puts "  #{b}"
      }
        $stderr.puts ""
    end
  end

  def find_database(api, db_name)
    begin
      return api.database(db_name)
    rescue
      cmd_debug_error $!
      $stderr.puts $!
      $stderr.puts "Use '#{$prog} show-databases' to show the list of databases."
      exit 1
    end
    db
  end

  def find_table(api, db_name, table_name, type=nil)
    db = find_database(api, db_name)
    begin
      table = db.table(table_name)
    rescue
      $stderr.puts $!
      $stderr.puts "Use '#{$prog} show-tables #{db_name}' to show the list of tables."
      exit 1
    end
    if type && table.type != type
      $stderr.puts "Table '#{db_name}.#{table_name} is not a #{type} table but a #{table.type} table"
    end
    table
  end
end
end

