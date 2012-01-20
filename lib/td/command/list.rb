
module TreasureData
module Command
module List

  class CommandParser < OptionParser
    def initialize(name, req_args, opt_args, varlen, argv)
      super()
      @req_args = req_args
      @opt_args = opt_args
      @varlen = varlen
      @argv = argv
      @has_options = false
      @message = ''
    end

    attr_accessor :message

    def on(*argv)
      @has_options = true
      super
    end

    def banner
      s = @message.dup
      if @has_options
        s << "\n"
        s << "options:\n"
      end
      s
    end

    def cmd_parse(argv=@argv||ARGV)
      parse!(argv)
      if argv.length < @req_args.length || (!@varlen && argv.length > (@req_args.length+@opt_args.length))
        cmd_usage nil
      end
      if argv.length <= 1
        return argv[0]
      else
        return argv
      end
    rescue
      cmd_usage $!
    end

    def cmd_usage(msg=nil)
      puts self.to_s
      puts "error: #{msg}" if msg
      exit 1
    end
  end

  class CommandOption
    def initialize(name, args, description, examples)
      @name = name
      @args = args
      @description = description.to_s
      @examples = examples
      @override_message = nil
    end

    attr_reader :name, :args, :description, :examples
    attr_accessor :override_message

    def compile!
      return if @usage_args

      args = @args.dup
      if args.last.to_s =~ /_$/
        @varlen = true
        args.push args.pop.to_s[0..-2]+'...'
      elsif args.last.to_s =~ /_\?$/
        @varlen = true
        args.push args.pop.to_s[0..-3]+'...?'
      end

      @req_args, @opt_args = args.partition {|a| a.to_s !~ /\?$/ }
      @opt_args = @opt_args.map {|a| a.to_s[0..-2].to_sym }

      @usage_args = "#{@name}"
      @req_args.each {|a| @usage_args << " <#{a}>" }
      @opt_args.each {|a| @usage_args << " [#{a}]" }
    end

    def create_optparse(argv)
      compile!
      op = CommandParser.new(@name, @req_args, @opt_args, @varlen, argv)

      message = "usage:\n"
      message << "  $ #{File.basename($0)} #{@usage_args}\n"
      unless @examples.empty?
        message << "\n"
        message << "example:\n"
        @examples.each {|l|
          message << "  $ #{File.basename($0)} #{l}\n"
        }
      end
      message << "\n"
      message << "description:\n"
      @description.split("\n").each {|l|
        message << "  #{l}\n"
      }

      op.message = message
      op.summary_indent = "  "

      if msg = @override_message
        (class<<op;self;end).module_eval do
          define_method(:to_s) { msg }
        end
      end

      op
    end

    def usage
      compile!
      "%-40s   # %s" % [@usage_args, @description]
    end

    def group
      @name.split(':', 2).first
    end
  end

  LIST = []
  COMMAND = {}
  GUESS = {}
  HELP_EXCLUDE = [/^help/, /^account/, /^aggr/]

  def self.add_list(name, args, description, *examples)
    LIST << COMMAND[name] = CommandOption.new(name, args, description, examples)
  end

  def self.add_alias(new_cmd, old_cmd)
    COMMAND[new_cmd] = COMMAND[old_cmd]
  end

  def self.add_guess(wrong, correct)
    GUESS[wrong] = correct
  end

  def self.cmd_usage(name)
    if c = COMMAND[name]
      c.create_optparse([]).cmd_usage
    end
    nil
  end

  def self.get_method(name)
    if c = COMMAND[name]
      name = c.name
      group, action = c.group
      require 'td/command/common'
      require "td/command/#{group}"
      cmd = name.gsub(/[\:\-]/, '_')
      m = Object.new.extend(Command).method(cmd)
      return Proc.new {|args| m.call(c.create_optparse(args)) }
    end
    nil
  end

  def self.show_guess(wrong)
    if correct = GUESS[wrong]
      $stderr.puts "Did you mean this?: #{correct}"
    end
  end

  def self.get_option(name)
    COMMAND[name]
  end

  def self.show_help(indent='  ')
    before_group = nil
    LIST.each {|c|
      next if HELP_EXCLUDE.any? {|pattern| pattern =~ c.name }
      if before_group != c.group
        before_group = c.group
        puts ""
      end
      puts "#{indent}#{c.usage}"
    }
  end

  def self.get_group(group)
    LIST.map {|c|
      c.group == group
    }
  end

  def self.finishup
    groups = {}
    LIST.each {|c|
      (groups[c.group] ||= []) << c
    }
    groups.each_pair {|group,ops|
      if ops.size > 1 && c = COMMAND[group]
        c = c.dup

        msg = %[Additional commands, type "#{File.basename($0)} help COMMAND" for more details:\n\n]
        ops.each {|op|
          msg << %[  #{op.usage}\n]
        }
        msg << %[\n]
        c.override_message = msg

        COMMAND[group] = c
      end
    }
  end

  add_list 'db:list', %w[], 'Show list of tables in a database', 'db:list', 'dbs'
  add_list 'db:show', %w[db], 'Describe a information of a database', 'db example_db'
  add_list 'db:create', %w[db], 'Create a database', 'db:create example_db'
  add_list 'db:delete', %w[db], 'Delete a database', 'db:delete example_db'

  add_list 'table:list', %w[db?], 'Show list of tables', 'table:list', 'table:list example_db', 'tables'
  add_list 'table:show', %w[db table], 'Describe a information of a table', 'table example_db table1'
  add_list 'table:create', %w[db table], 'Create a table', 'table:create example_db table1'
  add_list 'table:delete', %w[db table], 'Delete a table', 'table:delete example_db table1'
  add_list 'table:import', %w[db table files_], 'Parse and import files to a table', 'table:import example_db table1 --apache access.log', 'table:import example_db table1 --json -t time - < test.json'
  add_list 'table:tail', %w[db table], 'Get recently imported logs', 'table:tail example_db table1', 'table:tail example_db table1 -t "2011-01-02 03:04:05" -n 30'

  add_list 'result:info', %w[], 'Show information of the MySQL server', 'result:info'
  add_list 'result:list', %w[], 'Show list of result tables', 'result:list', 'results'
  add_list 'result:create', %w[name], 'Create a result table', 'result:create rset1'
  add_list 'result:delete', %w[name], 'Delete a result table', 'result:delete rset1'
  add_list 'result:connect', %w[sql?], 'Connect to the server using mysql command', 'result:connect'
  #add_list 'result:get', %w[name], 'Download dump of the result table'

  add_list 'status', %w[], 'Show schedules, jobs, tables and results', 'status', 's'

  add_list 'schema:show', %w[db table], 'Show schema of a table', 'schema example_db table1'
  add_list 'schema:set', %w[db table columns_?], 'Set new schema on a table', 'schema:set example_db table1 user:string size:int'
  add_list 'schema:add', %w[db table columns_], 'Add new columns to a table', 'schema:add example_db table1 user:string size:int'
  add_list 'schema:remove', %w[db table columns_], 'Remove columns from a table', 'schema:remove example_db table1 user size'

  add_list 'sched:list', %w[], 'Show list of schedules', 'sched:list', 'scheds'
  add_list 'sched:create', %w[name cron sql], 'Create a schedule', 'sched:create sched1 "0 * * * *" -d example_db "select count(*) from table1" -r rset1'
  add_list 'sched:delete', %w[name], 'Delete a schedule', 'sched:delete sched1'
  add_list 'sched:history', %w[name max?], 'Show history of scheduled queries', 'sched sched1 --page 1'

  add_list 'query', %w[sql], 'Issue a query', 'query -d example_db -w -r rset1 "select count(*) from table1"'

  add_list 'job:show', %w[job_id], 'Show status and result of a job', 'job 1461'
  add_list 'job:list', %w[max?], 'Show list of jobs', 'jobs', 'jobs --page 1'
  add_list 'job:kill', %w[job_id], 'Kill or cancel a job', 'job:kill 1461'

  add_list 'account', %w[user_name?], 'Setup a Treasure Data account'
  add_list 'apikey:show', %w[], 'Show Treasure Data API key'
  add_list 'apikey:set', %w[apikey], 'Set Treasure Data API key'

  add_list 'aggr:list', %w[], 'Show list of aggregation schemas'
  add_list 'aggr:show', %w[name], 'Describe a aggregation schema'
  add_list 'aggr:create', %w[name relation_key], 'Create a aggregation schema'
  add_list 'aggr:delete', %w[name], 'Delete a aggregation schema'
  add_list 'aggr:add-log', %w[name db table entry_name o1_key? o2_key? o3_key?], 'Add a log aggregation entry'
  add_list 'aggr:add-attr', %w[name db table entry_name method_name parameters_?], 'Add an attribute aggregation entry'
  add_list 'aggr:del-log', %w[name entry_name], 'Delete a log aggregation entry'
  add_list 'aggr:del-attr', %w[name entry_name], 'Delete an attribute aggregation entry'

  add_list 'server:status', %w[], 'Show status of the Treasure Data server'

  add_list 'help:all', %w[], 'Show usage of all commands'
  add_list 'help', %w[command], 'Show usage of a command'

  # aliases
  add_alias 'db', 'db:show'
  add_alias 'dbs', 'db:list'

  add_alias 'database:show', 'db:show'
  add_alias 'database:list', 'db:list'
  add_alias 'database:create', 'db:create'
  add_alias 'database:delete', 'db:delete'
  add_alias 'database', 'db:show'
  add_alias 'databases', 'db:list'

  add_alias 'table', 'table:show'
  add_alias 'tables', 'table:list'

  add_alias 'result', 'help'  # dummy
  add_alias 'results', 'result:list'

  add_alias 'schema', 'schema:show'

  add_alias 'schedule:list', 'sched:list'
  add_alias 'schedule:create', 'sched:create'
  add_alias 'schedule:delete', 'sched:delete'
  add_alias 'schedule:history', 'sched:history'
  add_alias 'schedule:hist', 'sched:history'
  add_alias 'sched:hist', 'sched:history'
  add_alias 'sched', 'sched:history'
  add_alias 'scheds', 'sched:list'
  add_alias 'schedules', 'sched:list'

  add_alias 'job', 'job:show'
  add_alias 'jobs', 'job:list'
  add_alias 'kill', 'job:kill'

  add_alias 'aggr', 'aggr:show'
  add_alias 'aggrs', 'aggr:list'

  add_alias 'apikey', 'apikey:show'
  add_alias 'server', 'server:status'

  add_alias 's', 'status'

  # backward compatibility
  add_alias 'show-databases',   'db:list'
  add_alias 'show-dbs',         'db:list'
  add_alias 'create-database',  'db:create'
  add_alias 'create-db',        'db:create'
  add_alias 'drop-database',    'db:delete'
  add_alias 'drop-db',          'db:delete'
  add_alias 'delete-database',  'db:delete'
  add_alias 'delete-db',        'db:delete'
  add_alias 'show-tables',      'table:list'
  add_alias 'show-table',       'table:show'
  add_alias 'create-log-table', 'table:create'
  add_alias 'create-table',     'table:create'
  add_alias 'drop-log-table',   'table:delete'
  add_alias 'drop-table',       'table:delete'
  add_alias 'delete-log-table', 'table:delete'
  add_alias 'delete-table',     'table:delete'
  add_guess 'show-job',         'job:show'
  add_guess 'show-jobs',        'job:list'
  add_guess 'server-status',    'server:status'
  add_alias 'import',           'table:import'

  finishup
end
end
end

