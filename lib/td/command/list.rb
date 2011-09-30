
module TreasureData
module Command
module List

  class CommandOption < OptionParser
    def initialize(name, args, description)
      super()

      @name = name
      @description = description.to_s

      if args.last.to_s =~ /_$/
        @varlen = true
        args.push args.pop.to_s[0..-2]+'...'
      elsif args.last.to_s =~ /_\?$/
        @varlen = true
        args.push args.pop.to_s[0..-3]+'...?'
      end

      @req_args, opt_args = args.partition {|a| a.to_s !~ /\?$/ }
      @opt_args = opt_args.map {|a| a.to_s[0..-2].to_sym }
      @args = @req_args + @opt_args

      @usage_args = "#{@name}"
      @req_args.each {|a| @usage_args << " <#{a}>" }
      @opt_args.each {|a| @usage_args << " [#{a}]" }

      @has_options = false

      self.summary_indent = "  "

      banner  = "usage:\n"
      banner << "  $ #{File.basename($0)} #{@usage_args}\n"
      banner << "\n"
      banner << "description:\n"
      @description.split("\n").each {|l|
        banner << "  #{l}\n"
      }
      self.banner = banner

      @message = nil
      @argv = nil
    end

    attr_accessor :message, :argv

    def message
      @message || to_s
    end

    def banner
      s = super.dup
      if @has_options
        s << "\n"
        s << "options:\n"
      end
      s << "\n"
      s
    end

    def usage
      "%-40s   # %s" % [@usage_args, @description]
    end

    def cmd_parse(argv=@argv||ARGV)
      parse!(argv)
      if argv.length < @req_args.length || (!@varlen && argv.length > @argv.length)
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
      puts self.message
      if msg
        puts ""
        puts "error: #{msg}"
      end
      exit 1
    end

    def group
      name.split(':', 2).first
    end

    def on(*argv)
      @has_options = true
      super
    end

    def with_args(argv)
      d = dup
      d.argv = argv
      d
    end

    attr_reader :name
    attr_reader :description
  end

  LIST = []
  COMMAND = {}
  GUESS = {}
  HELP_EXCLUDE = ['help', 'account']

  def self.add_list(name, args, description)
    LIST << COMMAND[name] = CommandOption.new(name, args, description)
  end

  def self.add_alias(new_cmd, old_cmd)
    COMMAND[new_cmd] = COMMAND[old_cmd]
  end

  def self.add_guess(wrong, correct)
    GUESS[wrong] = correct
  end

  def self.get_method(name)
    if op = COMMAND[name]
      name = op.name
      group, action = op.group
      require 'td/command/common'
      require "td/command/#{group}"
      cmd = name.gsub(':', '_')
      m = Object.new.extend(Command).method(cmd)
      return Proc.new {|args| m.call(op.with_args(args)) }
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
    LIST.each {|op|
      next if HELP_EXCLUDE.include?(op.name)
      if before_group != op.group
        before_group = op.group
        puts ""
      end
      puts "#{indent}#{op.usage}"
    }
  end

  def self.get_group(group)
    LIST.map {|op|
      op.group == group
    }
  end

  def self.finishup
    groups = {}
    LIST.each {|op|
      (groups[op.group] ||= []) << op
    }
    groups.each_pair {|group,ops|
      if ops.size > 1 && xop = COMMAND[group].dup
        msg = %[Additional commands, type "#{File.basename($0)} help COMMAND" for more details:\n\n]
        ops.each {|op|
          msg << %[  #{op.usage}\n]
        }
        msg << %[\n]
        xop.message = msg
        COMMAND[group] = xop
      end
    }
  end

  add_list 'db:list', %w[], 'Show list of tables in a database'
  add_list 'db:show', %w[db], 'Describe a information of a database'
  add_list 'db:create', %w[db], 'Create a database'
  add_list 'db:delete', %w[db], 'Delete a database'

  add_list 'table:list', %w[db?], 'Show list of tables'
  add_list 'table:show', %w[db table], 'Describe a information of a table'
  add_list 'table:create', %w[db table], 'Create a table'
  add_list 'table:delete', %w[db table], 'Delete a table'
  add_list 'table:import', %w[db table files_], 'Parse and import files to a table'
  #add_list 'table:tail', %w[db table], 'Get recently imported logs'

  add_list 'schema:show', %w[db table], 'Show schema of a table'
  add_list 'schema:set', %w[db table columns_?], 'Set new schema on a table'
  add_list 'schema:add', %w[db table columns_], 'Add new columns to a table'
  add_list 'schema:remove', %w[db table columns_], 'Remove columns from a table'

  add_list 'sched:list', %w[], 'Show list of schedules'
  add_list 'sched:create', %w[name cron sql], 'Create a schedule'
  add_list 'sched:delete', %w[name], 'Delete a schedule'
  add_list 'sched:history', %w[name max?], 'Show history of scheduled queries'

  add_list 'query', %w[sql], 'Issue a query'

  add_list 'job:show', %w[job_id], 'Show status and result of a job'
  add_list 'job:list', %w[max?], 'Show list of jobs'
  add_list 'job:kill', %w[job_id], 'Kill or cancel a job'

  add_list 'account', %w[user_name?], 'Setup a Treasure Data account'
  add_list 'apikey:show', %w[], 'Show Treasure Data API key'
  add_list 'apikey:set', %w[apikey], 'Set Treasure Data API key'

  add_list 'server:status', %w[], 'Show status of the Treasure Data server'

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

  add_alias 'apikey', 'apikey:show'

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

