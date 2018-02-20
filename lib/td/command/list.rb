module TreasureData
module Command
module List

  class CommandParser < OptionParser
    def initialize(name, req_args, opt_args, varlen, argv, req_conn)
      super()
      @name = name
      @req_args = req_args
      @opt_args = opt_args
      @varlen = varlen
      @argv = argv
      @has_options = false
      @message = ''
      @cmd_requires_connectivity = req_conn
    end

    attr_accessor :message, :name, :cmd_requires_connectivity, :argv

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
      $stdout.puts self.to_s
      $stdout.puts "Error: #{msg}" if msg
      exit 1
    end
  end

  class CommandOption
    def initialize(name, args, description, examples = [], req_conn = true)
      @name = name
      @args = args
      @description = description.to_s
      @examples = examples
      @req_conn = req_conn
      @override_message = nil
    end

    attr_reader :name, :args, :description, :examples, :req_conn
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
      op = CommandParser.new(@name, @req_args, @opt_args, @varlen, argv, @req_conn)

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
        (class << op;self;end).module_eval do
          define_method(:to_s) { msg }
        end
      end

      op
    end

    def usage
      compile!
      description_lines = @description.split("\n")
      strout = "%-40s   # %s" % [@usage_args, description_lines[0]]
      if description_lines.length > 1
        description_lines[1..-1].each {|lines|
          strout += "\n"
          strout += "%-40s     # %s" % ["", lines]
        }
      end
      strout
    end

    def group
      @name.split(':', 2).first
    end
  end

  LIST = []
  COMMAND = {}
  GUESS = {}
  HELP_EXCLUDE = [/^help/, /^account/, /^update/, /^user/]
  USAGE_EXCLUDE = [/bulk_import:upload_part\z/, /bulk_import:delete_part\z/]

  def self.add_list(name, args, description, examples = [], cmd_req_conn = true)
    LIST << COMMAND[name] = CommandOption.new(name, args, description, examples, cmd_req_conn)
  end

  def self.add_alias(new_cmd, old_cmd)
    COMMAND[new_cmd] = COMMAND[old_cmd]
  end

  def self.add_guess(wrong, correct)
    GUESS[wrong] = correct
  end

  def self.cmd_usage(name)
    if c = COMMAND[name]
      c.create_optparse([]).to_s
    else
      nil
    end
  end

  def self.get_method(name)
    if c = COMMAND[name]
      name = c.name
      group, action = c.group
      require 'td/command/common'
      require "td/command/#{group}"
      cmd = name.gsub(/[\:\-]/, '_')
      m = Object.new.extend(Command).method(cmd)
      return Proc.new { |args| m.call(c.create_optparse(args)) }, c.req_conn
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
        $stdout.puts ""
      end
      $stdout.puts "#{indent}#{c.usage}"
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
          unless USAGE_EXCLUDE.any? {|pattern| pattern =~ op.name }
            msg << %[  #{op.usage}\n]
          end
        }
        msg << %[\n]
        c.override_message = msg

        COMMAND[group] = c
      end
    }
  end

  add_list 'db:list', %w[], 'Show list of tables in a database', ['db:list', 'dbs']
  add_list 'db:show', %w[db], 'Describe information of a database', ['db example_db']
  add_list 'db:create', %w[db], 'Create a database', ['db:create example_db']
  add_list 'db:delete', %w[db], 'Delete a database', ['db:delete example_db']

  add_list 'export:result', %w[id, name], 'Dump logs of a target query to the specified place', ['export:result target_job_id result']
  add_list 'export:table', %w[db table], 'Dump logs in a table to the specified storage', ['export:table example_db table1 --s3-bucket mybucket -k KEY_ID -s SECRET_KEY']

  add_list 'table:list', %w[db?], 'Show list of tables', ['table:list', 'table:list example_db', 'tables']
  add_list 'table:show', %w[db table], 'Describe information of a table', ['table example_db table1']
  add_list 'table:create', %w[db table], 'Create a table', ['table:create example_db table1']
  add_list 'table:delete', %w[db table], 'Delete a table', ['table:delete example_db table1']
  add_list 'table:import', %w[db table files_], 'Parse and import files to a table', ['table:import example_db table1 --apache access.log', 'table:import example_db table1 --json -t time - < test.json']
  add_list 'table:export', %w[db table], 'Dump logs in a table to the specified storage', ['table:export example_db table1 --s3-bucket mybucket -k KEY_ID -s SECRET_KEY']
  add_list 'table:swap', %w[db table1 table2], 'Swap names of two tables', ['table:swap example_db table1 table2']
  add_list 'table:rename', %w[db from_table dest_table], 'rename exist table', ['table:rename example_db table1 table2']
  add_list 'table:tail', %w[db table], 'Get recently imported logs', ['table:tail example_db table1', 'table:tail example_db table1 -n 30']
  add_list 'table:partial_delete', %w[db table], 'Delete logs from the table within the specified time range', ['table:partial_delete example_db table1 --from 1341000000 --to 1341003600']
  add_list 'table:expire', %w[db table expire_days], 'Expire data in table after specified number of days. Set to 0 to disable the expiration.', ['table:expire example_db table1 30']
  add_list 'table:update', %w[db table], 'Update table options', ['table:update example_db table1 --include-v false']

  add_list 'bulk_import:list', %w[], 'List bulk import sessions', ['bulk_import:list']
  add_list 'bulk_import:show', %w[name], 'Show list of uploaded parts', ['bulk_import:show']
  add_list 'bulk_import:create', %w[name db table], 'Create a new bulk import session to the table', ['bulk_import:create logs_201201 example_db event_logs']
  add_list 'bulk_import:prepare_parts', %w[files_], 'Convert files into part file format', ['bulk_import:prepare_parts logs/*.csv --format csv --columns time,uid,price,count --time-column "time" -o parts/']
  add_list 'bulk_import:upload_part', %w[name id path.msgpack.gz], 'Upload or re-upload a file into a bulk import session', ['bulk_import:upload_part logs_201201 01h data-201201-01.msgpack.gz']
  add_list 'bulk_import:upload_parts', %w[name files_], 'Upload or re-upload files into a bulk import session', ['bulk_import:upload_parts parts/* --parallel 4']
  add_list 'bulk_import:delete_part', %w[name id], 'Delete a uploaded file from a bulk import session', ['bulk_import:delete_part logs_201201 01h']
  add_list 'bulk_import:delete_parts', %w[name ids_], 'Delete uploaded files from a bulk import session', ['bulk_import:delete_parts logs_201201 01h 02h 03h']
  add_list 'bulk_import:perform', %w[name], 'Start to validate and convert uploaded files', ['bulk_import:perform logs_201201']
  add_list 'bulk_import:error_records', %w[name], 'Show records which did not pass validations', ['bulk_import:error_records logs_201201']
  add_list 'bulk_import:commit', %w[name], 'Start to commit a performed bulk import session', ['bulk_import:commit logs_201201']
  add_list 'bulk_import:delete', %w[name], 'Delete a bulk import session', ['bulk_import:delete logs_201201']
  add_list 'bulk_import:freeze', %w[name], 'Reject succeeding uploadings to a bulk import session', ['bulk_import:freeze logs_201201']
  add_list 'bulk_import:unfreeze', %w[name], 'Unfreeze a frozen bulk import session', ['bulk_import:unfreeze logs_201201']

  add_list 'import:list', %w[], 'List bulk import sessions', ['import:list']
  add_list 'import:show', %w[name], 'Show list of uploaded parts', ['import:show']
  add_list 'import:create', %w[name db table], 'Create a new bulk import session to the table', ['import:create logs_201201 example_db event_logs']
  add_list 'import:jar_version', %w[], 'Show import jar version', ['import:jar_version'], false
  add_list 'import:jar_update', %w[], 'Update import jar to the latest version', ['import:jar_update']
  add_list 'import:prepare', %w[files_], 'Convert files into part file format', ['import:prepare logs/*.csv --format csv --columns time,uid,price,count --time-column "time" -o parts/'], false
  add_list 'import:upload', %w[name files_], 'Upload or re-upload files into a bulk import session', ['import:upload parts/* --parallel 4']
  add_list 'import:auto', %w[name files_], 'Upload files and automatically perform and commit the data', ['import:auto parts/* --parallel 4']
  add_list 'import:perform', %w[name], 'Start to validate and convert uploaded files', ['import:perform logs_201201']
  add_list 'import:error_records', %w[name], 'Show records which did not pass validations', ['import:error_records logs_201201']
  add_list 'import:commit', %w[name], 'Start to commit a performed bulk import session', ['import:commit logs_201201']
  add_list 'import:delete', %w[name], 'Delete a bulk import session', ['import:delete logs_201201']
  add_list 'import:freeze', %w[name], 'Reject succeeding uploadings to a bulk import session', ['import:freeze logs_201201']
  add_list 'import:unfreeze', %w[name], 'Unfreeze a frozen bulk import session', ['import:unfreeze logs_201201']
  add_list 'import:config', %w[files_], 'create guess config from arguments', ['import:config "s3://<s3_access_key>:<s3_secret_key>@/my_bucket/path/to/*.csv" -o seed.yml']

  add_list 'result:list', %w[], 'Show list of result URLs', ['result:list', 'results']
  add_list 'result:show', %w[name], 'Describe information of a result URL', ['result name']
  add_list 'result:create', %w[name URL], 'Create a result URL', ['result:create name mysql://my-server/mydb']
  add_list 'result:delete', %w[name], 'Delete a result URL', ['result:delete name']

  add_list 'status', %w[], 'Show schedules, jobs, tables and results', ['status', 's']

  add_list 'schema:show', %w[db table], 'Show schema of a table', ['schema example_db table1']
  add_list 'schema:set', %w[db table columns_?], 'Set new schema on a table', ['schema:set example_db table1 user:string size:int']
  add_list 'schema:add', %w[db table columns_], 'Add new columns to a table', ['schema:add example_db table1 user:string size:int']
  add_list 'schema:remove', %w[db table columns_], 'Remove columns from a table', ['schema:remove example_db table1 user size']

  add_list 'sched:list', %w[], 'Show list of schedules', ['sched:list', 'scheds']
  add_list 'sched:create', %w[name cron sql?], 'Create a schedule', ['sched:create sched1 "0 * * * *" -d example_db "select count(*) from table1" -r rset1',
                                                                    'sched:create sched1 "0 * * * *" -d example_db -q query.txt -r rset2']
  add_list 'sched:delete', %w[name], 'Delete a schedule', ['sched:delete sched1']
  add_list 'sched:update', %w[name], 'Modify a schedule', ['sched:update sched1 -s "0 */2 * * *" -d my_db -t "Asia/Tokyo" -D 3600']
  add_list 'sched:history', %w[name max?], 'Show history of scheduled queries', ['sched sched1 --page 1']
  add_list 'sched:run', %w[name time], 'Run scheduled queries for the specified time', ['sched:run sched1 "2013-01-01 00:00:00" -n 6']
  add_list 'sched:result', %w[name], "Show status and result of the last job ran.\n" +
                                 "--last [N] option enables to show the result before N from the last.\n" +
                                 "The other options are identical to those of the 'job:show' command.",
                                 ['sched:result NAME | sched:result NAME --last | sched:result NAME --last 3']

  add_list 'query', %w[sql?], 'Issue a query', ['query -d example_db -w -r rset1 "select count(*) from table1"',
                                               'query -d example_db -w -r rset1 -q query.txt']

  add_list 'job:show', %w[job_id], 'Show status and result of a job', ['job:show 1461']
  add_list 'job:status', %w[job_id], 'Show status progress of a job', ['job:status 1461']
  add_list 'job:list', %w[max?], 'Show list of jobs', ['jobs', 'jobs --page 1']
  add_list 'job:kill', %w[job_id], 'Kill or cancel a job', ['job:kill 1461']

  add_list 'account', %w[user_name?], 'Setup a Treasure Data account'
  add_list 'account:usage', %w[user_name?], 'Show resource usage information'
  add_list 'password:change', %w[], 'Change password'
  add_list 'apikey:show', %w[], 'Show Treasure Data API key', [], false
  add_list 'apikey:set', %w[apikey], 'Set Treasure Data API key', [], false

  add_list 'user:list', %w[], 'Show list of users'
  add_list 'user:show', %w[name], 'Show an user'
  add_list 'user:create', %w[name], 'Create an user'
  add_list 'user:delete', %w[name], 'Delete an user'
  add_list 'user:apikey:list', %w[name], 'Show API keys of an user'
  add_list 'user:apikey:add', %w[name], 'Add an API key to an user'
  add_list 'user:apikey:remove', %w[name apikey], 'Remove an API key from an user'
  add_list 'user:password:change', %w[name], 'Change password for any user (must be an Administrator)'

  add_list 'server:status', %w[], 'Show status of the Treasure Data server'
  add_list 'server:endpoint', %w[api_endpoint], "Set the Treasure Data API server's endpoint (must be a valid URI)", ["server:endpoint 'https://api.treasuredata.com'"]

  add_list 'sample:apache', %w[path.json], 'Create a sample log file', [], false

  add_list 'help:all', %w[], 'Show usage of all commands', [], false
  add_list 'help', %w[command], 'Show usage of a command', [], false

  add_list 'update', %w[], 'Update td and related libraries for TreasureData toolbelt'

  connector_guess_example_config = "
  in:
    type: s3
    bucket: my-s3-bucket
    endpoint: s3-us-west-1.amazonaws.com
    path_prefix: path/prefix/to/import/
    access_key_id: ABCXYZ123ABCXYZ123
    secret_access_key: AbCxYz123aBcXyZ123
  out:
    mode: append"
  add_list 'connector:guess', %w[config?], 'Run guess to generate connector config file', ["connector:guess config.yml -o td-bulkload.yml\n\nexample config.yml:#{connector_guess_example_config}"]
  add_list 'connector:preview', %w[config], 'Show preview of connector execution', ['connector:preview td-bulkload.yml']

  add_list 'connector:issue', %w[config], 'Run one time connector execution', ['connector:issue td-bulkload.yml']

  add_list 'connector:list', %w[], 'Show list of connector sessions', ['connector:list']
  add_list 'connector:create', %w[name cron database table config], 'Create new connector session', ['connector:create connector1 "0 * * * *" connector_database connector_table td-bulkload.yml']
  add_list 'connector:show', %w[name], 'Show connector session', ['connector:show connector1']
  add_list 'connector:update', %w[name config?], 'Modify connector session', ['connector:update connector1 -c td-bulkload.yml -s \'@daily\' ...']
  add_list 'connector:delete', %w[name], 'Delete connector session', ['connector:delete connector1']
  add_list 'connector:history', %w[name], 'Show job history of connector session', ['connector:history connector1']
  add_list 'connector:run', %w[name time?], 'Run connector with session for the specified time option', ['connector:run connector1 "2016-01-01 00:00:00"']

  add_list 'workflow', %w[], 'Run a workflow command'
  add_list 'workflow:reset', %w[], 'Reset the workflow module'
  add_list 'workflow:update', %w[version?], 'Update the workflow module'
  add_list 'workflow:version', %w[], 'Show workflow module version'

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

  add_alias 'result', 'result:show'
  add_alias 'results', 'result:list'

  add_alias 'schema', 'schema:show'

  add_alias 'schedule:list', 'sched:list'
  add_alias 'schedule:create', 'sched:create'
  add_alias 'schedule:delete', 'sched:delete'
  add_alias 'schedule:history', 'sched:history'
  add_alias 'schedule:result', 'sched:result'
  add_alias 'schedule:hist', 'sched:history'
  add_alias 'sched:hist', 'sched:history'
  add_alias 'sched', 'sched:history'
  add_alias 'scheds', 'sched:list'
  add_alias 'schedules', 'sched:list'

  add_alias 'import',              'import:show'
  add_alias 'imports',             'import:list'
  add_alias 'import:java_version', 'import:jar_version'


  add_alias 'bulk_import', 'bulk_import:show'
  add_alias 'bulk_imports', 'bulk_import:list'

  add_alias 'import', 'import:show'

  add_alias 'job', 'job:show'
  add_alias 'jobs', 'job:list'
  add_alias 'kill', 'job:kill'

  add_alias 'user', 'user:show'
  add_alias 'users', 'user:list'
  add_alias 'user:apikey', 'user:apikey:list'
  add_alias 'user:apikeys', 'user:apikey:list'

  add_alias 'apikey', 'apikey:show'
  add_alias 'server', 'server:status'
  add_alias 'sample', 'sample:apache'

  add_alias 's', 'status'

  add_alias 'connector', 'connector:guess'

  add_alias 'wf', 'workflow'

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

  finishup

end # module List
end # module Command
end # module TreasureData
