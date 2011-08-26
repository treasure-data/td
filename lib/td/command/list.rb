
module TreasureData
module Command
module List

  LIST = []
  ALIASES = {}
  GUESS = {}

  def self.add_list(cmd, file, description)
    LIST << [cmd, file, description]
  end

  def self.add_alias(new_cmd, old_cmd)
    ALIASES[new_cmd] = old_cmd
  end

  def self.get_description(command)
    LIST.each {|cmd,file,description|
      if cmd == command
        return description
      end
    }
    nil
  end

  def self.add_guess(wrong, correct)
    GUESS[wrong] = correct
  end


  # commands
  add_list 'database:show',     'database', 'Describe a information of a database'
  add_list 'database:list',     'database', 'Show list of tables in a database'
  add_list 'database:create',   'database', 'Create a database'
  add_list 'database:delete',   'database', 'Delete a database'

  add_list 'table:show',        'table', 'Describe a information of a table'
  add_list 'table:list',        'table', 'Show list of tables'
  add_list 'table:create',      'table', 'Create a table'
  add_list 'table:delete',      'table', 'Delete a table'

  add_list 'schema:show',       'schema', 'Show schema of a table'
  add_list 'schema:set',        'schema', 'Set new schema on a table'
  add_list 'schema:add',        'schema', 'Add new columns to a table'
  add_list 'schema:remove',     'schema', 'Remove columns from a table'

  add_list 'import',            'import', 'Import files to a table'

  add_list 'query',             'query', 'Issue a query'

  add_list 'job:show',          'query', 'Show status and result of a job'
  add_list 'job:list',          'query', 'Show list of jobs'
  #add_list 'job:cancel',        'query', 'Cancel a job'

  add_list 'help',              'list', 'Show usage of a command'
  add_list 'account',           'account', 'Setup a Treasure Data account'

  add_list 'server:status',     'server', 'Show status of the Treasure Data server'

  # aliases
  add_alias 'database', 'database:show'
  add_alias 'databases', 'database:list'

  add_alias 'db:show', 'database:show'
  add_alias 'db:list', 'database:list'
  add_alias 'db:create', 'database:create'
  add_alias 'db:delete', 'database:delete'
  add_alias 'db', 'database:show'
  add_alias 'dbs', 'database:list'

  add_alias 'table', 'table:show'
  add_alias 'tables', 'table:list'

  add_alias 'schema', 'schema:show'

  add_alias 'job', 'job:show'
  add_alias 'jobs', 'job:list'

  # backward compatibility
  add_alias 'show-databases',   'database:list'
  add_alias 'show-dbs',         'database:list'
  add_alias 'create-database',  'database:create'
  add_alias 'create-db',        'database:create'
  add_alias 'drop-database',    'database:delete'
  add_alias 'drop-db',          'database:delete'
  add_alias 'delete-database',  'database:delete'
  add_alias 'delete-db',        'database:delete'
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

  def self.get_method(command)
    command = ALIASES[command] || command
    LIST.each {|cmd,file,description|
      if cmd == command
        require 'td/command/common'
        require "td/command/#{file}"
        name = command.gsub(/[-:]/,'_')
        return Object.new.extend(Command).method(name)
      end
    }
    nil
  end

  def self.show_guess(wrong)
    if correct = GUESS[wrong]
      $stderr.puts "Did you mean this?: #{correct}"
    end
  end

  def self.help(indent)
    LIST.map {|cmd,file,description|
      if cmd != 'help'
        "#{indent}%-18s %s" % [cmd, description.split("\n").first]
      end
    }.join("\n")
  end
end

def help
  op = cmd_opt 'help', :command
  cmd = op.cmd_parse

  ARGV.clear
  ARGV[0] = '--help'

  method = List.get_method(cmd)
  unless method
    $stderr.puts "'#{cmd}' is not a td command. Run '#{$prog}' to show the list."
    List.show_guess(cmd)
    exit 1
  end

  method.call
end

def version
  require 'td/version'
  puts "td-#{TreasureData::VERSION}"
end

end
end

