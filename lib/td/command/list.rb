
module TreasureData
module Command
module List

  LIST = []
  ALIASES = {}
  GUESS = {}

  def self.add_list(file, cmd, description)
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

  add_list 'list', 'help', 'Show usage of a command'
  add_list 'account', 'account', 'Setup a Treasure Data account'
  add_list 'server', 'server-status', 'Show status of the Treasure Data server'
  add_list 'database', 'show-databases', 'Show list of databases'
  add_list 'table', 'show-tables', 'Show list of tables'
  add_list 'query', 'show-jobs', 'Show list of jobs'
  add_list 'database', 'create-database', 'Create a database'
  add_list 'table', 'create-log-table', 'Create a log table'
  #add_list 'table', 'create-item-table', 'Create a item table'
#  add_list 'table', 'create-schema-table', 'Create a schema table on a table'
  #add_list 'table', 'alter-schema-table', 'Updates a schema table'
  add_list 'database', 'drop-database', 'Delete a database'
  add_list 'table', 'drop-table', 'Delete a table'
  add_list 'query', 'query', 'Start a query'
  add_list 'query', 'job', 'Show status and result of a job'
  add_list 'import', 'import', 'Import files to a table'
  add_list 'list', 'version', 'Show version'

  add_alias 'show-dbs', 'show-databases'
  add_alias 'show-database', 'show-databases'
  add_alias 'create-db', 'create-databases'
  add_alias 'drop-db', 'create-databases'
  add_alias 'show-table', 'show-tables'
  add_alias 'delete-database', 'drop-database'
  add_alias 'delete-table', 'drop-table'
  add_alias 'jobs', 'show-jobs'
#  add_alias 'create-schema', 'create-schema-table'
  #add_alias 'alter-schema', 'alter-schema-table'

  add_guess 'create-table', 'create-log-table'
  add_guess 'drop-log-table', 'drop-table'
  #add_guess 'drop-item-table', 'drop-table'
  add_guess 'delete-log-table', 'drop-table'
  #add_guess 'delete-item-table', 'drop-table'
  add_guess 'show-job', 'job'

  def self.get_method(command)
    command = ALIASES[command] || command
    LIST.each {|cmd,file,description|
      if cmd == command
        require 'td/command/common'
        require "td/command/#{file}"
        name = command.gsub('-','_')
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

