
module TRD
module Command
module List

	LIST = []
	ALIASES = {}

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

	add_list 'list', 'help', 'Show usage of a command'
	add_list 'account', 'account', 'Setup an account'
	add_list 'database', 'show-databases', 'Show list of databases'
	add_list 'table', 'show-tables', 'Show list of tables'
	add_list 'query', 'show-jobs', 'Show list of jobs'
	add_list 'database', 'create-database', 'Create a database'
	add_list 'table', 'create-log-table', 'Create a log table'
	add_list 'table', 'create-item-table', 'Create a item table'
	add_list 'database', 'drop-database', 'Delete a database'
	add_list 'table', 'drop-log-table', 'Delete a log table'
	add_list 'table', 'drop-item-table', 'Delete a item table'
	add_list 'table', 'drop-table', 'Delete a table'
	add_list 'query', 'query', 'Start a query'
	add_list 'query', 'job', 'Show status of a job'

	add_alias 'show-dbs', 'show-databases'
	add_alias 'create-db', 'create-databases'
	add_alias 'drop-db', 'create-databases'

	def self.get_method(command)
		command = ALIASES[command] || command
		LIST.each {|cmd,file,description|
			if cmd == command
				require 'trd/command/common'
				require "trd/command/#{file}"
				name = command.gsub('-','_')
				return Object.new.extend(Command).method(name)
			end
		}
		nil
	end

	def self.help(indent)
		LIST.map {|cmd,file,description|
			"#{indent}%-18s %s" % [cmd, description.split("\n").first]
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
		$stderr.puts "'#{cmd}' is not a trd command. Run '#{$prog}' to show the list."
		exit 1
	end

	method.call
end

end
end

