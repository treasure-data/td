
module TRD
module Command

	def self.define_command(name, &block)
		define_method(name) {|argv|
			op = OptionParser.new
			op.banner = "usage: #{$prog} #{name}"
			block.call(argv,op)
		}
	end

	module List
		LIST = []
		FILES = {}

		def self.add(name, msg, fname, common=false)
			LIST << [name, msg, common]
			FILES[name] = fname
		end

		add 'account',         'Set your account',       'config', true
		add 'create-database', 'Create a database',      'api', true
		add 'drop-database',   'Delete a database',      'api', true
		add 'create-table',    'Create a table',         'api', true
		add 'drop-table',      'Delete a table',         'api', true
		add 'show-databases',  'Show list of databases', 'api', true
		add 'show-tables',     'Show list of tables',    'api', true
		add 'import',          'Import file to a table', 'import', true  # TODO
		add 'query',           'Execute a query',        'query', true   # TODO
		add 'help',            'Show usage of a command', 'help', false

		def self.common_help(indent='')
			LIST.map {|name,msg,common|
				format_help(name, msg, indent) if common
			}.compact.join("\n")
		end

		def self.all_help(indent='')
			LIST.map {|name,msg,common|
				format_help(name, msg, indent) if common
			}.join("\n")
		end

		def self.call(cmd)
			fname = FILES[cmd]
			mname = cmd.gsub(/[^a-zA-Z0-9]+/,'_')
			begin
				require "trd/command/#{fname}"
				m = TRD::Command.method(mname)
			rescue LoadError, NameError
				raise "'#{cmd}' is not a trd command. See '#{$prog} --help'"
			end
			m.call
		end

		private
		def self.format_help(name, msg, indent='')
		"#{indent}%-17s %s" % [name, msg]
		end
	end

end
end

