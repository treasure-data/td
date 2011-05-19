
module TRD
module Command
	private
	def cmd_opt(name, *args)
		req_args, opt_args = args.partition {|a| a.to_s !~ /\?$/ }
		opt_args = opt_args.map {|a| a.to_s[0..-2].to_sym }
		args = req_args + opt_args

		args_line = req_args.map {|a| "<#{a}>" } + opt_args.map {|a| "[#{a}]" }
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
							ARGV.length > args.length
						cmd_usage nil
					end
					#Hash[*args.zip(ARGV).flatten]
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

	def cmd_render_table(rows)
		require 'hirb'
		Hirb::Helpers::Table.render(rows)
	end

	def cmd_render_tree(nodes)
		require 'hirb'
		Hirb::Helpers::Tree.render(nodes)
	end

	def find_database(api, db_name)
		dbs = api.databases
		db = dbs.find {|db| db.name == db_name }
		unless db
			$stderr.puts "No such database: '#{db_name}'"
			$stderr.puts "Use '#{$prog} show-database' to show list of databases."
			exit 1
		end
		db
	end
end
end

