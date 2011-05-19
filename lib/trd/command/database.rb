
module TRD
module Command

	def create_database
		op = cmd_opt 'create-database', :db_name
		db_name = op.cmd_parse

		conf = cmd_config
		api = cmd_api(conf)

		db = api.database(db_name, true)

		$stderr.puts "Database '#{db_name}' is created."
		$stderr.puts "Use '#{$prog} create-log-table #{db_name}.<table_name>' to create a table."
	end

	def drop_database
		op = cmd_opt 'show-databases', :db_name

		op.banner << "\noptions:\n"

		force = false
		op.on('-f', '--force', 'clear tables and delete the database', TrueClass) {|b|
			force = true
		}

		db_name = op.cmd_parse

		conf = cmd_config
		api = cmd_api(conf)

		db = find_database(api, db_name)

		unless force
			unless db.tables.empty?
				$stderr.puts "Database '#{db_name}' is not empty. Drop tables first or use '-f' option."
				exit 1
			end
		end

		db.delete

		$stderr.puts "Database '#{db_name}' is deleted."
	end

	def show_databases
		op = cmd_opt 'show-databases'
		op.cmd_parse

		conf = cmd_config
		api = cmd_api(conf)

		dbs = api.databases

		rows = []
		dbs.each {|db|
			rows << {:Name => db.name}
		}
		puts cmd_render_table(rows)

		if dbs.empty?
			$stderr.puts "There are no databases."
			$stderr.puts "Use '#{$prog} create-database <db_name>' to create a database."
		end
	end

	alias show_dbs show_databases

end
end

