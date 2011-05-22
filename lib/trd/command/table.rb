
module TRD
module Command

	def create_table_type(type, db_name, table_name)
		conf = cmd_config
		api = cmd_api(conf)

		db = find_database(api, db_name)

		t = db.table(table_name, type)
		if t
			$stderr.puts "Table '#{db_name}.#{table_name}' already exists."
			exit 1
		end

		db.table(table_name, type, true)

		$stderr.puts "Table '#{db_name}.#{table_name}' is created."
	end
	private :create_table_type

	def create_log_table
		op = cmd_opt 'create-log-table', :db_name, :table_name
		db_name, table_name = op.cmd_parse

		create_table_type(:log, db_name, table_name)
	end

	def create_item_table
		op = cmd_opt 'create-item-table', :db_name, :table_name
		db_name, table_name = op.cmd_parse

		create_table_type(:item, db_name, table_name)
	end

	def drop_table
		op = cmd_opt 'drop-table', :db_name, :table_name
		db_name, table_name = op.cmd_parse

		conf = cmd_config
		api = cmd_api(conf)

		db = find_database(api, db_name)

		table = db.table(table_name)
		unless table
			$stderr.puts "No such table: '#{db_name}.#{table_name}'"
			$stderr.puts "Use '#{$prog} show-tables #{db_name}' to show list of tables."
			exit 1
		end

		table.delete

		$stderr.puts "Table '#{db_name}.#{table_name}' is deleted."
	end

	def drop_table_type(type, db_name, table_name)
		conf = cmd_config
		api = cmd_api(conf)

		db = find_database(api, db_name)

		table = db.table(table_name, type)
		unless table
			$stderr.puts "No such #{type} table: '#{db_name}.#{table_name}'"
			$stderr.puts "Use '#{$prog} show-tables #{db_name}' to show list of tables."
			exit 1
		end

		table.delete

		$stderr.puts "Table '#{db_name}.#{table_name}' is deleted."
	end
	private :drop_table_type

	def drop_log_table
		op = cmd_opt 'drop-log-table', :db_name, :table_name
		db_name, table_name = op.cmd_parse

		drop_table_type(:log, db_name, table_name)
	end

	def drop_item_table
		op = cmd_opt 'drop-item-table', :db_name, :table_name
		db_name, table_name = op.cmd_parse

		drop_table_type(:item, db_name, table_name)
	end

	def show_tables
		op = cmd_opt 'show-tables', :db_name?
		db_name = op.cmd_parse

		conf = cmd_config
		api = cmd_api(conf)

		if db_name
			db = find_database(api, db_name)
			dbs = [db]
		else
			dbs = api.databases
		end

		rows = []
		dbs.each {|db|
			db.tables.each {|table|
				rows << {:Database => db.name, :Table => table.name, :Type => table.type.to_s} # TODO , :Count => table.count}
			}
		}
		rows = rows.sort_by {|map|
			[map[:Database], map[:Type].size, map[:Table]]
		}

		puts cmd_render_table(rows)

		if rows.empty?
			if db_name
				$stderr.puts "Database '#{db_name}' has no tables."
				$stderr.puts "Use '#{$prog} create-log-table #{db_name} <table_name>' to create a table."
			elsif dbs.empty?
				$stderr.puts "There are no databases."
				$stderr.puts "Use '#{$prog} create-database <db_name>' to create a database."
			else
				$stderr.puts "There are no tables."
				$stderr.puts "Use '#{$prog} create-log-table <db_name> <table_name>' to create a table."
			end
		end
	end

end
end

