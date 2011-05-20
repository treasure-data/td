require 'trd/api_iface'
require 'trd/error'

module TRD

class API
	def self.authenticate(user, password)
		iface = APIInterface.new(nil)
		apikey = iface.authenticate(user, password)
		new(apikey)
	end

	def initialize(apikey)
		@iface = APIInterface.new(apikey)
	end

	attr_reader :iface

	def apikey
		@iface.apikey
	end

	def database(db_name, create=false)
		if create
			@iface.create_database(db_name)
		end
		log_counts, item_counts = @iface.show_tables(db_name)
		logs = log_counts.map {|table_name,count|
			LogTable.new(self, db_name, table_name, count)
		}
		items = item_counts.map {|table_name,count|
			ItemTable.new(self, db_name, table_name, count)
		}
		tables = logs + items
		Database.new(self, db_name, tables)
	end

	def databases
		names = @iface.list_databases
		names.map {|db_name|
			Database.new(self, db_name)
		}
	end

	def delete_database(db_name)
		@iface.drop_database(db_name)
	end

	def log_table(db_name, table_name, create=false)
		if create
			@iface.create_log_table(db_name, table_name)
			return LogTable.new(self, db_name, table_name)
		end
		count = @iface.show_log_tables(db_name)[table_name]
		unless count
			raise NotFoundError, "Log table '#{table_name}' does not exist"
		end
		LogTable.new(self, db_name, table_name, count)
	end

	def item_table(db_name, table_name, create=false)
		if create
			@iface.create_item_table(db_name, table_name)
			return ItemTable.new(self, db_name, table_name)
		end
		count = @iface.show_item_tables(db_name)[table_name]
		unless count
			raise NotFoundError, "Item table '#{table_name}' does not exist"
		end
		ItemTable.new(self, db_name, table_name, count)
	end

	def log_tables(db_name)
		counts = @iface.show_log_tables(db_name)
		counts.map {|table_name,count|
			LogTable.new(self, db_name, table_name, count)
		}
	end

	def item_tables(db_name)
		counts = @iface.show_item_tables(db_name)
		counts.map {|table_name,count|
			ItemTable.new(self, db_name, table_name, count)
		}
	end

	def delete_log_table(db_name, table_name)
		@iface.drop_log_table(db_name, table_name)
	end

	def delete_item_table(db_name, table_name)
		@iface.drop_item_table(db_name, table_name)
	end

	def delete_table(db_name, table_name)
		begin
			@iface.drop_log_table(db_name, table_name)
		rescue NotFoundError
			@iface.drop_item_table(db_name, table_name)
		end
	end

	def tables(db_name)
		log_tables(db_name) + item_tables(db_name)
	end

	def log_count(db_name, table_name)
		count = @iface.show_log_tables(db_name)[table_name]
		unless count
			raise NotFoundError, "Log table '#{table_name}' does not exist"
		end
		count
	end

	def item_count(db_name, table_name)
		count = @iface.show_item_tables(db_name)[table_name]
		unless count
			raise NotFoundError, "Item table '#{table_name}' does not exist"
		end
		count
	end

	def query(db_name, query)
		job_id = @iface.query(db_name, query)
		Job.new(self, db_name, job_id)
	end

	def job(db_name, job_id)
		job_id = job_id.to_s
		jobs = @iface.list_jobs
		j = jobs.find {|j| j[:job_id] == job_id }
		unless j
			raise NotFoundError, "Job #{job_id} does not exist"
		end
		Job.new(self, db_name, job_id, j['status'])
	end

	def jobs
		js = @iface.list_jobs
		js.map {|j|
			Job.new(self, j['database'], j['job_id'], j['status'])
		}
	end

	def import(type, db, table, stream, stream_size=stream.lstat.size, format="json.gz")
		if type == :log
			import_log(db, table, stream, stream_size, format)
		elsif type == :item
			import_item(db, table, stream, stream_size, format)
		else
			raise ArgumentError, "type should be :log or :item"
		end
	end

	def import_log(db, table, stream, stream_size=stream.lstat.size, format="json.gz")
		@iface.import_log(db, table, stream, stream_size, format)
	end

	def import_item(db, table, stream, stream_size=stream.lstat.size, format="json.gz")
		@iface.import_item(db, table, stream, stream_size, format)
	end
end

end


module TRD

class APIObject
	def initialize(api)
		@api = api
	end
end

class Database < APIObject
	def initialize(api, db_name, tables=nil)
		super(api)
		@db_name = db_name
		@tables = tables
	end

	def name
		@db_name
	end

	def query(q)
		@api.query(@db_name, q)
	end

	def log_tables
		update_tables! unless @tables
		@tables.select {|table| table.log_table?  }
	end

	def item_tables
		update_tables! unless @tables
		@tables.select {|table| table.item_table?  }
	end

	def tables
		update_tables! unless @tables
		@tables
	end

	def log_table(table_name)
		log_tables.find {|table| table.name == table_name }
	end

	def item_table(table_name)
		item_tables.find {|table| table.name == table_name }
	end

	def table(table_name, type=nil, create=false)
		update_tables! unless @tables
		case type
		when nil
			return @tables.find {|table| table.name == table_name }
		when :log, :item
			table = @tables.find {|table|
				table.type == type && table.name == table_name
			}
			if table
				return table
			end

			if create
				if type == :log
					table = @api.log_table(@db_name, table_name, true)
				else
					table = @api.item_table(@db_name, table_name, true)
				end
				@tables << table
				return table
			end
		else
			raise ArgumentError, "invalid type name '#{type}'"
		end
		nil
	end

	def log_table(table_name, create=false)
		table(table_name, :log, create)
	end

	def item_table(table_name, create=false)
		table(table_name, :item, create)
	end

	def delete
		@api.delete_database(@db_name)
	end

	def update_tables!
		@tables = @api.tables(@db_name)
	end
end

class Table < APIObject
	def initialize(api, type, db_name, table_name, count=nil)
		super(api)
		@type = type
		@db_name = db_name
		@table_name = table_name
		@count = count
	end

	attr_reader :type

	def database_name
		@db_name
	end

	def database
		@api.database(@db_name)
	end

	def name
		@table_name
	end

	def identifier
		"#{@db_name}.#{@table_name}"
	end

	def log_table?
		@type == :log
	end

	def item_table?
		@type == :item
	end

	def count
		update_count! unless @count
		@count
	end

	def delete
		if log_table?
			@api.delete_log_table(@db_name, @table_name)
		else
			@api.delete_item_table(@db_name, @table_name)
		end
	end
end

class ItemTable < Table
	def initialize(api, db_name, table_name, count=nil)
		super(api, :item, db_name, table_name, count)
	end

	def update_count!
		@count = @api.item_count(@db_name, @table_name)
	end
end

class LogTable < Table
	def initialize(api, db_name, table_name, count=nil)
		super(api, :log, db_name, table_name, count)
	end

	def update_count!
		@count = @api.log_count(@db_name, @table_name)
	end
end

class Job < APIObject
	def initialize(api, db_name, job_id, status=nil, result=nil, debug=nil)
		super(api)
		@db_name = db_name
		@job_id = job_id
		@status = status
		@result = result
		@debug = debug
	end

	attr_reader :job_id

	def database_name
		@db_name
	end

	def wait(timeout=nil)
		# TODO
	end

	def status
		return nil unless finished?
		@status
	end

	def result
		return nil unless finished?
		@result
	end

	def debug
		return nil unless finished?
		@debug
	end

	def finished?
		update_status! unless @status
		@status != "running"
	end

	def running?
		!finished?
	end

	def update_status!
		map = @api.iface.show_job(db_name, job_id)
		@result = map['result']
		@status = map['status']
		@debug = map['debug']
	end
end

end

