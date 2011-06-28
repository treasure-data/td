require 'trd/api_iface'
require 'trd/error'

module TRD

class API
  def self.authenticate(user, password)
    iface = APIInterface.new(nil)
    apikey = iface.authenticate(user, password)
    new(apikey)
  end

  def self.server_status
    iface = APIInterface.new(nil)
    iface.server_status
  end

  def initialize(apikey)
    @iface = APIInterface.new(apikey)
  end

  attr_reader :iface

  def apikey
    @iface.apikey
  end

  def server_status
    @iface.server_status
  end

  # => true
  def create_database(db_name)
    @iface.create_database(db_name)
  end

  # => true
  def delete_database(db_name)
    @iface.delete_database(db_name)
  end

  # => [Database]
  def databases
    names = @iface.list_databases
    names.map {|db_name|
      Database.new(self, db_name)
    }
  end

  # => Database
  def database(db_name)
    names = @iface.list_databases
    names.each {|n|
      if n == db_name
        return Database.new(self, db_name)
      end
    }
    raise NotFoundError, "Database '#{db_name}' does not exist"
  end

  # => true
  def create_table(db_name, table_name, type)
    @iface.create_table(db_name, table_name, type)
  end

  # => true
  def create_log_table(db_name, table_name)
    create_table(db_name, table_name, :log)
  end

  # => true
  def create_item_table(db_name, table_name)
    create_table(db_name, table_name, :item)
  end

  # => type:Symbol
  def delete_table(db_name, table_name)
    @iface.delete_table(db_name, table_name)
  end

  # => [Table]
  def tables(db_name)
    m = @iface.list_tables(db_name)
    m.map {|table_name,(type,count)|
      Table.new(self, db_name, table_name, type, count)
    }
  end

  # => Table
  def table(db_name, table_name)
    m = @iface.list_tables(db_name)
    m.each_pair {|name,(type,count)|
      if name == table_name
        return Table.new(self, db_name, name, type, count)
      end
    }
    raise NotFoundError, "Table '#{db_name}.#{table_name}' does not exist"
  end

  # => Job
  def query(q, db_name=nil)
    job_id = @iface.hive_query(q, db_name)
    Job.new(self, job_id, :hive, q)  # TODO url
  end

  # => [Job=]
  def jobs(from=nil, to=nil)
    js = @iface.list_jobs(from, to)
    js.map {|job_id,type,status,query|
      Job.new(self, job_id, type, query, status)  # TODO url
    }
  end

  # => Job
  def job(job_id)
    job_id = job_id.to_s
    type, query, status, result, url, debug = @iface.show_job(job_id)
    Job.new(self, job_id, type, query, status, url, result, debug)
  end

  # => type:Symbol, result:String, url:String
  def job_status(job_id)
    type, query, status, result, url, debug = @iface.show_job(job_id)
    return query, status, result, url, debug
  end

  # => time:Flaot
	def import(db_name, table_name, format, stream, stream_size=stream.lstat.size)
    @iface.import(db_name, table_name, format, stream, stream_size)
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

  def tables
    update_tables! unless @tables
    @tables
  end

  def create_table(name, type)
    @api.create_table(@db_name, name, type)
  end

  def create_log_table(name)
    create_table(name, :log)
  end

  def create_item_table(name)
    create_table(name, :item)
  end

  def table(table_name)
    @api.table(@db_name, table_name)
  end

  def delete
    @api.delete_database(@db_name)
  end

  def update_tables!
    @tables = @api.tables(@db_name)
  end
end

class Table < APIObject
  def initialize(api, db_name, table_name, type, count)
    super(api)
    @db_name = db_name
    @table_name = table_name
    @type = type
    @count = count
  end

  attr_reader :type, :count

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

  def delete
    @api.delete_table(@db_name, @table_name)
  end
end

class Job < APIObject
  def initialize(api, job_id, type, query, status=nil, url=nil, result=nil, debug=nil)
    super(api)
    @job_id = job_id
    @type = type
    @url = url
    @query = query
    @status = status
    @result = result
    @debug = debug
  end

  attr_reader :job_id, :type

  def wait(timeout=nil)
    # TODO
  end

  def query
    update_status! unless @query
    @query
  end

  def status
    update_status! unless @status
    @status
  end

  def url
    update_status! unless @url
    @url
  end

  def debug
    update_status! unless @debug
    @debug
  end

  def result
    return nil unless finished?
    update_status! unless @result
    @result.split("\n").map {|line|
      # TODO format of the result is TSV for now
      line.split("\t")
    }
  end

  def finished?
    update_status! unless @status
    if @status == "success" || @status == "error"
      return true
    else
      return false
    end
  end

  def running?
    !finished?
  end

  def update_status!
    query, status, result, url, debug = @api.job_status(@job_id)
    @query = query
    @status = status
    @result = result
    @url = url
    @debug = debug
    self
  end
end


end

