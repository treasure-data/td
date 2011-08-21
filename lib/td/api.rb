
module TreasureData


class APIError < StandardError
end

class AuthError < APIError
end

class AlreadyExistsError < APIError
end

class NotFoundError < APIError
end


class API
  def initialize(apikey)
    require 'json'
    @apikey = apikey
  end

  # TODO error check & raise appropriate errors

  attr_reader :apikey

  def self.validate_database_name(name)
    name = name.to_s
    if name.empty?
      raise "Empty name is not allowed"
    end
    if name.length < 3 || 32 < name.length
      raise "Name must be 3 to 32 characters, got #{name.length} characters."
    end
    unless name =~ /^([a-z0-9_]+)$/
      raise "Name must consist only of alphabets, numbers, '_'."
    end
    name
  end

  def self.validate_table_name(name)
    validate_database_name(name)
  end

  def self.validate_column_name(name)
    name = name.to_s
    if name.empty?
      raise "Empty column name is not allowed"
    end
    if 32 < name.length
      raise "Column name must be to 32 characters, got #{name.length} characters."
    end
    unless name =~ /^([a-z0-9_]+)$/
      raise "Column name must consist only of alphabets, numbers, '_'."
    end
  end

  def self.normalize_type_name(name)
    case name
    when /int/i, /integer/i
      "int"
    when /long/i, /bigint/i
      "long"
    when /string/i
      "string"
    when /float/i
      "float"
    when /double/i
      "double"
    else
      raise "Type name must eather of int, long, string float or double"
    end
  end

  ####
  ## Database API
  ##

  # => [name:String]
  def list_databases
    code, body, res = get("/v3/database/list")
    if code != "200"
      raise_error("List databases failed", res)
    end
    # TODO format check
    js = JSON.load(body)
    names = js["databases"].map {|dbinfo| dbinfo['name'] }
    return names
  end

  # => true
  def delete_database(db)
    code, body, res = post("/v3/database/delete/#{e db}")
    if code != "200"
      raise_error("Delete database failed", res)
    end
    return true
  end

  # => true
  def create_database(db)
    code, body, res = post("/v3/database/create/#{e db}")
    if code != "200"
      raise_error("Create database failed", res)
    end
    return true
  end


  ####
  ## Table API
  ##

  # => {name:String => [type:Symbol, count:Integer]}
  def list_tables(db)
    code, body, res = get("/v3/table/list/#{e db}")
    if code != "200"
      raise_error("List tables failed", res)
    end
    # TODO format check
    js = JSON.load(body)
    result = {}
    js["tables"].map {|m|
      name = m['name']
      type = (m['type'] || '?').to_sym
      count = (m['count'] || 0).to_i  # TODO?
      schema = JSON.parse(m['schema'] || '[]')
      result[name] = [type, schema, count]
    }
    return result
  end

  def create_log_or_item_table(db, table, type)
    code, body, res = post("/v3/table/create/#{e db}/#{e table}/#{type}")
    if code != "200"
      raise_error("Create #{type} table failed", res)
    end
    return true
  end
  private :create_log_or_item_table

  # => true
  def create_log_table(db, table)
    create_table(db, table, :log)
  end

  # => true
  def create_item_table(db, table)
    create_table(db, table, :item)
  end

  def create_table(db, table, type)
    schema = schema.to_s
    code, body, res = post("/v3/table/create/#{e db}/#{e table}/#{type}")
    if code != "200"
      raise_error("Create #{type} table failed", res)
    end
    return true
  end
  private :create_table

  # => true
  def update_schema(db, table, schema_json)
    code, body, res = post("/v3/table/update-schema/#{e db}/#{e table}", {'schema'=>schema_json})
    if code != "200"
      raise_error("Create schema table failed", res)
    end
    return true
  end

  # => type:Symbol
  def delete_table(db, table)
    code, body, res = post("/v3/table/delete/#{e db}/#{e table}")
    if code != "200"
      raise_error("Drop table failed", res)
    end
    # TODO format check
    js = JSON.load(body)
    type = (js['type'] || '?').to_sym
    return type
  end


  ####
  ## Job API
  ##

  # => [(jobId:String, type:Symbol, status:String, start_at:String, end_at:String)]
  def list_jobs(from=0, to=nil)
    params = {}
    params['from'] = from.to_s if from
    params['to'] = to.to_s if to
    code, body, res = get("/v3/job/list", params)
    if code != "200"
      raise_error("List jobs failed", res)
    end
    # TODO format check
    js = JSON.load(body)
    result = []
    js['jobs'].each {|m|
      job_id = m['job_id']
      type = (m['type'] || '?').to_sym
      status = m['status']
      query = m['query']
      start_at = m['start_at']
      end_at = m['end_at']
      result << [job_id, type, status, query, start_at, end_at]
    }
    return result
  end

  # => (type:Symbol, status:String, result:String, url:String)
  def show_job(job_id)
    code, body, res = get("/v3/job/show/#{e job_id}")
    if code != "200"
      raise_error("Show job failed", res)
    end
    # TODO format check
    js = JSON.load(body)
    # TODO debug
    type = (js['type'] || '?').to_sym  # TODO
    query = js['query']
    status = js['status']
    debug = js['debug']
    url = js['url']
    start_at = js['start_at']
    end_at = js['end_at']
    return [type, query, status, url, debug, start_at, end_at]
  end

  def job_result(job_id)
    require 'msgpack'
    code, body, res = get("/v3/job/result/#{e job_id}", {'format'=>'msgpack'})
    if code != "200"
      raise_error("Get job result failed", res)
    end
    result = []
    MessagePack::Unpacker.new.feed_each(body) {|row|
      result << row
    }
    return result
  end

  def job_result_format(job_id, format)
    # TODO chunked encoding
    code, body, res = get("/v3/job/result/#{e job_id}", {'format'=>format})
    if code != "200"
      raise_error("Get job result failed", res)
    end
    return body
  end

  def job_result_each(job_id, &block)
    # TODO chunked encoding
    require 'msgpack'
    code, body, res = get("/v3/job/result/#{e job_id}", {'format'=>'msgpack'})
    if code != "200"
      raise_error("Get job result failed", res)
    end
    result = []
    MessagePack::Unpacker.new.feed_each(body) {|row|
      yield row
    }
    nil
  end

  def job_result_raw(job_id, format)
    code, body, res = get("/v3/job/result/#{e job_id}", {'format'=>format})
    if code != "200"
      raise_error("Get job result failed", res)
    end
    return body
  end

  # => jobId:String
  def hive_query(q, db=nil)
    code, body, res = post("/v3/job/issue/hive/#{e db}", {'query'=>q})
    if code != "200"
      raise_error("Query failed", res)
    end
    # TODO format check
    js = JSON.load(body)
    return js['job_id'].to_s
  end


  ####
  ## Import API
  ##

  # => time:Float
  def import(db, table, format, stream, size)
    code, body, res = put("/v3/table/import/#{e db}/#{e table}/#{format}", stream, size)
    if code[0] != ?2
      raise_error("Import failed", res)
    end
    # TODO format check
    js = JSON.load(body)
    time = js['time'].to_f
    return time
  end


  ####
  ## User API
  ##

  # apikey:String
  def authenticate(user, password)
    code, body, res = post("/v3/user/authenticate", {'user'=>user, 'password'=>password})
    if code != "200"
      raise_error("Authentication failed", res)
    end
    # TODO format check
    js = JSON.load(body)
    apikey = js['apikey']
    return apikey
  end

  ####
  ## Server Status API
  ##

  # => status:String
  def server_status
    code, body, res = get('/v3/system/server_status')
    if code != "200"
      return "Server is down (#{code})"
    end
    # TODO format check
    js = JSON.load(body)
    status = js['status']
    return status
  end

  private
  host = 'api.treasure-data.com'
  port = 80
  if e = ENV['TD_API_SERVER']
    host, port_ = e.split(':',2)
    port_ = port_.to_i
    port = port_ if port_ != 0
  end

  HOST = host
  PORT = port
  USE_SSL = false
  BASE_URL = ''

  def get(url, params=nil)
    http, header = new_http

    path = BASE_URL + url
    if params && !params.empty?
      path << "?"+params.map {|k,v|
        "#{k}=#{e v}"
      }.join('&')
    end

    request = Net::HTTP::Get.new(path, header)

    response = http.request(request)
    return [response.code, response.body, response]
  end

  def post(url, params=nil)
    http, header = new_http

    path = BASE_URL + url

    request = Net::HTTP::Post.new(path, header)
    request.set_form_data(params) if params

    response = http.request(request)
    return [response.code, response.body, response]
  end

  def put(url, stream, size)
    http, header = new_http

    path = BASE_URL + url

    header['Content-Type'] = 'application/octet-stream'
    header['Content-Length'] = size.to_s

    request = Net::HTTP::Put.new(url, header)
    if stream.class.name == 'StringIO'
      request.body = stream.string
    else
      if request.respond_to?(:body_stream=)
        request.body_stream = stream
      else  # Ruby 1.8
        request.body = stream.read
      end
    end

    response = http.request(request)
    return [response.code, response.body, response]
  end

  def new_http
    require 'net/http'
    require 'time'

    http = Net::HTTP.new(HOST, PORT)
    if USE_SSL
      http.use_ssl = true
      http.verify_mode = OpenSSL::SSL::VERIFY_PEER
      store = OpenSSL::X509::Store.new
      http.cert_store = store
    end

    #http.read_timeout = options[:read_timeout]

    header = {}
    if @apikey
      header['Authorization'] = "TD1 #{apikey}"
    end
    header['Date'] = Time.now.rfc2822

    return http, header
  end

  def raise_error(msg, res)
    begin
      js = JSON.load(res.body)
      msg = js['message']
      error_code = js['error_code']

      if res.code == "404"
        raise NotFoundError, "#{error_code}: #{msg}"
      elsif res.code == "409"
        raise AlreadyExistsError, "#{error_code}: #{msg}"
      else
        raise APIError, "#{error_code}: #{msg}"
      end

    rescue
      if res.code == "404"
        raise NotFoundError, "#{msg}: #{res.body}"
      elsif res.code == "409"
        raise AlreadyExistsError, "#{msg}: #{res.body}"
      else
        raise APIError, "#{msg}: #{res.body}"
      end
    end
    # TODO error
  end

  def e(s)
    require 'cgi'
    CGI.escape(s.to_s)
  end
end


end

