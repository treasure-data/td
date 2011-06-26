
module TRD


class APIInterface
  def initialize(apikey)
    require 'json'
    @apikey = apikey
  end

  # TODO error check & raise appropriate errors

  attr_reader :apikey

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
      type = m['type'].to_sym
      count = m['count'] || 0  # TODO?
      result[name] = [type, count]
    }
    return result
  end

  # => true
  def create_table(db, table, type)
    code, body, res = post("/v3/table/create/#{e db}/#{e table}/#{type}")
    if code != "200"
      raise_error("Create #{type} table failed", res)
    end
    return true
  end

  # => true
  def create_log_table(db, table)
    create_table(db, table, :log)
  end

  # => true
  def create_item_table(db, table)
    create_table(db, table, :item)
  end

  # => type:Symbol
  def delete_table(db, table)
    code, body, res = post("/v3/table/delete/#{e db}/#{e table}")
    if code != "200"
      raise_error("Drop table failed", res)
    end
    # TODO format check
    js = JSON.load(body)
    type = js['type'].to_sym
    return type
  end


  ####
  ## Job API
  ##

  # => [(jobId:String, type:Symbol, status:String)]
  def list_jobs(from=0, to=nil)
    code, body, res = get("/v3/job/list")
    if code != "200"
      raise_error("List jobs failed", res)
    end
    # TODO format check
    js = JSON.load(body)
    result = []
    js['jobs'].each {|m|
      job_id = m['job_id']
      type = m['type'].to_sym
      status = m['status']
      result << [job_id, type, status]
    }
    return result
  end

  # => (type:Symbol, status:String, result:String, url:String)
  def show_job(job_id, from=nil, to=nil)
    params = {}
    params['from'] = from.to_s if from
    params['to'] = to.to_s if to
    code, body, res = get("/v3/job/show/#{e job_id}", params)
    if code != "200"
      raise_error("Show job failed", res)
    end
    # TODO format check
    js = JSON.load(body)
    # TODO debug
    type = (js['type'] || 'hive').to_sym  # TODO
    status = js['status']
    result = js['result']
    url = js['url']
    return [type, status, result, url]
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
  def import(db, table, format, stream, stream_size=stream.lstat.size)
    code, body, res = put("/v3/table/import/#{e db}/#{e table}/#{format}", stream, stream_size)
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
  HOST = '50.19.208.190'
  PORT = 80
  USE_SSL = false
  BASE_URL = ''

  def get(url, params=nil)
    http, header = new_http

    path = BASE_URL + url
    if params && !params.empty?
      path << params.map {|k,v|
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

  def put(url, stream, stream_size)
    http, header = new_http

    path = BASE_URL + url

    header['Content-Length'] = stream_size.to_s

    request = Net::HTTP::Put.new(url, header)
    if request.respond_to?(:body_stream=)
      request.body_stream = stream
    else  # Ruby 1.8
      request.body = stream.read
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
      header['Authorization'] = "TRD #{apikey}"
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

