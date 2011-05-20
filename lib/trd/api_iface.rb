
module TRD


class APIInterface
	def initialize(apikey)
		require 'json'
		@apikey = apikey
	end

	# TODO error check & raise appropriate errors

	attr_reader :apikey

	# => true
	def create_database(db)
		code, body, res = post("/v2/database/create/#{e db}")
		if code != "200"
			raise_error("Create database failed", res)
		end
		return true
	end

	# => true
	def create_log_table(db, table)
		code, body, res = post("/v2/log_table/create/#{e db}/#{e table}")
		if code != "200"
			raise_error("Create log table failed", res)
		end
		return true
	end

	# => true
	def create_item_table(db, table)
		code, body, res = post("/v2/item_table/create/#{e db}/#{e table}")
		if code != "200"
			raise_error("Create item table failed", res)
		end
		return true
	end

	# => true
	def drop_database(db)
		code, body, res = post("/v2/database/delete/#{e db}")
		if code != "200"
			raise_error("Drop database failed", res)
		end
		return true
	end

	# => true
	def drop_log_table(db, table)
		code, body, res = post("/v2/log_table/delete/#{e db}/#{e table}")
		if code != "200"
			raise_error("Drop log table failed", res)
		end
		return true
	end

	# => true
	def drop_item_table(db, table)
		code, body, res = post("/v2/item_table/delete/#{e db}/#{e table}")
		if code != "200"
			raise_error("Drop item table failed", res)
		end
		return true
	end

	# => [name:String]
	def list_databases
		code, body, res = get("/v2/database/list")
		if code != "200"
			raise_error("List databases failed", res)
		end
		# TODO format check
		js = JSON.load(body)
		names = js["databases"].map {|dbinfo| dbinfo['name'] }
		return names
	end

	# => [logTables, itemTables]
	def show_tables(db)
		code, body, res = get("/v2/database/show/#{e db}")
		if code != "200"
			raise_error("List item tables failed", res)
		end
		# TODO format check
		js = JSON.load(body)
		logs = {}
		js["log_tables"].each {|m|
			logs[m['name']] = m['count']
		}
		items = {}
		js["item_tables"].each {|m|
			items[m['name']] = m['count']
		}
		return logs, items
	end

	# => {name:String => count:Integer}
	def show_log_tables(db)
		code, body, res = get("/v2/log_table/list/#{e db}")
		if code != "200"
			raise_error("List log tables failed", res)
		end
		# TODO format check
		js = JSON.load(body)
		result = {}
		js["log_tables"].each {|m|
			result[m['name']] = m['count'] || 0  # TODO?
		}
		return result
	end

	# => {name:String => count:Integer}
	def show_item_tables(db)
		code, body, res = get("/v2/item_table/list/#{e db}")
		if code != "200"
			raise_error("List item tables failed", res)
		end
		# TODO format check
		js = JSON.load(body)
		result = {}
		js["item_tables"].each {|m|
			result[m['name']] = m['count']
		}
		return result
	end

	# => jobId:Integer
	def query(db, q)
		code, body, res = post("/v2/hive/issue/#{e db}", {'query'=>q})
		if code != "200"
			raise_error("Query failed", res)
		end
		# TODO format check
		js = JSON.load(body)
		return js['job_id']
	end

	# [{'job_id' => id:Integer, 'database' => db:String, 'status' => str}]
	def list_jobs
		code, body, res = get("/v2/job/list")
		if code != "200"
			raise_error("List jobs failed", res)
		end
		# TODO format check
		js = JSON.load(body)
		js['jobs']
	end

	# {'job_id' => id:Integer, 'database' => db:String, 'status' => str, 'result' => obj, 'debug' => obj}
	def show_job(db, job_id)
		code, body, res = get("/v2/hive/status/#{e db}/#{e job_id}")
		if code != "200"
			raise_error("Show job failed", res)
		end
		# TODO format check
		js = JSON.load(body)
		js
	end

	def import_log(db, table, stream, stream_size=stream.lstat.size, format="json.gz")
		code, body, res = put("/v2/log_table/import/#{e db}/#{e table}/#{format}", stream, stream_size)
		if code[0] != ?2
			raise_error("Import log failed", res)
		end
		return true
	end

	def import_item(db, table, stream, stream_size=stream.lstat.size, format="json.gz")
		code, body, res = put("/v2/item_table/import/#{e db}/#{e table}/#{format}", stream, stream_size)
		if code[0] != ?2
			raise_error("Import item failed", res)
		end
		return true
	end

	def authenticate(user, password)
		code, body, res = post("/v2/user/authenticate", {'user'=>user, 'password'=>password})
		if code != "200"
			raise_error("Authentication failed", res)
		end
		# TODO format check
		js = JSON.load(body)
		apikey = js['apikey']
		return apikey
	end

	private
	HOST = 'api.treasure-data.com'
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
		# TODO error
		if res.code == "404"
			raise NotFoundError, "#{msg}: #{res.body}"
		else
			raise APIError, "#{msg}: #{res.body}"
		end
	end

	def e(s)
		require 'cgi'
		CGI.escape(s.to_s)
	end
end


end

