
module TRD

class APIError < StandardError
end

class AuthError < APIError
end


class API
	def self.option(op, arg)
	end

	HOST = 'office.pfidev.jp'
	PORT = 80
	USE_SSL = false
	BASE_URL = '/td_api2'

	def initialize(arg, conf)
		require 'json'
		@conf = conf
	end

	def authenticate(user, password)
		code, body, res = post("/v2/user/authenticate", {'user'=>user, 'password'=>password})
		if code != "200"
			raise_error("Authentication failed", res)
		end
		return body
	end

	def create_database(db)
		code, body, res = post("/v2/database/create/#{e db}")
		if code != "200"
			raise_error("Create database failed", res)
		end
		return nil
	end

	def create_log_table(db, table)
		code, body, res = post("/v2/log_table/create/#{e db}/#{e table}")
		if code != "200"
			raise_error("Create log table failed", res)
		end
		return nil
	end

	def create_item_table(db, table)
		code, body, res = post("/v2/item_table/create/#{e db}/#{e table}")
		if code != "200"
			raise_error("Create item table failed", res)
		end
		return nil
	end

	def drop_database(db)
		code, body, res = post("/v2/database/delete/#{e db}")
		if code != "200"
			raise_error("Drop database failed", res)
		end
		return nil
	end

	def drop_log_table(db, table)
		code, body, res = post("/v2/log_table/delete/#{e db}/#{e table}")
		if code != "200"
			raise_error("Drop log table failed", res)
		end
		return nil
	end

	def drop_item_table(db, table)
		code, body, res = post("/v2/item_table/delete/#{e db}/#{e table}")
		if code != "200"
			raise_error("Drop item table failed", res)
		end
		return nil
	end

	def databases
		code, body, res = get("/v2/database/list")
		if code != "200"
			raise_error("List databases failed", res)
		end
		# TODO format check
		js = JSON.load(body)
		names = js["databases"].map {|dbinfo| dbinfo['name'] }
		return names
	end

	def log_tables(db)
		code, body, res = get("/v2/log_table/list/#{e db}")
		if code == "404"
			return nil
		elsif code != "200"
			raise_error("List log tables failed", res)
		end
		# TODO format check
		js = JSON.load(body)
		names = js["log_tables"].map {|tblinfo| tblinfo['name'] }
		names
	end

	def item_tables(db)
		code, body, res = get("/v2/item_table/list/#{e db}")
		if code == "404"
			return nil
		elsif code != "200"
			raise_error("List item tables failed", res)
		end
		# TODO format check
		js = JSON.load(body)
		names = js["item_tables"].map {|tblinfo| tblinfo['name'] }
		names
	end

	def query(db, q)
		code, body, res = post("/v2/hive/issue/#{e db}", {'query'=>q})
		if code != "200"
			raise_error("Query failed", res)
		end
		return JSON.load(body)
	end

	private
	def get(url, params=nil, api_auth=true)
		http, header = new_http(api_auth, :get)

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

	def post(url, params=nil, api_auth=true)
		http, header = new_http(api_auth, :post)

		path = BASE_URL + url

		request = Net::HTTP::Post.new(path, header)
		request.set_form_data(params) if params

		response = http.request(request)
		return [response.code, response.body, response]
	end

	def new_http(api_auth, type)
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
		if api_auth
			apikey = @conf['account.apikey']
			unless apikey
				raise "Account is not configured. Run '#{$prog} account' first."
			end
			header['Authorization'] = "TRD #{apikey}"
		end
		header['Date'] = Time.now.rfc2822

		return http, header
	end

	def raise_error(msg, res)
		# TODO error
		raise APIError, "#{msg}: #{res.body}"
	end

	def e(s)
		require 'cgi'
		CGI.escape(s.to_s)
	end
end


end

