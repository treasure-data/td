
module TRD

class APIError < StandardError
end

class AuthError < APIError
end


class API
	def self.option(op, arg)
	end

	HOST = 'treasure-data.com'
	PORT = 80
	USE_SSL = false
	BASE_URL = ""

	def initialize(arg, conf)
	end

	def authenticate(user, password)
		# TODO
		"apikey dummy"
	end

	def create_database(db)
		# TODO
	end

	def create_log_table(db, table)
		# TODO
	end

	def create_item_table(db, table)
		# TODO
	end

	def drop_database(db)
		# TODO
	end

	def drop_table(db, table)
		# TODO
	end

	def databases
		# TODO
		[]
	end

	def log_tables(db)
		# TODO
		nil
	end

	def item_tables(db)
		# TODO
		nil
	end

	def query(q)
		# TODO
	end

	private
	def get(url, params=nil, api_auth=true)
		http = new_http(api_auth, :get)

		path = BASE_URL + url
		if params && !params.empty?
			path << params.map {|k,v|
				"#{k}=#{e v}"
			}.join('&')
		end

		request = Net::HTTP::Get.new(url)

		response = http.request(request)
		return [response, response.body, response.status]
	end

	def post(url, params=nil, api_auth=true)
		http = new_http(api_auth, :post)

		path = BASE_URL + url

		request = Net::HTTP::Post.new(url)
		request.set_form_data(params) if params

		response = http.request(request)
		return [response, response.body, response.status]
	end

	def new_http(api_auth, type)
		require 'net/http'
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
			header['Authenticate'] = @conf['account.apikey']
		end
		header['Date'] = Time.now.rfc2822

		http
	end

	def e(s)
		require 'cgi'
		CGI.escape(s.to_s)
	end
end

end

