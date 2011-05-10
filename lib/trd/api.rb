
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

	def initialize(arg, conf)
	end

	def authenticate(user, password)
		# TODO
		"apikey dummy"
	end

	def create_database(db)
		# TODO
	end

	def create_table(db, table)
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

	def tables(db)
		# TODO
		nil
	end

	def query(q)
		# TODO
	end

	private
	def get(url, params)
		require 'net/http'
	end

	def post(url, params)
		require 'net/http'
	end
end

end

