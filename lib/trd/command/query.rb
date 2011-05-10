
include TRD

def Command.query
	op = OptionParser.new
	op.banner = <<-EOF
usage: #{$prog} query <SELECT>

description:
  Execute a query.
	EOF

	(class<<self;self;end).module_eval do
		define_method(:usage) do |msg|
			puts op.to_s
			puts ""
			puts "error: #{msg}" if msg
			exit 1
		end
	end

	arg = {
		:database => nil,
	}

	require 'trd/api'
	TRD::API.option(op, arg)

	op.on('-d', '--database', 'Use the database') {|s|
		arg[:database] = s
	}

	begin
		op.parse!(ARGV)
		usage nil if ARGV.length != 1

		query = ARGV[0]

	rescue
		usage $!.to_s
	end

	if db = arg[:database]
		query = "USE #{db}; "+query
	end

	conf = TRD::Config.read(CONFIG_PATH)

	api = TRD::API.new(arg, conf)

	result = api.query(query)

	# TODO
	p result
end

