
include TRD

def Command.show_databases
	op = OptionParser.new
	op.banner = <<-EOF
usage: #{$prog} show-databases

description:
  Show databases
	EOF

	(class<<self;self;end).module_eval do
		define_method(:usage) do |msg|
			puts op.to_s
			puts ""
			puts "error: #{msg}" if msg
			exit 1
		end
	end

	require 'trd/api'

	arg = {}
	TRD::API.option(op, arg)

	begin
		op.parse!(ARGV)
		usage nil if ARGV.length != 0
	rescue
		usage $!.to_s
	end

	conf = TRD::Config.read(CONFIG_PATH)

	api = TRD::API.new(arg, conf)

	databases = api.databases

	databases.each {|db|
		puts "#{db}"
	}
end


def Command.show_tables
	op = OptionParser.new
	op.banner = <<-EOF
usage: #{$prog} show-tables <db name>

description:
  Show tables in the database.
	EOF

	arg = {
		:item => false,
		:log => false,
	}

	op.on('-i', '--item', 'show item tables only', TrueClass) {|b|
		arg[:item] = b
	}

	op.on('-l', '--log', 'show log tables only', TrueClass) {|b|
		arg[:log] = b
	}

	(class<<self;self;end).module_eval do
		define_method(:usage) do |msg|
			puts op.to_s
			puts ""
			puts "error: #{msg}" if msg
			exit 1
		end
	end

	require 'trd/api'

	arg = {}
	TRD::API.option(op, arg)

	begin
		op.parse!(ARGV)
		usage nil if ARGV.length != 1

		name = ARGV[0]

	rescue
		usage $!.to_s
	end

	conf = TRD::Config.read(CONFIG_PATH)

	api = TRD::API.new(arg, conf)

	if arg[:log] && !arg[:item]
		log_tables = api.log_tables(name)
	elsif arg[:item] && !arg[:log]
		item_tables = api.item_tables(name)
	else
		log_tables = api.log_tables(name)
		item_tables = api.item_tables(name)
	end

	if log_tables
		puts "Log tables:"
		log_tables.each {|t|
			puts "  #{t}"
		}
	end

	if item_tables
		puts "Item tables:"
		item_tables.each {|t|
			puts "  #{t}"
		}
	end

	if !log_tables && !item_tables
		puts "Database '#{name}' does not exist."
	end
end

