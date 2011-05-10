
include TRD

def Command.create_database
	op = OptionParser.new
	op.banner = <<-EOF
usage: #{$prog} create-database <db name>

description:
  Create a database.
	EOF

	(class<<self;self;end).module_eval do
		define_method(:usage) do |msg|
			puts op.to_s
			puts ""
			puts "error: #{msg}" if msg
			exit 1
		end
	end

	arg = {}

	require 'trd/api'
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
	api.create_database(name)

	puts "Database '#{name}' is created."
	puts "Use '#{$prog} create-table #{name}.<tabl name>' to create a table."
end


def Command.create_log_table
	op = OptionParser.new
	op.banner = <<-EOF
usage: #{$prog} create-log-table <db name>.<table name>

description:
  Create a log table in the database.
	EOF

	(class<<self;self;end).module_eval do
		define_method(:usage) do |msg|
			puts op.to_s
			puts ""
			puts "error: #{msg}" if msg
			exit 1
		end
	end

	arg = {}

	require 'trd/api'
	TRD::API.option(op, arg)

	begin
		op.parse!(ARGV)
		usage nil if ARGV.length != 1

		name = ARGV[0]

		if name.count('.') != 1
			raise "Invalid table name '#{name}'. Use <db>.<table> format like 'myapp.access'."
		end

		db, table = name.split('.')

	rescue
		usage $!.to_s
	end

	conf = TRD::Config.read(CONFIG_PATH)

	api = TRD::API.new(arg, conf)

	api.create_log_table(db, table)

	puts "Log table '#{name}' is created."
end


def Command.create_item_table
	op = OptionParser.new
	op.banner = <<-EOF
usage: #{$prog} create-item-table <db name>.<table name>

description:
  Create a item table in the database.
	EOF

	(class<<self;self;end).module_eval do
		define_method(:usage) do |msg|
			puts op.to_s
			puts ""
			puts "error: #{msg}" if msg
			exit 1
		end
	end

	arg = {}

	require 'trd/api'
	TRD::API.option(op, arg)

	begin
		op.parse!(ARGV)
		usage nil if ARGV.length != 1

		name = ARGV[0]

		if name.count('.') != 1
			raise "Invalid table name '#{name}'. Use <db>.<table> format like 'myapp.user'."
		end

		db, table = name.split('.')

	rescue
		usage $!.to_s
	end

	conf = TRD::Config.read(CONFIG_PATH)

	api = TRD::API.new(arg, conf)

	api.create_item_table(db, table)

	puts "Item table '#{name}' is created."
end


def Command.drop_database
	op = OptionParser.new
	op.banner = <<-EOF
usage: #{$prog} drop-database <db name>

description:
  Delete a database.
	EOF

	arg = {
		:force => false
	}

	op.on('-f', '--force', 'don\'t confirm to delete', TrueClass) {|b|
		arg[:force] = b
	}

	(class<<self;self;end).module_eval do
		define_method(:usage) do |msg|
			puts op.to_s
			puts ""
			puts "error: #{msg}" if msg
			exit 1
		end
	end

	arg = {}

	require 'trd/api'
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

	unless api.databases.include?(name)
		puts "Database '#{name}' does not exist."
		exit 1
	end

	unless arg[:force]
		print "Are you sure to delete database #{name}? [y/N]:"
		line = STDIN.gets || ""
		unless line =~ /y/i || line =~ /yes/i
			puts "Canceled."
			exit 0
		end
	end

	api.drop_database(name)

	puts "Database '#{name}' is deleted."
end


def Command.drop_table
	op = OptionParser.new
	op.banner = <<-EOF
usage: #{$prog} drop-table <db name>.<table name>

description:
  Delete a table.
	EOF

	arg = {
		:force => false,
		:only_type => nil,
	}

	op.on('-f', '--force', 'don\'t confirm to delete', TrueClass) {|b|
		arg[:force] = b
	}

	op.on('-i', '--item', 'drop a item table', TrueClass) {|b|
		arg[:only_type] = b
	}

	op.on('-l', '--log', 'drop a log table', TrueClass) {|b|
		arg[:only_type] = b
	}

	(class<<self;self;end).module_eval do
		define_method(:usage) do |msg|
			puts op.to_s
			puts ""
			puts "error: #{msg}" if msg
			exit 1
		end
	end

	arg = {}

	require 'trd/api'
	TRD::API.option(op, arg)

	begin
		op.parse!(ARGV)
		usage nil if ARGV.length != 1

		name = ARGV[0]

		if name.count('.') != 1
			raise "Invalid table name '#{name}'. Use <db>.<table> format like 'myapp.access'."
		end

		db, table = name.split('.')

	rescue
		usage $!.to_s
	end

	conf = TRD::Config.read(CONFIG_PATH)

	api = TRD::API.new(arg, conf)

	log_tables = api.log_tables(db)
	item_tables = api.item_tables(db)
	if !log_tables && !item_tables
		puts "Database '#{db}' does not exist."
		exit 1
	end
	log_tables ||= []
	item_tables ||= []

	case arg[:only_type]
	when :log
		if log_tables.include?(table)
			type = :log
		else
			puts "Log table '#{name}' does not exist."
			exit 1
		end
	when :item
		if item_tables.include?(table)
			type = :item
		else
			puts "Item table '#{name}' does not exist."
			exit 1
		end
	else
		if log_tables.include?(table)
			type = :log
		elsif item_tables.include?(table)
			type = :item
		else
			puts "Table '#{name}' does not exist."
			exit 1
		end
	end

	unless arg[:force]
		print "Are you sure to delete #{type} table #{name}? [y/N]:"
		line = STDIN.gets || ""
		unless line =~ /y/i || line =~ /yes/i
			puts "Canceled."
			exit 0
		end
	end

	if type == :item
		api.drop_item_table(db, table)
	else
		api.drop_log_table(db, table)
	end

	puts "Table '#{name}' is deleted."
end

