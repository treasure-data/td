
include TRD

def Command.account
	op = OptionParser.new
	op.banner = <<-EOF
usage: #{$prog} config [user-name]

description:
  Configure TreasureData.com account

options:
	EOF

	(class<<self;self;end).module_eval do
		define_method(:usage) do |msg|
			puts op.to_s
			puts "error: #{msg}" if msg
			exit 1
		end
	end

	arg = {
		:force => false
	}

	op.on('-f', '--force', 'overwrite current setting', TrueClass) {|b|
		arg[:force] = b
	}

	begin
		op.parse!(ARGV)

		user = ARGV.shift if ARGV.length > 0

		usage nil if ARGV.length != 0

	rescue
		usage $!.to_s
	end

	conf = TRD::Config.read(CONFIG_PATH, true)
	if current_user = conf['account.user']
		unless arg[:force]
			puts "TreasureData.com account is already configured with '#{current_user}' account."
			puts "Add '-f' option to overwrite this setting."
			exit 0
		end
	end

	unless user
		print "User name: "
		line = STDIN.gets || ""
		user = line.strip
	end

	if user.empty?
		puts "Canceled."
		exit 0
	end

	apikey = nil

	2.times do
		begin
			system "stty -echo"  # TODO termios
			print "Password: "
			password = STDIN.gets || ""
			password = password[0..-2]  # strip \n
		ensure
			system "stty echo"   # TODO termios
			print "\n"
		end

		if password.empty?
			puts "Canceled."
			exit 0
		end

		require 'trd/api'
		api = API.new({}, conf)

		begin
			apikey = api.authenticate(user, password)
		rescue TRD::AuthError
			puts "User name or password mismatched."
		end

		break if apikey
	end
	return unless apikey

	puts "Authenticated successfully."

	conf["account.user"] = user
	conf["account.apikey"] = apikey
	conf.save

	puts "Use '#{$prog} create-database <name>' to create a database"
end

