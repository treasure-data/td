
include TRD

def Command.help
	op = OptionParser.new
	op.banner = <<-EOF
usage: #{$prog} help <command>

description:
  Show usage of a command.
	EOF

	(class<<self;self;end).module_eval do
		define_method(:usage) do |msg|
			puts op.to_s
			puts ""
			puts "error: #{msg}" if msg
			exit 1
		end
	end

	begin
		op.parse!(ARGV)
		usage nil if ARGV.empty?

		cmd = ARGV[0]

	rescue
		usage $!.to_s
	end

	ARGV.clear
	ARGV[0] = '--help'

	TRD::Command::List.call(cmd)
end

