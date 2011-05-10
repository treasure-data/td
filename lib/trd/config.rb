
module TRD

class ConfigError < StandardError
end

class ConfigNotFoundError < ConfigError
end

class ConfigParseError < ConfigError
end


class Config
	def initialize
		@path = nil
		@conf = {}   # cate.key = val
	end

	def open(path)
		@path = path
		read
		self
	end

	def self.read(path, create=false)
		if File.exist?(path)
			new.open(path)
		else
			dir = File.dirname(path)
			unless File.directory?(dir)
				Dir.mkdir(dir)
			end
			File.open(path,"w") {|f| }
			new.open(path)
		end
	end

	def save(path=@path)
		@path = path
		write
	end

	def [](cate_key)
		@conf[cate_key]
	end

	def []=(cate_key, val)
		@conf[cate_key] = val
	end

	private
	def read
		begin
			data = File.read(@path)
		rescue
			raise ConfigNotFoundError, $!.to_s
		end

		cate = ""

		data.each_line {|line|
			line.strip!
			case line
			when /^#/
				next
			when /\[(.+)\]/
				cate = $~[1]
			when /^(\w+)\s*=\s*(.+?)\s*$/
				key = $~[1]
				val = $~[2]
				@conf["#{cate}.#{key}"] = val
			else
				raise ConfigParseError, "invalid config line '#{line}'"
			end
		}
	end

	def write
		File.open(@path, "w") {|f|
			@conf.keys.map {|cate_key|
				cate_key.split('.',2)
			}.zip(@conf.values).group_by {|(cate,key),val|
				cate
			}.each {|cate,cate_key_vals|
				f.puts "[#{cate}]"
				cate_key_vals.each {|(cate,key),val|
					f.puts "  #{key} = #{val}"
				}
			}
		}
	end
end


end
