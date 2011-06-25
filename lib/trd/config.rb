require 'trd/error'

module TRD


class Config
	def initialize
		@path = nil
		@conf = {}   # section.key = val
	end

	def self.read(path, create=false)
		new.read(path)
	end

	def [](cate_key)
		@conf[cate_key]
	end

	def []=(cate_key, val)
		@conf[cate_key] = val
	end

	def save(path=@path)
		@path = path
		write
	end

	def read(path=@path)
		@path = path
		begin
			data = File.read(@path)
		rescue
			e = ConfigNotFoundError.new($!.to_s)
      e.set_backtrace($!.backtrace)
      raise e
		end

		section = ""

		data.each_line {|line|
			line.strip!
			case line
			when /^#/
				next
			when /\[(.+)\]/
				section = $~[1]
			when /^(\w+)\s*=\s*(.+?)\s*$/
				key = $~[1]
				val = $~[2]
				@conf["#{section}.#{key}"] = val
			else
				raise ConfigParseError, "invalid config line '#{line}' at #{@path}"
			end
		}

		self
	end

	def write
		require 'fileutils'
		FileUtils.mkdir_p File.dirname(@path)
		File.open(@path, "w") {|f|
			@conf.keys.map {|cate_key|
				cate_key.split('.',2)
			}.zip(@conf.values).group_by {|(section,key),val|
				section
			}.each {|section,cate_key_vals|
				f.puts "[#{section}]"
				cate_key_vals.each {|(section,key),val|
					f.puts "  #{key} = #{val}"
				}
			}
		}
	end
end


end
