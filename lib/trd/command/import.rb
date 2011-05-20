
module TRD
module Command

	def import
		op = cmd_opt 'import', :db_name, :table_name, :files_

		op.banner << "\noptions:\n"

		format = :apache

		op.on('--apache', 'import apache common log file (default)') {
			format = :apache
		}

		db_name, table_name, *paths = op.cmd_parse

		conf = cmd_config
		api = cmd_api(conf)

		db = find_database(api, db_name)

		unless db.log_table(table_name)
			$stderr.puts "No such log table: '#{db_name}.#{table_name}'"
			$stderr.puts "Use '#{$prog} show-tables #{db_name}' to show list of tables."
			exit 1
		end

		files = paths.map {|path|
			if path == '-'
				$stdin
			else
				File.open(path)
			end
		}


		files.zip(paths).each {|file,path|
			ib = ImportFileBuilder.new(db_name, table_name)
			begin
				puts "importing #{path}..."
				import_apache(file, path, ib)

				puts "uploading #{path}..."
				file, size = ib.flush
				api.import_log(db_name, table_name, file)
			ensure
				ib.close
			end
		}

		puts "done."
	end

	private
	class ImportFileBuilder
		def initialize(db, table)
			require 'tempfile'
			require 'zlib'
			@db = db
			@table = table
			@file = Tempfile.new('trd-import')
			@writer = Zlib::GzipWriter.new(@file)
			@first = true
			write_header
		rescue
			@writer.close if @writer rescue nil
			@file.close if @file rescue nil
			@file.unlink if @file rescue nil
			raise
		end

		attr_reader :db, :table

		def flush
			write_footer
			@writer.flush
			size = @file.pos
			@file.pos = 0
			return @file, size
		end

		def close
			@writer.close
			@file.close
			@file.unlink
		end

		def write_header
			@writer << %<{"table":#{(@db+'.'+@table).to_json},"logs":[>
		end

		def add(time, props)
			if @first
				@first = false
			else
				@writer.write(',')
			end
			props["timestamp"] = time
			@writer << JSON.dump(props)
		end

		def write_footer
			@writer << %<]}>
		end
	end

	def import_apache(file, path, ib)
		i = 0
		n = 0
		file.each_line {|l|
			i += 1
			begin
				m = /^(.*?) .*? .*? \[(.*?)\] "(\S+?)(?: +(.*?) +(\S*?))?" (.*?) .*? "(.*?)" "(.*?)"/.match(l)
				unless m
					raise "invalid log format at #{path}:#{i}"
				end
				t = /^(\d*?)\/(\w\w\w?)\/(\d\d\d\d)\:(\d\d\:\d\d\:\d\d) ([\d\+\-]*)/.match(m[2])
				unless t
					raise "invalid time format at #{path}:#{i}"
				end

				time = Time.parse("#{t[2]} #{t[1]} #{t[3]} #{t[4]} #{t[5]}").utc.to_i

				cols = {
					"ip" => m[1],
					"method" => m[3],
					"url" => m[4],
					"code" => m[6],
					"ua" => m[8],
				}

				ib.add(time, cols)

				n += 1
				if n % 10000 == 0
					puts "imported #{n} entries..."
				end
			rescue
				puts "ignored: #{l.dump}"
				puts "(#{$!})"
			end
		}
		puts "imported #{n} entries."
	end
end
end

