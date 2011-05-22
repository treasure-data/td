
module TRD
module Command

	def query
		op = cmd_opt 'query', :db_name, :query
		db_name, query = op.cmd_parse

		conf = cmd_config
		api = cmd_api(conf)

		db = find_database(api, db_name)

		job = db.query(query)

		$stderr.puts "Job #{job.job_id} is started."
		$stderr.puts "Use '#{$prog} job #{job.job_id}' to show the status."
	end

	def show_jobs
		op = cmd_opt 'show-jobs', :db_name?
		db_name = op.cmd_parse

		conf = cmd_config
		api = cmd_api(conf)

		jobs = api.jobs

		rows = []
		jobs.each {|job|
			rows << {:JobID => job.job_id, :Status => job.status}
		}

		puts cmd_render_table(rows)
	end

	def job
		op = cmd_opt 'job', :job_id

		op.banner << "\noptions:\n"

		verbose = nil
		op.on('-v', '--verbose', 'show verbose messages', TrueClass) {|b|
			verbose = b
		}

		job_id = op.cmd_parse

		conf = cmd_config
		api = cmd_api(conf)

		job = api.job(job_id)

		puts "JobID      : #{job.job_id}"
		puts "Status     : #{job.status}"
		if verbose # TODO debug message on show job
			puts "Debug      :"
			(job.debug || {}).each_pair {|k,v|
				puts "  #{k}:"
				v.to_s.split("\n").each {|line|
					puts "    "+line
				}
			}
		end
		#puts "Debug      :\n#{job.debug}"# if verbose

		if job.finished?
			puts "Result     :"
			cmd_render_table(job.result).split("\n").each {|line|
				puts line
			}
		end

		$stderr.puts "Use '-v' option to show detailed messages." unless verbose
	end

end
end

