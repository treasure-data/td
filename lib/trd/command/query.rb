
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
		$stderr.puts "Use '#{$0} job #{db_name} #{job.job_id}' to show the status."
	end

	def show_jobs
		op = cmd_opt 'show-jobs', :db_name?
		db_name = op.cmd_parse

		conf = cmd_config
		api = cmd_api(conf)

		jobs = api.jobs

		rows = []
		jobs.each {|job|
			rows << {:Database => job.database_name, :JobID => job.job_id, :Status => job.status}
		}

		puts cmd_render_table(rows)
	end

	def job
		op = cmd_opt 'job', :db_name, :job_id

		op.banner << "\noptions:\n"

		verbose = nil
		op.on('-v', '--verbose', 'show verbose messages', TrueClass) {|b|
			verbose = b
		}

		db_name, job_id = op.cmd_parse

		conf = cmd_config
		api = cmd_api(conf)

		db = find_database(api, db_name)

		job = api.job(db_name, job_id)

		puts "Database   : #{job.database_name}"
		puts "JobID      : #{job.job_id}"
		puts "Status     : #{job.status}"
		puts "Debug      :\n#{job.debug}" if verbose

		if job.finished?
			puts cmd_render_table(job.result)
		end
	end

end
end

