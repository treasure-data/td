
module TreasureData
module Command

  def sched_list(op)
    require 'td/command/job'  # job_priority_name_of

    op.cmd_parse

    client = get_client

    scheds = client.schedules

    rows = []
    has_org = false
    scheds.each {|sched|
      rows << {:Name => sched.name, :Cron => sched.cron, :Timezone => sched.timezone, :Delay => sched.delay, :Priority => job_priority_name_of(sched.priority), :Result => sched.result_url, :Database => sched.database, :Query => sched.query, :"Next schedule" => sched.next_time ? sched.next_time.localtime : nil, :Organization => sched.org_name }
      has_org = true if sched.org_name
    }
    rows = rows.sort_by {|map|
      map[:Name]
    }

    puts cmd_render_table(rows, :fields => (has_org ? [:Organization] : [])+[:Name, :Cron, :Timezone, :"Next schedule", :Delay, :Priority, :Result, :Database, :Query], :max_width=>500)
  end

  def sched_create(op)
    org = nil
    db_name = nil
    timezone = nil
    delay = 0
    result_url = nil
    result_user = nil
    result_ask_password = false
    priority = nil
    retry_limit = nil

    op.on('-g', '--org ORGANIZATION', "create the schedule under this organization") {|s|
      org = s
    }
    op.on('-d', '--database DB_NAME', 'use the database (required)') {|s|
      db_name = s
    }
    op.on('-t', '--timezone TZ', 'name of the timezone (like Asia/Tokyo)') {|s|
      timezone = s
    }
    op.on('-D', '--delay SECONDS', 'delay time of the schedule', Integer) {|i|
      delay = i
    }
    op.on('-r', '--result RESULT_URL', 'write result to the URL (see also result:create subcommand)') {|s|
      result_url = s
    }
    op.on('-u', '--user NAME', 'set user name for the result URL') {|s|
      result_user = s
    }
    op.on('-p', '--password', 'ask password for the result URL') {|s|
      result_ask_password = true
    }
    op.on('-P', '--priority PRIORITY', 'set priority') {|s|
      priority = job_priority_id_of(s)
      unless priority
        raise "unknown priority #{s.inspect} should be -2 (very-low), -1 (low), 0 (normal), 1 (high) or 2 (very-high)"
      end
    }
    op.on('-R', '--retry COUNT', 'automatic retrying count', Integer) {|i|
      retry_limit = i
    }

    name, cron, sql = op.cmd_parse

    unless db_name
      $stderr.puts "-d, --database DB_NAME option is required."
      exit 1
    end

    if result_url
      require 'td/command/result'
      result_url = build_result_url(result_url, result_user, result_ask_password)
    end

    client = get_client

    # local existance check
    get_database(client, db_name)

    begin
      first_time = client.create_schedule(name, :cron=>cron, :query=>sql, :database=>db_name, :result=>result_url, :timezone=>timezone, :delay=>delay, :priority=>priority, :retry_limit=>retry_limit, :organization=>org)
    rescue AlreadyExistsError
      cmd_debug_error $!
      $stderr.puts "Schedule '#{name}' already exists."
      exit 1
    end

    $stderr.puts "Schedule '#{name}' is created. It starts at #{first_time.localtime}."
  end

  def sched_delete(op)
    name = op.cmd_parse

    client = get_client

    begin
      client.delete_schedule(name)
    rescue NotFoundError
      cmd_debug_error $!
      $stderr.puts "Schedule '#{name}' does not exist."
      $stderr.puts "Use '#{$prog} sched:list' to show list of the schedules."
      exit 1
    end

    $stderr.puts "Schedule '#{name}' is deleted."
  end

  def sched_update(op)
    cron = nil
    sql = nil
    db_name = nil
    result = nil
    timezone = nil
    delay = nil
    priority = nil
    retry_limit = nil

    op.on('-s', '--schedule CRON', 'change the schedule') {|s|
      cron = s
    }
    op.on('-q', '--query SQL', 'change the query') {|s|
      sql = s
    }
    op.on('-d', '--database DB_NAME', 'change the database') {|s|
      db_name = s
    }
    op.on('-r', '--result RESULT_TABLE', 'change the result table') {|s|
      result = s
    }
    op.on('-t', '--timezone TZ', 'change the name of the timezone (like Asia/Tokyo)') {|s|
      timezone = s
    }
    op.on('-D', '--delay SECONDS', 'change the delay time of the schedule', Integer) {|i|
      delay = i
    }
    op.on('-P', '--priority PRIORITY', 'set priority') {|s|
      priority = job_priority_id_of(s)
      unless priority
        raise "unknown priority #{s.inspect} should be -2 (very-low), -1 (low), 0 (normal), 1 (high) or 2 (very-high)"
      end
    }
    op.on('-R', '--retry COUNT', 'automatic retrying count', Integer) {|i|
      retry_limit = i
    }


    name = op.cmd_parse

    params = {}
    params['cron'] = cron if cron
    params['query'] = sql if sql
    params['database'] = db_name if db_name
    params['result'] = result if result
    params['timezone'] = timezone if timezone
    params['delay'] = delay.to_s if delay
    params['priority'] = priority.to_s if priority
    params['retry_limit'] = retry_limit.to_s if retry_limit

    if params.empty?
      $stderr.puts op.to_s
      exit 1
    end

    client = get_client

    begin
      client.update_schedule(name, params)
    rescue NotFoundError
      cmd_debug_error $!
      $stderr.puts "Schedule '#{name}' does not exist."
      $stderr.puts "Use '#{$prog} sched:list' to show list of the schedules."
      exit 1
    end

    $stderr.puts "Schedule '#{name}' is updated."
  end

  def sched_history(op)
    require 'td/command/job'  # job_priority_name_of

    page = 0
    skip = 0

    op.on('-p', '--page PAGE', 'skip N pages', Integer) {|i|
      page = i
    }
    op.on('-s', '--skip N', 'skip N schedules', Integer) {|i|
      skip = i
    }

    name, max = op.cmd_parse

    max = (max || 20).to_i

    if page
      skip += max * page
    end

    client = get_client

    begin
      history = client.history(name, skip, skip+max-1)
    rescue NotFoundError
      cmd_debug_error $!
      $stderr.puts "Schedule '#{name}' does not exist."
      $stderr.puts "Use '#{$prog} sched:list' to show list of the schedules."
      exit 1
    end

    scheds = client.schedules
    if s = scheds.find {|s| s.name == name }
      puts "Organization : #{s.org_name}"
      puts "Name         : #{s.name}"
      puts "Cron         : #{s.cron}"
      puts "Timezone     : #{s.timezone}"
      puts "Delay        : #{s.delay} sec"
      puts "Next         : #{s.next_time}"
      puts "Result       : #{s.result_url}"
      puts "Priority     : #{job_priority_name_of(s.priority)}"
      puts "Retry limit  : #{s.retry_limit}"
      puts "Database     : #{s.database}"
      puts "Query        : #{s.query}"
    end

    rows = []
    history.each {|j|
      rows << {:Time => j.scheduled_at.localtime, :JobID => j.job_id, :Status => j.status, :Priority => job_priority_name_of(j.priority), :Result=>j.result_url}
    }

    puts cmd_render_table(rows, :fields => [:JobID, :Time, :Status, :Priority, :Result])
  end

  def sched_run(op)
    num = 1

    op.on('-n', '--num N', 'number of jobs to run', Integer) {|i|
      num = i
    }

    name, time = op.cmd_parse

    if time.to_i.to_s == time.to_s
      # UNIX time
      t = Time.at(time.to_i)
    else
      require 'time'
      begin
        t = Time.parse(time)
      rescue
        $stderr.puts "invalid time format: #{time}"
        exit 1
      end
    end

    client = get_client

    begin
      jobs = client.run_schedule(name, t.to_i, num)
    rescue NotFoundError
      cmd_debug_error $!
      $stderr.puts "Schedule '#{name}' does not exist."
      $stderr.puts "Use '#{$prog} sched:list' to show list of the schedules."
      exit 1
    end

    rows = []
    jobs.each_with_index {|job,i|
      rows << {:JobID => job.job_id, :Time => job.scheduled_at ? job.scheduled_at.localtime : nil}
    }

    $stderr.puts "Scheduled #{num} jobs from #{t}."
    puts cmd_render_table(rows, :fields => [:JobID, :Time], :max_width=>500)
  end

end
end
