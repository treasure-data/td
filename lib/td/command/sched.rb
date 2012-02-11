
module TreasureData
module Command

  def sched_list(op)
    op.cmd_parse

    client = get_client

    scheds = client.schedules

    rows = []
    scheds.each {|sched|
      rows << {:Name => sched.name, :Cron => sched.cron, :Timezone => sched.timezone, :Delay => sched.delay, :Result => sched.rset_name, :Query => sched.query, :"Next schedule" => sched.next_time ? sched.next_time.localtime : nil }
    }
    rows = rows.sort_by {|map|
      map[:Name]
    }

    puts cmd_render_table(rows, :fields => [:Name, :Cron, :Timezone, :"Next schedule", :Delay, :Result, :Query], :max_width=>500)
  end

  def sched_show(op)
    name, max = op.cmd_parse

    client = get_client

    scheds = client.schedules

    s = scheds.find {|sched|
      sched.name == name
    }

    unless s
      $stderr.puts "Schedule '#{name}' does not exist."
      exit 1
    end

    puts "Name         : #{s.name}"
    puts "Cron         : #{s.cron}"
    puts "Timezone     : #{s.timezone}"
    puts "Delay        : #{s.delay}"
    puts "Next         : #{s.next_time}"
    puts "Result       : #{s.rset}"
    puts "Database     : #{s.database}"
    puts "Query        : #{s.query}"
  end

  def sched_create(op)
    db_name = nil
    result = nil
    timezone = nil
    delay = 0

    op.on('-d', '--database DB_NAME', 'use the database (required)') {|s|
      db_name = s
    }
    op.on('-r', '--result RESULT_TABLE', 'write result to the result table (use result:create command)') {|s|
      result = s
    }
    op.on('-t', '--timezone TZ', 'name of the timezone (like Asia/Tokyo)') {|s|
      timezone = s
    }
    op.on('-D', '--delay SECONDS', 'delay time of the schedule', Integer) {|i|
      delay = i
    }

    name, cron, sql = op.cmd_parse

    unless db_name
      $stderr.puts "-d, --database DB_NAME option is required."
      exit 1
    end

    client = get_client

    # local existance check
    get_database(client, db_name)

    begin
      first_time = client.create_schedule(name, :cron=>cron, :query=>sql, :database=>db_name, :result=>result, :timezone=>timezone, :delay=>delay)
    rescue AlreadyExistsError
      cmd_debug_error $!
      $stderr.puts "Schedule '#{name}' already exists."
      exit 1
    end

    $stderr.puts "Schedule '#{name}' is created. It starts at #{first_time}."
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

  def sched_history(op)
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

    rows = []
    history.each {|j|
      rows << {:Time => j.scheduled_at.localtime, :JobID => j.job_id, :Status => j.status, :Result=>j.rset_name}
    }

    puts cmd_render_table(rows, :fields => [:JobID, :Time, :Status, :Result])
  end

  def sched_run(op)
    num = 1

    op.on('-n', '--num N', 'number of jobs to run', Integer) {|i|
      num = i
    }

    name, time = op.cmd_parse

    if time.to_i.to_s == time.to_s
      # UNIX time
      time = time.to_i
    else
      begin
        time = Time.parse(time).to_i
      rescue
        $stderr.puts "invalid time format: #{time}"
      end
    end

    client = get_client

    begin
      client.run_schedule(name, time, num)
    rescue NotFoundError
      cmd_debug_error $!
      $stderr.puts "Schedule '#{name}' does not exist."
      $stderr.puts "Use '#{$prog} sched:list' to show list of the schedules."
    end

    puts "Scheduled #{num} jobs."
  end

end
end
