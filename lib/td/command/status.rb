
module TreasureData
module Command

  def status(op)
    op.cmd_parse

    client = get_client

    # +----------------+
    # |     scheds     |
    # +----------------+
    # +----------------+
    # |      jobs      |
    # +----------------+
    # +------+ +-------+
    # |tables| |results|
    # +------+ +-------+

    scheds = []
    jobs = []
    tables = []
    results = []

    s = client.schedules
    s.each {|sched|
      scheds << {:Name => sched.name, :Cron => sched.cron, :Result => sched.result_url, :Query => sched.query}
    }
    scheds = scheds.sort_by {|map|
      map[:Name]
    }
    x1, y1 = status_render(0, 0, "[Schedules]", scheds, :fields => [:Name, :Cron, :Result, :Query])

    j = client.jobs(0, 4)
    j.each {|job|
      start = job.start_at
      elapsed = Command.humanize_elapsed_time(start, job.end_at)
      jobs << {:JobID => job.job_id, :Status => job.status, :Query => job.query.to_s, :Start => (start ? start.localtime : ''), :Elapsed => elapsed, :Result => job.result_url}
    }
    x2, y2 = status_render(0, 0, "[Jobs]", jobs, :fields => [:JobID, :Status, :Start, :Elapsed, :Result, :Query])

    dbs = client.databases
    dbs.map {|db|
      db.tables.each {|table|
        tables << {:Database => db.name, :Table => table.name, :Count => table.count.to_s, :Size => table.estimated_storage_size_string}
      }
    }
    x3, y3 = status_render(0, 0, "[Tables]", tables, :fields => [:Database, :Table, :Count, :Size])

    rs = client.results
    rs.each {|r|
      results << {:Name => r.name, :URL => r.url}
    }
    results = results.sort_by {|map|
      map[:Name]
    }
    x4, y4 = status_render(x3+2, y3, "[Results]", results, :fields => [:Name, :URL])

    (y3-y4-1).times do
      $stdout.print "\eD"
    end
    $stdout.print "\eE"
  end

  private
  def status_render(movex, movey, msg, *args)
    lines = cmd_render_table(*args).split("\n")
    lines.pop  # remove 'N rows in set' line
    lines.unshift(msg)
    #lines.unshift("")

    $stdout.print "\e[#{movey}A" if movey > 0

    max_width = 0
    height = 0
    lines.each {|line|
      $stdout.print "\e[#{movex}C" if movex > 0
      $stdout.puts line
      width = line.length
      max_width = width if max_width < width
      height += 1
    }

    return movex+max_width, height
  end

end # module Command
end # module TreasureData

