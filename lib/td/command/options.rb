module TreasureData
module Command
module Options

  def job_show_options(op)
    verbose = nil
    wait = false
    output = nil
    format = nil
    render_opts = {:header => false}
    limit = nil
    exclude = false

    op.on('-v', '--verbose', 'show logs', TrueClass) {|b|
      verbose = b
    }
    op.on('-w', '--wait', 'wait for finishing the job', TrueClass) {|b|
      wait = b
    }
    op.on('-G', '--vertical', 'use vertical table to show results', TrueClass) {|b|
      render_opts[:vertical] = b
    }
    op.on('-o', '--output PATH', 'write result to the file') {|s|
      unless Dir.exist?(File.dirname(s))
        s = File.expand_path(s)
      end
      output = s
      format = 'tsv' if format.nil?
    }
    op.on('-f', '--format FORMAT', 'format of the result to write to the file (tsv, csv, json, msgpack, and msgpack.gz)') {|s|
      unless ['tsv', 'csv', 'json', 'msgpack', 'msgpack.gz'].include?(s)
        raise "Unknown format #{s.dump}. Supported formats are: tsv, csv, json, msgpack, and msgpack.gz"
      end
      format = s
    }
    op.on('-l', '--limit ROWS', 'limit the number of result rows shown when not outputting to file') {|s|
      unless s.to_i > 0
        raise "Invalid limit number. Must be a positive integer"
      end
      limit = s.to_i
    }
    op.on('-c', '--column-header', 'output of the columns\' header when the schema is available',
                                   '  for the table (only applies to tsv and csv formats)', TrueClass) {|b|
      render_opts[:header] = b;
    }
    op.on('-x', '--exclude', 'do not automatically retrieve the job result', TrueClass) {|b|
      exclude = b
    }

    op.on('--null STRING', "null expression in csv or tsv") {|s|
      render_opts[:null_expr] = s.to_s
    }

    [op,verbose, wait, output, format, render_opts, limit, exclude]
  end

end # module Options
end # module Command
end # module TrasureData
