
module TreasureData
module Command

  def result_show(op)
    name = op.cmd_parse
    client = get_client

    rs = client.results
    r = rs.find {|r| name == r.name }

    unless r
      $stderr.puts "Result URL '#{name}' does not exist."
      $stderr.puts "Use '#{$prog} " + Config.cl_options_string + "result:create #{name} <URL>' to create the URL."
      exit 1
    end

    $stdout.puts "Name : #{r.name}"
    $stdout.puts "URL  : #{r.url}"
  end

  def result_list(op)
    set_render_format_option(op)

    op.cmd_parse

    client = get_client

    rs = client.results

    rows = []
    rs.each {|r|
      rows << {:Name => r.name, :URL => r.url}
    }
    rows = rows.sort_by {|map|
      map[:Name]
    }

    $stdout.puts cmd_render_table(rows, :fields => [:Name, :URL], :render_format => op.render_format)

    if rs.empty?
      $stderr.puts "There are no result URLs."
      $stderr.puts "Use '#{$prog} " + Config.cl_options_string + "result:create <name> <url>' to create a result URL."
    end
  end

  def result_create(op)
    result_user = nil
    result_ask_password = false

    op.on('-u', '--user NAME', 'set user name for authentication') {|s|
      result_user = s
    }
    op.on('-p', '--password', 'ask password for authentication') {|s|
      result_ask_password = true
    }

    name, url = op.cmd_parse
    API.validate_result_set_name(name)

    client = get_client

    url = build_result_url(url, result_user, result_ask_password)

    opts = {}
    begin
      client.create_result(name, url, opts)
    rescue AlreadyExistsError
      $stderr.puts "Result URL '#{name}' already exists."
      exit 1
    end

    $stderr.puts "Result URL '#{name}' is created."
  end

  def result_delete(op)
    name = op.cmd_parse

    client = get_client

    begin
      client.delete_result(name)
    rescue NotFoundError
      $stderr.puts "Result URL '#{name}' does not exist."
      exit 1
    end

    $stderr.puts "Result URL '#{name}' is deleted."
  end

  private
  def build_result_url(url, user, ask_password)
    if ask_password
      begin
        system "stty -echo"  # TODO termios
        $stdout.print "Password (typing will be hidden): "
        password = STDIN.gets || ""
        password = password[0..-2]  # strip \n
      rescue Interrupt
        $stderr.print "\ncanceled."
        exit 1
      ensure
        system "stty echo"   # TODO termios
        $stdout.print "\n"
      end
    end

    ups = nil
    if user && password
      require 'cgi'
      ups = "#{CGI.escape(user)}:#{CGI.escape(password)}@"
    elsif user
      require 'cgi'
      ups = "#{CGI.escape(user)}@"
    elsif password
      require 'cgi'
      ups = ":#{CGI.escape(password)}@"
    end
    if ups
      url = url.sub(/\A([\w]+:(?:\/\/)?)/, "\\1#{ups}")
    end

    url
  end

  # DEPRECATED: relying on API server side validation which will return
  #             immediately after query submission with error code 422.
  private
  def validate_td_result_url(url)
    re = /td:\/\/[^@]*@\/(.*)\/([^?]+)/
    match = re.match(url)
    if match.nil?
      raise ParameterConfigurationError, "Treasure Data result output invalid URL format"
    end
    dbs = match[1]
    tbl = match[2]
    begin
      API.validate_name("Treasure Data result output destination database", 3, 256, dbs)
      API.validate_name("Treasure Data result output destination table", 3, 256, tbl)
    rescue ParameterValidationError => e
      raise ParameterConfigurationError, e
    end
  end
end
end

