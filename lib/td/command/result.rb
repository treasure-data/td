
module TreasureData
module Command

  def result_show(op)
    name = op.cmd_parse

    client = get_client

    rs = client.results
    r = rs.find {|r| name == r.name }

    unless r
      $stderr.puts "Result URL '#{name}' does not exist."
      $stderr.puts "Use '#{$prog} result:create #{name} <URL>' to create the URL."
      exit 1
    end

    puts "Organization : #{r.org_name}"
    puts "Name         : #{r.name}"
    puts "URL          : #{r.url}"
  end

  def result_list(op)
    op.cmd_parse

    client = get_client

    rs = client.results

    rows = []
    has_org = false
    rs.each {|r|
      rows << {:Name => r.name, :URL => r.url, :Organization => r.org_name}
      has_org = true if r.org_name
    }
    rows = rows.sort_by {|map|
      map[:Name]
    }

    puts cmd_render_table(rows, :fields => (has_org ? [:Organization] : [])+[:Name, :URL])

    if rs.empty?
      $stderr.puts "There are no result URLs."
      $stderr.puts "Use '#{$prog} result:create <name> <url>' to create a result URL."
    end
  end

  def result_create(op)
    org = nil
    result_user = nil
    result_ask_password = false

    op.on('-g', '--org ORGANIZATION', "create the database under this organization") {|s|
      org = s
    }
    op.on('-u', '--user NAME', 'set user name for authentication') {|s|
      result_user = s
    }
    op.on('-p', '--password', 'ask password for authentication') {|s|
      result_ask_password = true
    }

    name, url = op.cmd_parse

    API.validate_database_name(name)

    client = get_client

    url = build_result_url(url, result_user, result_ask_password)

    opts = {}
    opts['organization'] = org if org
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
        print "Password (typing will be hidden): "
        password = STDIN.gets || ""
        password = password[0..-2]  # strip \n
      rescue Interrupt
        $stderr.print "\ncanceled."
        exit 1
      ensure
        system "stty echo"   # TODO termios
        print "\n"
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
end
end

