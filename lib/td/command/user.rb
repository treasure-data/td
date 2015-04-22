
module TreasureData
module Command

  def user_show(op)
    name = op.cmd_parse

    client = get_client

    users = client.users
    user = users.find {|user| name == user.name }
    unless user
      $stderr.puts "User '#{name}' does not exist."
      $stderr.puts "Use '#{$prog} " + Config.cl_options_string + "user:create <name>' to create an user."
      exit 1
    end

    $stderr.puts "Name  : #{user.name}"
    $stderr.puts "Email : #{user.email}"
  end

  def user_list(op)
    set_render_format_option(op)

    op.cmd_parse

    client = get_client

    users = client.users

    rows = []
    users.each {|user|
      rows << {:Name => user.name, :Email => user.email}
    }

    $stdout.puts cmd_render_table(rows, :fields => [:Name, :Email], :render_format => op.render_format)

    if rows.empty?
      $stderr.puts "There are no users."
      $stderr.puts "Use '#{$prog} " + Config.cl_options_string + "user:create <name>' to create an users."
    end
  end

  def user_create(op)
    email = nil
    random_password = false

    op.on('-e', '--email EMAIL', "Use this email address to identify the user") {|s|
      email = s
    }

    op.on('-R', '--random-password', "Generate random password", TrueClass) {|b|
      random_password = b
    }

    name = op.cmd_parse

    unless email
      $stderr.puts "-e, --email EMAIL option is required."
      exit 1
    end

    if random_password
      lower = ('a'..'z').to_a
      upper = ('A'..'Z').to_a
      digit = ('0'..'9').to_a
      symbol = %w[_ @ - + ;]

      r = []
      3.times { r << lower.sort_by{rand}.first }
      3.times { r << upper.sort_by{rand}.first }
      2.times { r << digit.sort_by{rand}.first }
      1.times { r << symbol.sort_by{rand}.first }
      password = r.sort_by{rand}.join

      $stdout.puts "Password: #{password}"

    else
      3.times do
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

        if password.empty?
          $stderr.puts "canceled."
          exit 0
        end

        begin
          system "stty -echo"  # TODO termios
          $stdout.print "Retype password: "
          password2 = STDIN.gets || ""
          password2 = password2[0..-2]  # strip \n
        rescue Interrupt
          $stderr.print "\ncanceled."
          exit 1
        ensure
          system "stty echo"   # TODO termios
          $stdout.print "\n"
        end

        if password == password2
          break
        end

        $stdout.puts "Doesn't match."
      end
    end

    client = get_client(:ssl => true)
    client.add_user(name, nil, email, password)

    $stderr.puts "User '#{name}' is created."
    $stderr.puts "Use '#{$prog} " + Config.cl_options_string + "user:apikeys #{name}' to show the API key."
  end

  def user_delete(op)
    name = op.cmd_parse

    client = get_client

    client.remove_user(name)

    $stderr.puts "User '#{name}' is deleted."
  end

  ## TODO user:email:change <name> <email>
  #def user_email_change(op)
  #end

  def user_apikey_add(op)
    name = op.cmd_parse

    client = get_client

    begin
      client.add_apikey(name)
    rescue TreasureData::NotFoundError
      $stderr.puts "User '#{name}' does not exist."
      $stderr.puts "Use '#{$prog} " + Config.cl_options_string + "users' to show users."
      exit 1
    end

    $stderr.puts "Added an API key to user '#{name}'."
    $stderr.puts "Use '#{$prog} " + Config.cl_options_string + "user:apikeys #{name}' to show the API key"
  end

  def user_apikey_remove(op)
    name, key = op.cmd_parse

    client = get_client

    begin
      client.remove_apikey(name, key)
    rescue TreasureData::NotFoundError
      $stderr.puts "User '#{name}' or API key '#{key}' does not exist."
      $stderr.puts "Use '#{$prog} " + Config.cl_options_string + "users' to show users."
      $stderr.puts "Use '#{$prog} " + Config.cl_options_string + "user:apikeys '#{key}' to show API keys"
      exit 1
    end

    $stderr.puts "Removed an an API key from user '#{name}'."
  end

  def user_apikey_list(op)
    set_render_format_option(op)

    name = op.cmd_parse

    client = get_client

    keys = client.list_apikeys(name)

    rows = []
    keys.each {|key|
      rows << {:Key => key}
    }

    $stdout.puts cmd_render_table(rows, :fields => [:Key], :render_format => op.render_format)
  end

  def user_password_change(op)
    name = op.cmd_parse

    password = nil

    3.times do
      begin
        system "stty -echo"  # TODO termios
        $stdout.print "New password (typing will be hidden): "
        password = STDIN.gets || ""
        password = password[0..-2]  # strip \n
      rescue Interrupt
        $stderr.print "\ncanceled."
        exit 1
      ensure
        system "stty echo"   # TODO termios
        $stdout.print "\n"
      end

      if password.empty?
        $stderr.puts "canceled."
        exit 0
      end

      begin
        system "stty -echo"  # TODO termios
        $stdout.print "Retype new password: "
        password2 = STDIN.gets || ""
        password2 = password2[0..-2]  # strip \n
      rescue Interrupt
        $stderr.print "\ncanceled."
        exit 1
      ensure
        system "stty echo"   # TODO termios
        $stdout.print "\n"
      end

      if password == password2
        break
      end

      $stdout.puts "Doesn't match."
    end

    client = get_client(:ssl => true)

    client.change_password(name, password)

    $stderr.puts "Password of user '#{name}' changed."
  end

end
end

