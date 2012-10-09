
module TreasureData
module Command

  def user_show(op)
    name = op.cmd_parse

    client = get_client

    users = client.users
    user = users.find {|user| name == user.name }
    unless user
      $stderr.puts "User '#{name}' does not exist."
      $stderr.puts "Use '#{$prog} user:create <name>' to create an user."
      exit 1
    end

    $stderr.puts "Name         : #{user.name}"
    $stderr.puts "Organization : #{user.org_name}"
    $stderr.puts "Email        : #{user.email}"
    $stderr.puts "Roles        : #{user.role_names.join(', ')}"
  end

  def user_list(op)
    op.cmd_parse

    client = get_client

    users = client.users

    rows = []
    users.each {|user|
      rows << {:Name => user.name, :Organization => user.org_name, :Email => user.email, :Roles => user.role_names.join(',')}
    }

    puts cmd_render_table(rows, :fields => [:Name, :Organization, :Email, :Roles])

    if rows.empty?
      $stderr.puts "There are no users."
      $stderr.puts "Use '#{$prog} user:create <name>' to create an users."
    end
  end

  def user_create(op)
    org = nil
    email = nil
    random_password = false
    create_org = nil

    op.on('-g', '--org ORGANIZATION', "create the user under this organization") {|s|
      org = s
    }

    op.on('-G', "create the user under the a new organization", TrueClass) {|b|
      create_org = b
    }

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

      puts "Password: #{password}"

    else
      3.times do
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

        if password.empty?
          $stderr.puts "canceled."
          exit 0
        end

        begin
          system "stty -echo"  # TODO termios
          print "Retype password: "
          password2 = STDIN.gets || ""
          password2 = password2[0..-2]  # strip \n
        rescue Interrupt
          $stderr.print "\ncanceled."
          exit 1
        ensure
          system "stty echo"   # TODO termios
          print "\n"
        end

        if password == password2
          break
        end

        puts "Doesn't match."
      end
    end

    client = get_client(:ssl => true)

    if create_org
      org ||= name
      client.create_organization(org)
    end

    ok = false
    begin
      client.add_user(name, org)

      begin
        client.change_email(name, email)
        client.change_password(name, password)
        client.add_apikey(name)
        ok = true

      ensure
        if !ok
          client.remove_user(name)
        end
      end

    ensure
      if create_org && !ok
        client.delete_organization(org)
      end
    end

    if create_org
      $stderr.puts "Organization '#{org}' and user '#{name}' are created."
    else
      $stderr.puts "User '#{name}' is created."
    end
    $stderr.puts "Use '#{$prog} user:apikeys #{name}' to show the API key."
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

  ## TODO user:apikey:add <name>
  #def user_apikey_add(op)
  #end

  ## TODO user:apikey:remove <name> <apikey>
  #def user_apikey_remove(op)
  #end

  def user_apikey_list(op)
    name = op.cmd_parse

    client = get_client

    keys = client.list_apikeys(name)

    rows = []
    keys.each {|key|
      rows << {:Key => key}
    }

    puts cmd_render_table(rows, :fields => [:Key])
  end

  def user_password_change(op)
    name = op.cmd_parse

    password = nil

    3.times do
      begin
        system "stty -echo"  # TODO termios
        print "New password (typing will be hidden): "
        password = STDIN.gets || ""
        password = password[0..-2]  # strip \n
      rescue Interrupt
        $stderr.print "\ncanceled."
        exit 1
      ensure
        system "stty echo"   # TODO termios
        print "\n"
      end

      if password.empty?
        $stderr.puts "canceled."
        exit 0
      end

      begin
        system "stty -echo"  # TODO termios
        print "Retype new password: "
        password2 = STDIN.gets || ""
        password2 = password2[0..-2]  # strip \n
      rescue Interrupt
        $stderr.print "\ncanceled."
        exit 1
      ensure
        system "stty echo"   # TODO termios
        print "\n"
      end

      if password == password2
        break
      end

      puts "Doesn't match."
    end

    client = get_client(:ssl => true)

    client.change_password(name, password)

    $stderr.puts "Password of user '#{name}' changed."
  end

end
end

