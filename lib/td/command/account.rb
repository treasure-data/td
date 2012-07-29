
module TreasureData
module Command

  def account(op)
    op.banner << "\noptions:\n"

    force = false
    op.on('-f', '--force', 'overwrite current account setting', TrueClass) {|b|
      force = true
    }

    user_name = op.cmd_parse

    conf = nil
    begin
      conf = Config.read
    rescue ConfigError
    end
    if conf && conf['account.apikey']
      unless force
        if conf['account.user']
          $stderr.puts "Account is already configured with '#{conf['account.user']}' account."
        else
          $stderr.puts "Account is already configured."
        end
        $stderr.puts "Add '-f' option to overwrite."
        exit 0
      end
    end

    puts "Enter your Treasure Data credentials."
    unless user_name
      begin
        print "Email: "
        line = STDIN.gets || ""
        user_name = line.strip
      rescue Interrupt
        $stderr.puts "\ncanceled."
        exit 1
      end
    end

    if user_name.empty?
      $stderr.puts "canceled."
      exit 0
    end

    client = nil

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
        # enalbe SSL for the authentication
        client = Client.authenticate(user_name, password, :ssl=>true)
      rescue TreasureData::AuthError
        $stderr.puts "User name or password mismatched."
      end

      break if client
    end
    return unless client

    $stderr.puts "Authenticated successfully."

    conf ||= Config.new
    conf["account.user"] = user_name
    conf["account.apikey"] = client.apikey
    conf.save

    $stderr.puts "Use '#{$prog} db:create <db_name>' to create a database."
  end

end
end

