
module TD
module Command

  def account
    op = cmd_opt 'account', :user_name?

    op.banner << "\noptions:\n"

    force = false
    op.on('-f', '--force', 'overwrite current setting', TrueClass) {|b|
      force = true
    }

    user_name = op.cmd_parse

    require 'td/config'
    conf = nil
    begin
      conf = Config.read
    rescue ConfigError
    end
    if conf && conf['account.user']
      unless force
        $stderr.puts "TreasureData account is already configured with '#{conf['account.user']}' account."
        $stderr.puts "Add '-f' option to overwrite this setting."
        exit 0
      end
    end

    unless user_name
      print "User name: "
      line = STDIN.gets || ""
      user_name = line.strip
    end

    if user_name.empty?
      $stderr.puts "Canceled."
      exit 0
    end

    api = nil

    2.times do
      begin
        system "stty -echo"  # TODO termios
        print "Password: "
        password = STDIN.gets || ""
        password = password[0..-2]  # strip \n
      ensure
        system "stty echo"   # TODO termios
        print "\n"
      end

      if password.empty?
        $stderr.puts "Canceled."
        exit 0
      end

      require 'td/api'

      begin
        api = API.authenticate(user_name, password)
      rescue TD::AuthError
        $stderr.puts "User name or password mismatched."
      end

      break if api
    end
    return unless api

    $stderr.puts "Authenticated successfully."

    conf ||= Config.new
    conf["account.user"] = user_name
    conf["account.apikey"] = api.apikey
    conf.save

    $stderr.puts "Use '#{$prog} create-database <db_name>' to create a database."
  end

end
end

