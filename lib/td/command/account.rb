require 'td/helpers'

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
        print "Password (typing will be hidden): "
        password = get_password
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

  def account_usage(op)
    op.cmd_parse

    client = get_client

    a = client.account

    $stderr.puts "Storage:  #{a.storage_size_string}"
  end

  private

  if Helpers.on_windows?
    require 'Win32API'

    def get_char
      Win32API.new('msvcrt', '_getch', [], 'L').Call
    rescue Exception
      Win32API.new('crtdll', '_getch', [], 'L').Call
    end

    def get_password
      password = ''

      while c = get_char
        break if c == 13 || c == 10 # CR or NL
        if c == 127 || c == 8  # 128: backspace, 8: delete
          password.slice!(-1, 1)
        else
          password << c.chr
        end
      end

      password
    end
  else
    def get_password
      system "stty -echo"  # TODO termios
      password = STDIN.gets || ""
      password[0..-2]  # strip \n
    end
  end
end
end

