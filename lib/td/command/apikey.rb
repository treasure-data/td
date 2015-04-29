
module TreasureData
module Command

  def apikey_show(op)
    if Config.apikey
      $stdout.puts Config.apikey
      return
    end

    conf = nil
    begin
      conf = Config.read
    rescue ConfigError
    end

    if !conf || !conf['account.apikey']
      $stderr.puts "Account is not configured yet."
      $stderr.puts "Use '#{$prog} apikey:set' or '#{$prog} account' first."
      exit 1
    end

    $stdout.puts conf['account.apikey']
  end

  def apikey_set(op)
    op.banner << "\noptions:\n"

    force = false
    op.on('-f', '--force', 'overwrite current account setting', TrueClass) {|b|
      force = true
    }

    apikey = op.cmd_parse

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

    conf ||= Config.new
    conf.delete("account.user")
    conf["account.apikey"] = apikey
    conf.save

    $stdout.puts "API key is set."
    $stdout.puts "Use '#{$prog} db:create <db_name>' to create a database."
  end

end
end
