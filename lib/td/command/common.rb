
module TreasureData

autoload :API, 'td/client/api'
autoload :Client, 'td/client'
autoload :Database, 'td/client'
autoload :Table, 'td/client'
autoload :Schema, 'td/client'
autoload :Job, 'td/client'

module Command

  private
  def initialize
    @render_indent = ''
  end

  def get_client(opts={})
    unless opts.has_key?(:ssl)
      opts[:ssl] = Config.secure
    end
    apikey = Config.apikey
    unless apikey
      raise ConfigError, "Account is not configured."
    end
    Client.new(apikey, opts)
  end

  def get_ssl_client(opts={})
    opts[:ssl] = true
    get_client(opts)
  end

  def cmd_render_table(rows, *opts)
    require 'hirb'
    Hirb::Helpers::Table.render(rows, *opts)
  end

  def gen_table_fields(has_org, fields)
    if has_org
      fields.unshift(:Organization)
    else
      fields
    end
  end

  #def cmd_render_tree(nodes, *opts)
  #  require 'hirb'
  #  Hirb::Helpers::Tree.render(nodes, *opts)
  #end

  def cmd_debug_error(ex)
    if $verbose
      $stderr.puts "error: #{$!.class}: #{$!.to_s}"
      $!.backtrace.each {|b|
        $stderr.puts "  #{b}"
      }
        $stderr.puts ""
    end
  end

  def cmd_format_elapsed(start, finish)
    if start
      if !finish
        finish = Time.now.utc
      end
      e = finish.to_i - start.to_i
      elapsed = ''
      if e >= 3600
        elapsed << "#{e/3600}h "
        e %= 3600
        elapsed << "%2dm " % (e/60)
        e %= 60
        elapsed << "%2dsec" % e
      elsif e >= 60
        elapsed << "%2dm " % (e/60)
        e %= 60
        elapsed << "%2dsec" % e
      else
        elapsed << "%2dsec" % e
      end
    else
      elapsed = ''
    end
    elapsed = "% 13s" % elapsed  # right aligned
  end

  def get_database(client, db_name)
    begin
      return client.database(db_name)
    rescue
      cmd_debug_error $!
      $stderr.puts $!
      $stderr.puts "Use '#{$prog} database:list' to show the list of databases."
      exit 1
    end
    db
  end

  def get_table(client, db_name, table_name)
    db = get_database(client, db_name)
    begin
      table = db.table(table_name)
    rescue
      $stderr.puts $!
      $stderr.puts "Use '#{$prog} table:list #{db_name}' to show the list of tables."
      exit 1
    end
    #if type && table.type != type
    #  $stderr.puts "Table '#{db_name}.#{table_name} is not a #{type} table but a #{table.type} table"
    #end
    table
  end

  def ask_password(max=3, &block)
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

      yield password
    end
  end

end
end
