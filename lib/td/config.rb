
module TreasureData


class ConfigError < StandardError
end

class ConfigNotFoundError < ConfigError
end

class ConfigParseError < ConfigError
end


class Config
  def initialize
    @path = nil
    @conf = {}   # section.key = val
  end

  def self.read(path=Config.path, create=false)
    new.read(path)
  end

  def [](cate_key)
    @conf[cate_key]
  end

  def []=(cate_key, val)
    @conf[cate_key] = val
  end

  def delete(cate_key)
    @conf.delete(cate_key)
  end

  def read(path=@path)
    @path = path
    begin
      data = File.read(@path)
    rescue
      e = ConfigNotFoundError.new($!.to_s)
      e.set_backtrace($!.backtrace)
      raise e
    end

    section = ""

    data.each_line {|line|
      line.strip!
      case line
      when /^#/
        next
      when /\[(.+)\]/
        section = $~[1]
      when /^(\w+)\s*=\s*(.+?)\s*$/
        key = $~[1]
        val = $~[2]
        @conf["#{section}.#{key}"] = val
      else
        raise ConfigParseError, "invalid config line '#{line}' at #{@path}"
      end
    }

    self
  end

  def save(path=@path||Config.path)
    @path = path
    write
  end

  private
  def write
    require 'fileutils'
    FileUtils.mkdir_p File.dirname(@path)
    File.open(@path, "w") {|f|
      @conf.keys.map {|cate_key|
        cate_key.split('.',2)
      }.zip(@conf.values).group_by {|(section,key),val|
        section
      }.each {|section,cate_key_vals|
        f.puts "[#{section}]"
        cate_key_vals.each {|(section,key),val|
          f.puts "  #{key} = #{val}"
        }
      }
    }
  end

  @@path = ENV['TREASURE_DATA_CONFIG_PATH'] || ENV['TD_CONFIG_PATH'] || File.join(ENV['HOME'], '.td', 'td.conf')
  @@apikey = ENV['TREASURE_DATA_API_KEY'] || ENV['TD_API_KEY']
  @@apikey = nil if @@apikey == ""
  @@secure = true

  def self.path
    @@path
  end

  def self.path=(path)
    @@path = path
  end

  def self.apikey
    @@apikey || Config.read['account.apikey']
  end

  def self.apikey=(apikey)
    @@apikey = apikey
  end

  def self.secure
    @@secure
  end

  def self.secure=(secure)
    @@secure = secure
  end
end


end
