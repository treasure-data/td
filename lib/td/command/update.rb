require "td/updater"

module TreasureData
module Command

  def update(op)
    # for gem installation, this command is disallowed -
    #   it only works for the toolbelt.
    if Updater.disable?
      $stderr.puts Updater.disable_message
      exit
    end

    start_time = Time.now
    puts "Updating 'td' from #{TOOLBELT_VERSION}..."
    if new_version = Updater.update
      total_time = Time.now - start_time
      puts "Successully updated to #{new_version} in #{total_time}."
    else
      puts "Nothing to update."
    end
  end

end
end

