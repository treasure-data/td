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
    $stdout.puts "Updating 'td' from #{TOOLBELT_VERSION}..."
    if new_version = Updater.update
      $stdout.puts "Successfully updated to #{new_version} in #{Command.humanize_time((Time.now - start_time).to_i)}."
    else
      $stdout.puts "Nothing to update."
    end
  end

end # module Command
end # module TreasureData

