require "td/updater"

module TreasureData
module Command

  def update(op)
    if TreasureData::Updater.disable
      $stderr.puts "update is for only TD toolbelt"
      exit
    end

    $stderr.puts "Updating from #{TreasureData::VERSION}"
    if new_version = TreasureData::Updater.update
      $stderr.puts "updated to #{new_version}"
    else
      $stderr.puts "nothing to update"
    end
  end

end
end

