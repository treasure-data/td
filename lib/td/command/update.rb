require "td/updater"

module TreasureData
module Command

  def update(op)
    if TreasureData::Updater.disable?
      $stderr.puts TreasureData::Updater.disable_message
      exit
    end

    $stderr.puts <<EOS
Updating started at #{Time.now}
from #{TOOLBELT_VERSION}
EOS
    if new_version = TreasureData::Updater.update
      $stderr.puts "updated to #{new_version}"
    else
      $stderr.puts "nothing to update"
    end
    $stderr.puts "ended at #{Time.now}"
  end

end
end

