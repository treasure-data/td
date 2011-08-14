
module TreasureData
module Command

  def server_status
    op = cmd_opt 'server-status'
    op.cmd_parse

    require 'td/api'
    puts API.server_status
  end

end
end

