
module TRD
module Command

  def server_status
    op = cmd_opt 'server-status'
    op.cmd_parse

    require 'trd/api'
    puts API.server_status
  end

end
end

