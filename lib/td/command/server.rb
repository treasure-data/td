
module TreasureData
module Command

  def server_status
    op = get_option('server:status')

    op.cmd_parse

    puts Client.server_status
  end

end
end

