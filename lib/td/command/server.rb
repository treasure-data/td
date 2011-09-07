
module TreasureData
module Command

  def server_status(op)
    op.cmd_parse

    puts Client.server_status
  end

end
end

