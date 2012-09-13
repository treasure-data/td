
module TreasureData
module Command

  def plan_status(op)
    db_name, table_name = op.cmd_parse

    client = get_client

    a = client.account

    $stderr.puts "Storage:  #{a.storage_size_string}"
  end

end
end
