
module TreasureData
module Command

  def help(op)
    cmd = op.cmd_parse

    usage = List.cmd_usage(cmd)
    unless usage
      $stderr.puts "'#{cmd}' is not a td command. Run '#{$prog}' to show the list."
      List.show_guess(cmd)
      exit 1
    end

    puts usage
  end

  def help_all(op)
    cmd = op.cmd_parse

    TreasureData::Command::List.show_help(op.summary_indent)
    puts ""
    puts "Type '#{$prog} help COMMAND' for more information on a specific command."
  end

end
end
