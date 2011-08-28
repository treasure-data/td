
module TreasureData
module Command

  def help
    op = get_option('help')

    cmd = op.cmd_parse

    ARGV.clear
    ARGV[0] = '--help'

    method = List.get_method(cmd)
    unless method
      $stderr.puts "'#{cmd}' is not a td command. Run '#{$prog}' to show the list."
      List.show_guess(cmd)
      exit 1
    end

    method.call
  end

end
end
