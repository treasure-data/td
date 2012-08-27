
module TreasureData
  class FileReader

    class MessagePackParsingReader
      def initialize(io, error, opts)
        require 'msgpack'
        @io = io
        @error = error
        @u = MessagePack::Unpacker.new(@io)
      end

      def next
        @u.next
      end
    end

    class LineReader
      def initialize(io, error, opts)
        if encoding = opts[:encoding]
          io.external_encoding = encoding
        end
        #@delimiter = opts[:line_delimiter_expr] || /\r?\n/
        @io = io
        @error = error
      end

      def next_row
        @io.readline($/).chomp
      end
    end

    class DelimiterParser
      def initialize(reader, error, opts)
        @reader = reader
        @delimiter_expr = opts[:delimiter_expr]
        @null_expr = opts[:null_expr]
      end

      def next
        row = @reader.next_row
        array = row.split(@delimiter_expr)
        array.map! {|x|
          @null_expr =~ x ? nil : x
        }
      end
    end

    # TODO
    #class QuotedDelimiterParsingReader
    #  def initialize(io, error, opts)
    #    require 'strscan'
    #    @io = io
    #    @error = error
    #    @delimiter_expr = opts[:delimiter_expr]
    #    @quote_char = opts[:quote_char]
    #    @escape_char = opts[:escape_char]
    #  end

    #  def next
    #  end
    #end

    class JSONParser
      def initialize(reader, error, opts)
        @reader = reader
        @error = error
      end

      def next
        while true
          line = @reader.next_row
          begin
            return JSON.parse(line)
          rescue
            @error.call("invalid json format: #{$!}", line)
            next
          end
        end
      end
    end

    # TODO
    #class ApacheParser
    #  REGEXP = /^([^ ]*) [^ ]* ([^ ]*) \[([^\]]*)\] "(\S+)(?: +([^ ]*) +\S*)?" ([^ ]*) ([^ ]*)(?: "([^\"]*)" "([^\"]*)")?$/
    #
    #  def initialize(reader, error, opts)
    #    @reader = reader
    #  end
    #
    #  def next
    #    while true
    #      m = REGEXP.match(@reader.next_row)
    #      if m
    #        h = {
    #          'host' => m[1],
    #          'user' => m[2],
    #          'time' => m[3],
    #          'method' => m[4],
    #          'path' => m[5],
    #          'code' => m[6],
    #          'size' => m[7].to_i,
    #          'referer' => m[8],
    #          'agent' => m[9],
    #        }
    #        return h
    #      end
    #    end
    #  end
    #end

    class AutoTypeConvertParserFilter
      def initialize(parser, error)
        @parser = parser
      end

      def next
        array = @parser.next
        array.map! {|s|
          # nil.to_i == 0 != nil.to_s
          i = s.to_i
          i.to_s == s ? i : s
        }
      end
    end

    class HashBuilder
      def initialize(parser, error, columns)
        @parser = parser
        @columns = columns
      end

      def next
        array = @parser.next
        Hash[@columns.zip(array)]
      end
    end

    class TimeParserFilter
      def initialize(parser, error, opts)
        require 'time'
        @parser = parser
        @error = error
        @time_column = opts[:time_column]
        unless @time_column
          raise '-t, --time-column NAME option is required'
        end
        @time_format = opts[:time_format]
      end

      def next
        while true
          row = @parser.next
          tval = row[@time_column]

          unless tval
            @error.call("time column '#{@time_column}' is missing", row)
            next
          end

          begin
            if tf = @time_format
              row['time'] = parse_time(tval, tf).to_i
            elsif tval.is_a?(Integer)
                row['time'] = tval
            else
              row['time'] = Time.parse(tval).to_i
            end
            return row

          rescue
            @error.call("invalid time format '#{tval}': #{$!}", row)
            next
          end
        end
      end

      if Time.respond_to?(:strptime)
        def parse_time(value, format)
          Time.strptime(value, format)
        end
      else
        def parse_time(value, format)
          Time.parse(DateTime.strptime(value, format).to_s)
        end
      end
    end

    def initialize
      @format = "text"
      @default_opts = {
        :delimiter_expr => /\t|,/,
        #:line_delimiter_expr => /\r?\n/,
        :null_expr => /\A(?:\\N|\-|)\z/,
        #:quote_char => "\"",
      }
      @opts = {}
      @parser_class = nil
    end

    attr_reader :default_opts, :opts
    attr_accessor :parser_class

    def init_optparse(op)
      op.on('-f', '--format NAME', "source file format [csv, tsv, msgpack, json]") {|s|
        set_format_template(s)
      }
      op.on('-h', '--columns NAME,NAME,...', 'column names (use --column-header instead if the first line has column names)') {|s|
        @opts[:column_names] = s.split(',')
      }
      op.on('-H', '--column-header', 'first line includes column names', TrueClass) {|b|
        @opts[:column_header] = b
      }
      op.on('-d', '--delimiter REGEX', "delimiter between columns (default: #{@default_opts[:delimiter_expr].inspect[1..-2]})") {|s|
        @opts[:delimiter_expr] = Regexp.new(s)
      }
      #op.on('-D', '--line-delimiter REGEX', "delimiter between rows (default: #{@default_opts[:line_delimiter_expr].inspect[1..-2]})") {|s|
      #  @opts[:line_delimiter_expr] = Regexp.new(s)
      #}
      op.on('-N', '--null REGEX', "null expression (default: #{@default_opts[:null_expr].inspect[1..-2]})") {|s|
        @opts[:null_expr] = Regexp.new(s)
      }
      # TODO
      #op.on('-E', '--escape CHAR', "escape character (default: no escape character)") {|s|
      #  @opts[:escape_char] = s
      #}
      #op.on('-Q', '--quote CHAR', "quote character (default: #{@default_opts[:quote_char]}") {|s|
      #  @opts[:quote_char] = s
      #}
      op.on('-S', '--all-string', 'disable automatic type conversion', TrueClass) {|b|
        @opts[:all_string] = b
      }
      op.on('-t', '--time-column NAME', 'name of the time column (default: auto detect)') {|s|
        @opts[:time_column] = s
      }
      op.on('-T', '--time-format FORMAT', 'strftime(3) format of the time column') {|s|
        @opts[:time_format] = s
      }
      op.on('-e', '--encoding NAME', "text encoding") {|s|
        @opts[:encoding] = s
      }
      op.on('-C', '--compress NAME', 'compression format name [plain, gzip] (default: auto detect)') {|s|
        @opts[:compress] = s
      }
    end

    def set_format_template(name)
      case name
      when 'csv'
        @format = 'text'
        @opts[:delimiter_expr] = /,/
      when 'tsv'
        @format = 'text'
        @opts[:delimiter_expr] = /\t/
      #when 'apache'
      #  @format = 'apache'
      #  @opts[:column_names] = ['host', 'user', 'time', 'method', 'path', 'code', 'size', 'referer', 'agent']
      #  @opts[:null_expr] = /\A(?:\-|)\z/
      #  @opts[:time_column] = 'time'
      #  @opts[:time_format] = '%d/%b/%Y:%H:%M:%S %z'
      when 'msgpack'
        @format = 'msgpack'
      when 'json'
        @format = 'json'
      else
        raise "Unknown format: #{name}"
      end
    end

    def compose_factory
      opts = @default_opts.merge(@opts)
      case @format
      when 'text'
        Proc.new {|io,error|
          reader = LineReader.new(io, error, opts)
          parser = DelimiterParser.new(reader, error, opts)
          if opts[:column_header]
            column_names = parser.next
          elsif opts[:column_names]
            column_names = opts[:column_names]
          else
            raise "--column-header or --columns option is required"
          end
          unless opts[:all_string]
            parser = AutoTypeConvertParserFilter.new(parser, error)
          end
          parser = HashBuilder.new(parser, error, column_names)
          parser = TimeParserFilter.new(parser, error, opts)
        }

      #when 'apache'

      when 'json'
        Proc.new {|io,error|
          reader = LineReader.new(io, error, opts)
          parser = JSONParser.new(reader, error, opts)
          if opts[:column_header]
            column_names = parser.next
          elsif opts[:column_names]
            column_names = opts[:column_names]
          end
          if column_names
            parser = HashBuilder.new(parser, error, column_names)
          end
          parser = TimeParserFilter.new(parser, error, opts)
        }

      when 'msgpack'
        Proc.new {|io,error|
          parser = MessagePackParsingReader.new(io, error, opts)
          if opts[:column_header]
            column_names = parser.next
          elsif opts[:column_names]
            column_names = opts[:column_names]
          end
          if column_names
            parser = HashBuilder.new(parser, error, column_names)
          end
          parser = TimeParserFilter.new(parser, error, opts)
        }
      end
    end

    def parse(io, error, &block)
      factory = compose_factory
      parser = factory.call(io, error)
      begin
        while record = parser.next
          block.call(record)
        end
      rescue EOFError
      end
    end

  end
end
