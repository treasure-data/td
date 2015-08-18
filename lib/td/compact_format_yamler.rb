require 'psych'

module TreasureData
  module CompactFormatYamler
    module Visitors
      class YAMLTree < Psych::Visitors::YAMLTree
        # NOTE support 2.0 following
        unless self.respond_to? :create
          class << self
            alias :create :new
          end
        end

        def visit_Hash o
          if o.class == ::Hash && o.values.all? {|v| v.kind_of?(Numeric) || v.kind_of?(String) || v.kind_of?(Symbol) }
            register(o, @emitter.start_mapping(nil, nil, true, Psych::Nodes::Mapping::FLOW))

            o.each do |k,v|
              accept k
              accept v
            end
            @emitter.end_mapping
          else
            super
          end
        end
      end
    end

    def self.dump(o, io = nil, options = {})
      if Hash === io
        options = io
        io = nil
      end

      visitor = ::TreasureData::CompactFormatYamler::Visitors::YAMLTree.create options
      visitor << o
      visitor.tree.yaml io, options
    end
  end
end
