module Fluent
  class TimeSeriesCounter < Fluent::BufferedOutput
    Fluent::Plugin.register_output('time_series_counter', self)

    unless method_defined?(:log)
      define_method('log') { $log }
    end

    config_param :tag, :string, :default => "tsc"
    config_param :count_key, :string, :default => nil
    config_param :count_key_delimiter, :string, :default => ":"
    config_param :count_value_name, :string, :default => "count"
    config_param :unit, :string, :default => nil
    config_param :uniq_key, :string, :default => "tsc_key"
    config_param :unit_key, :string, :default => "tsc_unit"
    config_param :time_key, :string, :default => "tsc_time"
    config_param :add_key_prefix, :string, :default => nil

    def initialize
      super
    end

    def configure(conf)
      super

      if !count_key
        raise ConfigError, "out_time_series_counter: required 'count_key' parameter."
      end

      if !unit
        raise ConfigError, "out_time_series_counter: required 'unit' parameter."
      end

      @count_keys = count_key.split(/\s*,\s*/).sort
      @unit = unit.split(/\s*,\s*/).inject({}) do |hash, i|
        hash[i] = true
        hash
      end
    end

    def format(tag, time, record)
      [tag, time, record].to_msgpack
    end

    def write(chunk)
      stats = {}
      chunk.msgpack_each do |tag, time, record|
        skip = false
        next unless time
        @count_keys.each do |k|
          # skip record if a record does not have requried count_keys
          skip = true unless record[k]
        end
        next if skip

        if @unit['min']
          count(stats, record, time, "min")
        end

        if @unit['hour']
          count(stats, record, time, "hour")
        end

        if @unit['day']
          count(stats, record, time, "day")
        end
      end

      output_stats(stats)
    end

    private
    def create_uniq_key(record, time)
      uniq_key = []

      if @add_key_prefix
        uniq_key << @add_key_prefix
      end

      @count_keys.each do |k|
        uniq_key << record[k]
      end

      uniq_key << time.to_s
      uniq_key.join(@count_key_delimiter)
    end

    def count(stats, record, time, unit)
      unix_time = 0
      case unit
      when "min"
        unit_time = time - (time % 60)
      when "hour"
        unit_time = time - (time % 3600)
      when "day"
        unit_time = time - (time % 86400)
      else
        return
      end
      tsc_key = create_uniq_key(record, unit_time)
      unless stats[tsc_key]
        stats[tsc_key] = {@count_value_name => 0} unless stats[tsc_key]
        @count_keys.each do |k|
          stats[tsc_key][k] = record[k]
        end
        stats[tsc_key][@unit_key] = unit
        stats[tsc_key][@time_key] = unit_time
      end
      stats[tsc_key][@count_value_name] += 1
    end

    def output_stats(stats)
      stats.each do |k, v|
        v[@uniq_key] = k
        Fluent::Engine.emit("#{@tag}", Fluent::Engine.now, v)
      end
    end
  end
end
