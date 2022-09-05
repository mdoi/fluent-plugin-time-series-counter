require 'fluent/plugin/output'

class Fluent::Plugin::TimeSeriesCounter < Fluent::Plugin::Output
  Fluent::Plugin.register_output('time_series_counter', self)
  helpers :event_emitter

  unless method_defined?(:log)
    define_method('log') { $log }
  end

  config_param :tag, :string, default: "tsc"
  config_param :count_key, :string, default: nil
  config_param :count_key_delimiter, :string, default: ":"
  config_param :count_value_name, :string, default: "count"
  config_param :unit, :string, default: nil
  config_param :uniq_key, :string, default: "tsc_key"
  config_param :unit_key, :string, default: "tsc_unit"
  config_param :time_key, :string, default: "tsc_time"

  def configure(conf)
    super

    if !count_key
      raise Fluent::ConfigError, "out_time_series_counter: required 'count_key' parameter."
    end

    if !unit
      raise Fluent::ConfigError, "out_time_series_counter: required 'unit' parameter."
    end

    @count_keys = count_key.split(/\s*,\s*/).sort
    @units = unit.split(/\s*,\s*/).inject({}) do |hash, i|
      hash[i] = true
      hash
    end
  end

  def formatted_to_msgpack_binary?
    true
  end


  def format(tag, time, record)
    [tag, time, record].to_msgpack
  end

  def write(chunk)
    stats = {}

    chunk.msgpack_each do |tag, time, record|
      skip = false

      @count_keys.each do |k|
        # skip record if a record does not have requried count_keys
        skip = true unless record[k]
      end
      next if skip

      if @units['min']
        count(stats, record, time, "min")
      end

      if @units['hour']
        count(stats, record, time, "hour")
      end

      if @units['day']
        count(stats, record, time, "day")
      end
    end
    output_stats(stats)
  end

  private
  def create_uniq_key(record, unit, time)
    uniq_key = []
    @count_keys.each do |k|
      uniq_key << record[k]
    end
    uniq_key << time.to_s
    uniq_key << unit
    uniq_key.join(@count_key_delimiter)
  end

  def count(stats, record, time, unit)
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

    tsc_key = create_uniq_key(record, unit, unit_time)
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
      router.emit(@tag, Fluent::Engine.now, v)
    end
  end
end
