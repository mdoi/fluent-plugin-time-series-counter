require 'helper'

# Load the plugin (Required)
require 'fluent/plugin/out_file'

class TimeSeriesCounterTest < Test::Unit::TestCase
  include Fluent::Test::Helpers

  def setup
    Fluent::Test.setup   # Setup test for Fluentd (Required)
  end

  CONFIG = %[
    count_key id
    unit hour
  ]

  def create_driver(conf = CONFIG)
    Fluent::Test::Driver::Output.new(Fluent::TimeSeriesCounter).configure(conf)
  end

  sub_test_case 'configuration' do
    test 'basic configuration' do
      d = create_driver
      assert_equal('id', d.instance.count_key)
      assert_equal({'hour' => true}, d.instance.unit)
    end
  end

  sub_test_case 'count' do
    test 'count per count_key' do
      d = create_driver
      d.run(default_tag: 'test') do
        d.feed({'id' => 1})
        d.feed({'id' => 1})
        d.feed({'id' => 2})
      end

      # result == uniq(count_key)
      assert_equal(2, d.events.size)

      results = d.events.flat_map{|_key, _time, result| result }

      result = results.find{ |r| r["id"] == 1 }
      assert_equal(2, result["count"])

      result = results.find{ |r| r["id"] == 2 }
      assert_equal(1, result["count"])
    end

    test 'count per unit(hour)' do
      d = create_driver

      d.run(default_tag: 'test') do
        d.feed(event_time('2022-09-01 10:00:00 UTC'), {'id' => 1})
        d.feed(event_time('2022-09-01 10:00:00 UTC'), {'id' => 1})

        d.feed(event_time('2022-09-01 11:00:00 UTC'), {'id' => 1})
      end

      # result == uniq(count_key)
      assert_equal(2, d.events.size)

      results = d.events.flat_map{|_key, _time, result| result }

      # Time.at(1662026400) => 2022-09-01 10:00:00 UTC
      result = results.find{ |r| r["tsc_time"] == 1662026400 }
      assert_equal(2, result["count"])

      # Time.at(1662030000) => 2022-09-01 11:00:00 UTC
      result = results.find{ |r| r["tsc_time"] == 1662030000 }
      assert_equal(1, result["count"])
    end

    test 'count only feeds with all keys' do
      d = create_driver(
        <<~EOS
          count_key a,b,c
          unit hour
        EOS
      )

      d.run(default_tag: 'test') do
        # no count
        d.feed({a: 1})
        d.feed({a: 1, b: 2})
        d.feed({a: 1, b: 2})

        # count
        d.feed({a: 1, b: 2, c: 3})
        d.feed({a: 1, b: 2, c: 3})
        d.feed({a: 1, b: 2, c: 3})
      end

      assert_equal(1, d.events.size)

      _key, _time, record = d.events.first

      assert_equal(3, record['count'])
    end

    test 'count per units' do
      d = create_driver(
        <<~EOS
          count_key id
          unit min,hour,day
        EOS
      )

      d.run(default_tag: 'test') do
        d.feed(event_time('2022-09-01 10:00:00 UTC'), {'id' => 1})
        d.feed(event_time('2022-09-01 10:01:00 UTC'), {'id' => 1})

        d.feed(event_time('2022-09-01 11:00:00 UTC'), {'id' => 1})

        d.feed(event_time('2022-09-02 10:00:00 UTC'), {'id' => 1})
        d.feed(event_time('2022-09-02 10:00:00 UTC'), {'id' => 1})
        d.feed(event_time('2022-09-02 10:00:00 UTC'), {'id' => 1})

        d.feed(event_time('2022-09-01 10:00:00 UTC'), {'id' => 2})
      end

      results = d.events.map{|_,_,r| r}

      assert_equal(4, results.count{|r| r['id'] == 1 && r['tsc_unit'] == 'min'})
      assert_equal(3, results.count{|r| r['id'] == 1 && r['tsc_unit'] == 'hour'})
      assert_equal(2, results.count{|r| r['id'] == 1 && r['tsc_unit'] == 'day'})

      assert_equal(1, results.count{|r| r['id'] == 2 && r['tsc_unit'] == 'min'})
      assert_equal(1, results.count{|r| r['id'] == 2 && r['tsc_unit'] == 'hour'})
      assert_equal(1, results.count{|r| r['id'] == 2 && r['tsc_unit'] == 'day'})
    end
  end
end
