#!/usr/bin/env ruby

require 'rubygems'
require 'bundler/setup'
require 'influxdb'
require 'json'
require 'time'

DATABASE = 'home'
NAME = 'weather'

TAG_FIELDS = ['model', 'channel', 'id', 'brand', 'msg_type']
VALUE_FIELDS = ['temperature_C', 'humidity', 'speed', 'direction_deg', 'direction_str', 'gust', 'rain', 'battery']
KNOWN_MODELS = [
  'Fine Offset Electronics WH1080/WH3080 Weather Station',
  'THGR122N',
]

db = InfluxDB::Client.new(DATABASE)

ARGF.each_line do |line|
  data = JSON.parse(line)
  next unless KNOWN_MODELS.include?(data['model'])
  record = {
    tags: {},
    timestamp: Time.strptime(data['time'], '%Y-%m-%d %H:%M:%S').to_i,
    values: {},
  }
  TAG_FIELDS.each { |tag| record[:tags][tag] = data[tag] unless data[tag].nil? }
  VALUE_FIELDS.each { |val| record[:values][val] = data[val] unless data[val].nil? }
  next if record[:values].empty?
  puts record
  db.write_point(NAME, record)
end
