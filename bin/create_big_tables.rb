# Copyright 2011 The Apache Software Foundation
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


# Script to create N tables of M regions.  To run it, do:
#
#   ./bin/hbase org.jruby.Main THIS_SCRIPT.rb
#

require 'optparse'
include Java
import org.apache.hadoop.hbase.HConstants
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.HBaseAdmin
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.HTableDescriptor
import org.apache.hadoop.hbase.HColumnDescriptor
import org.apache.hadoop.conf.Configuration
import org.apache.commons.logging.Log
import org.apache.commons.logging.LogFactory

# Name of this script
NAME = "create_big_tables"

# Create a logger and disable the DEBUG-level annoying client logging
def configureLogging(options)
  apacheLogger = LogFactory.getLog(NAME)
  # Configure log4j to not spew so much
  unless (options[:debug]) 
    logger = org.apache.log4j.Logger.getLogger("org.apache.hadoop.hbase.client")
    logger.setLevel(org.apache.log4j.Level::INFO)
  end
  return apacheLogger
end

# Get configuration instance
def getConfiguration()
  config = HBaseConfiguration.create()
  # No prefetching on .META.
  config.setInt("hbase.client.prefetch.limit", 1)
  # Make a config that retries at short intervals many times
  config.setInt("hbase.client.pause", 500)
  config.setInt("hbase.client.retries.number", 100)
  return config
end

# Create tables.
def create(options, prefix)
  # Get configuration
  config = getConfiguration()
  # Get an admin instance
  admin = HBaseAdmin.new(config) 
  index = 0
  $LOG.info("Creating " + options[:tables].to_s + " tables with " + options[:regions].to_s + " regions each; prefix=" + prefix)
  while index < options[:tables]
    htd = HTableDescriptor.new(prefix + $DELIMITER + index.to_s)
    $LOG.info("Creating table=" + htd.getNameAsString() + ", " + index.to_s + " of " + options[:tables].to_s)
    htd.addFamily(HColumnDescriptor.new("family"))
    admin.createTable(htd, Bytes.toBytes("0000000000"), Bytes.toBytes("zzzzzzzzzz"), options[:regions].to_i);
    index = index + 1
  end
end

# Drop tables that match prefix.
def drop(options, prefix)
  # Get configuration
  config = getConfiguration()
  # Get an admin instance
  admin = HBaseAdmin.new(config) 
  tables = admin.listTables()
  for t in tables
    if t.getNameAsString().start_with?(prefix + $DELIMITER)
      admin.disableTable(t.getNameAsString())
      admin.deleteTable(t.getNameAsString())
    end
  end
end

# Do command-line parsing
options = {}
optparse = OptionParser.new do |opts|
  opts.banner = "Usage: #{NAME}.rb [options] <prefix>"
  opts.separator 'Create tables with passed <prefix>'
  options[:drop] = false
  opts.on('-o', '--drop', 'Drop tables that match the passed prefix') do |count|
    options[:drop] = true
  end
  options[:tables] = 10
  opts.on('-t', '--tables=COUNT', 'How many tables to make; default=10') do |count|
    options[:tables] = count
  end
  options[:regions] = 10
  opts.on('-r', '--regions=COUNT', 'How many regions to make per table; default=10') do |count|
    options[:regions] = count
  end
  opts.on('-h', '--help', 'Display usage information') do
    puts opts
    exit
  end
  options[:debug] = false
  opts.on('-d', '--debug', 'Display extra debug logging') do
    options[:debug] = true
  end
end
optparse.parse!

# Check ARGVs
if ARGV.length < 1
  puts optparse
  exit 1
end
# Some globals
$LOG = configureLogging(options) 
$DELIMITER = '_'
prefix = ARGV[0]
if options[:drop]
  drop(options, prefix)
else
  create(options, prefix)
end
