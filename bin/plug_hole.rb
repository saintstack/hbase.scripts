# Plug hole in a table by adding a new region that spans the gap.
#
#  ${HBASE_HOME}/bin/hbase org.jruby.Main plug_hole.rb
#

#
# Copyright 2010 The Apache Software Foundation
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
#

include Java
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.HConstants
import org.apache.hadoop.hbase.HRegionInfo
import org.apache.hadoop.hbase.client.HTable
import org.apache.hadoop.hbase.client.HBaseAdmin
import org.apache.hadoop.hbase.client.Delete
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.client.Scan
import org.apache.hadoop.hbase.HTableDescriptor
import org.apache.hadoop.hbase.HRegionInfo
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.util.FSUtils
import org.apache.hadoop.hbase.util.Writables
import org.apache.hadoop.fs.Path
import org.apache.hadoop.fs.FileSystem
import org.apache.commons.logging.LogFactory

# Name of this script
NAME = "plug_hole"

# Print usage for this script
def usage
  puts 'Usage: %s.rb TABLENAME STARTROW ENDROW' % NAME
  puts 'WARNING: Its critical that STARTROW and ENDROW are actual region start keys' % NAME
  exit!
end

# Get configuration to use.
c = HBaseConfiguration.new()

# Set hadoop filesystem configuration using the hbase.rootdir.
# Otherwise, we'll always use localhost though the hbase.rootdir
# might be pointing at hdfs location.
c.set("fs.default.name", c.get(HConstants::HBASE_DIR))

# Get a logger and a metautils instance.
LOG = LogFactory.getLog(NAME)

# Check arguments
if ARGV.size != 3
  usage
end

# Get tablename and bounding keys from cmdline args.
tableName = HTableDescriptor.isLegalTableName(ARGV[0].to_java_bytes)
startkey = ARGV[1].to_java_bytes
endkey = ARGV[2].to_java_bytes

# Get reference to the meta table
metaTable = HTable.new(c, HConstants::META_TABLE_NAME)
metastartkey = HRegionInfo.createRegionName(tableName, startkey, HConstants::ZEROES)
metaendkey = HRegionInfo.createRegionName(tableName, endkey, HConstants::ZEROES)
# Check that we don't already have something in the gap.
scan = Scan.new()
scanner = metaTable.getScanner(scan)
hri = nil
tablehri = nil
while (result = scanner.next())
  hri = Writables.getHRegionInfo(result.getValue(HConstants::CATALOG_FAMILY, HConstants::REGIONINFO_QUALIFIER)) 
  next unless Bytes.equals(hri.getTableDesc().getName(), tableName)
  tablehri = hri unless tablehri
  rowid = result.getRow()
  next if Bytes.compareTo(rowid, metastartkey) < 0
  break if Bytes.compareTo(metaendkey, rowid) <= 0
  # If a region of same tablename and startkey already, then fail.
  if Bytes.equals(hri.getTableDesc().getName(), tableName) and \
      Bytes.equals(hri.getStartKey(), startkey)
    raise IOError.new("Already a region in the gap: " + hri.toString())
  end
end
scanner.close()

# If we got here, then go ahead and plug hole.
hole = HRegionInfo.new(tablehri.getTableDesc(), startkey, endkey)  
p = Put.new(hole.getRegionName())
p.add(HConstants::CATALOG_FAMILY, HConstants::REGIONINFO_QUALIFIER, Writables.getBytes(hole)) 
metaTable.put(p)
LOG.info("Added hole-plugging region=" + hole.toString)
