# Script that removes regions from a table between passed keys. It first
# offlines all, then closes and deletes mention from .META.  You need
# to run this script multiple times because first it does all
# offlining and then it does the close and delete.  It won't start
# second 'Close and delete...' step till first is complete.  You'll know
# you are done with this script when you no longer see 'Closed and deleted..'
# messages in the output.  Don't worry if you see NPE exceptions in the
# output.  Can happen in midst of region state changes.
# 
# When done there will be a hole in the table.  Use another script to
# plug the hole, plug_hole.rb.
#
# To see usage for this script, run: 
#
#  ${HBASE_HOME}/bin/hbase org.jruby.Main excise_regions.rb
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
import org.apache.hadoop.hbase.client.Get
import org.apache.hadoop.hbase.HTableDescriptor
import org.apache.hadoop.hbase.HRegionInfo
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.util.FSUtils
import org.apache.hadoop.hbase.util.Writables
import org.apache.hadoop.fs.Path
import org.apache.hadoop.fs.FileSystem
import org.apache.commons.logging.LogFactory

# Name of this script
NAME = "excise_regions"

# Print usage for this script
def usage
  puts 'Usage: %s.rb TABLENAME STARTROW ENDROW ARCHIVE_TABLENAME' % NAME
  exit!
end

# Passed 'dir' exists and is a directory else exception
def isDirExists(fs, dir)
  raise IOError.new("Does not exit: " + dir.toString()) unless fs.exists(dir)
  raise IOError.new("Not a directory: " + dir.toString()) unless fs.isDirectory(dir)
end

# Get configuration to use.
c = HBaseConfiguration.new()

# Set hadoop filesystem configuration using the hbase.rootdir.
# Otherwise, we'll always use localhost though the hbase.rootdir
# might be pointing at hdfs location.
c.set("fs.default.name", c.get(HConstants::HBASE_DIR))
fs = FileSystem.get(c)

# Get a logger and a metautils instance.
LOG = LogFactory.getLog(NAME)

# Check arguments
if ARGV.size != 4
  usage
end

# Get tablename and bounding keys from cmdline args.
tableName = HTableDescriptor.isLegalTableName(ARGV[0].to_java_bytes)
archiveTableName = HTableDescriptor.isLegalTableName(ARGV[3].to_java_bytes)
startkey = ARGV[1].to_java_bytes
endkey = ARGV[2].to_java_bytes

# Check that table exists in fs
rootdir = FSUtils.getRootDir(c)
tableDir = fs.makeQualified(Path.new(rootdir, Bytes.toString(tableName)))
isDirExists(fs, tableDir)

# Check archive table exists
archiveTable = HTable.new(c, archiveTableName)
# Check its there w/ right schema by doing nonsense get against wanted family
g = Get.new(archiveTableName)
g.addFamily(HConstants::CATALOG_FAMILY)
archiveTable.get(g)


# Get reference to the meta table
metaTable = HTable.new(c, HConstants::META_TABLE_NAME)
scan = Scan.new()
scanner = metaTable.getScanner(scan)
metastartkey = HRegionInfo.createRegionName(tableName, startkey, HConstants::ZEROES)
metaendkey = HRegionInfo.createRegionName(tableName, endkey, HConstants::ZEROES)
# Collect offlined hris in here.
offlined_hris = []
online = false
first = true
while (result = scanner.next())
  rowid = result.getRow()
  next if Bytes.compareTo(rowid, metastartkey) < 0
  break if Bytes.compareTo(metaendkey, rowid) <= 0
  hri = Writables.getHRegionInfo(result.getValue(HConstants::CATALOG_FAMILY, HConstants::REGIONINFO_QUALIFIER)) 
  if hri.isOffline()
    # If offlined, add it to our list of offlined regions.
    offlined_hris << hri
  else
    # Don't offline the region we added -- the one that plugs the hole we made
    next if Bytes.equals(hri.getTableDesc().getName(), tableName) and \
        Bytes.equals(hri.getStartKey(), startkey) and Bytes.equals(hri.getEndKey(), endkey)
    # Offline this region. Set the something-is-still-online flag.
    online = true
    hri.setOffline(true)
    p = Put.new(rowid)
    p.add(HConstants::CATALOG_FAMILY, HConstants::REGIONINFO_QUALIFIER, Writables.getBytes(hri)) 
    metaTable.put(p)
    LOG.info("Offlined=" + hri.getRegionNameAsString())
  end
end
scanner.close()

# Now if there offlined regions and nothing is online, close and delete
unless online or offlined_hris.empty?
  admin = HBaseAdmin.new(c)
  for hri in offlined_hris
     admin.close_region(hri.getRegionName(), nil) 
     d = Delete.new(hri.getRegionName())
     metaTable.delete(d)
     LOG.info("Closed and deleted=" + hri.getRegionNameAsString())
     p = Put.new(hri.getRegionName())
     p.add(HConstants::CATALOG_FAMILY, HConstants::REGIONINFO_QUALIFIER, Writables.getBytes(hri)) 
     archiveTable.put(p)
  end
end
