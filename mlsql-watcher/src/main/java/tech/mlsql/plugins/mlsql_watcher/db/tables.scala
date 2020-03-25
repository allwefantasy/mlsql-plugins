package tech.mlsql.plugins.mlsql_watcher.db

case class WExecutor(id: Int, name: String,
                     hostPort: String,
                     totalShuffleRead: Long,
                     totalShuffleWrite: Long,
                     addTime: Long,
                     removeTime: Long)

case class WJob(id: Int, groupId: String, executorName: String,
                diskBytesSpilled: Long,
                shuffleRemoteBytesRead: Long,
                shuffleLocalBytesRead: Long,
                shuffleRecordsRead: Long,
                shuffleBytesWritten: Long,
                shuffleRecordsWritten: Long,
                addTime: Long
               )
