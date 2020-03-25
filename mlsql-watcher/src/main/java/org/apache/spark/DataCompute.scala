package org.apache.spark


import _root_.streaming.core.strategy.platform.{PlatformManager, SparkRuntime}
import tech.mlsql.plugins.mlsql_watcher.db.{WExecutor, WJob}
import scala.collection.JavaConverters._

/**
 * 10/3/2020 WilliamZhu(allwefantasy@gmail.com)
 */
object DataCompute {
  def runtime = PlatformManager.getRuntime.asInstanceOf[SparkRuntime]
  def compute()={
    val statusStore = runtime.sparkSession.sparkContext.statusStore
    val executorItems = statusStore.executorList(false).map { item =>
      WExecutor(
        id = -1,
        name = item.id.toString,
        hostPort = item.hostPort,
        totalShuffleRead = item.totalShuffleRead,
        totalShuffleWrite = item.totalShuffleWrite,
        addTime = item.addTime.getTime,
        removeTime = item.removeTime.map(_.getTime).getOrElse(-1L)
      )
    }.toList



    val jobItems = statusStore.jobsList(new java.util.ArrayList[JobExecutionStatus]()).map { item =>
      val groupId = item.jobGroup.getOrElse("NONE")
      var diskBytesSpilled = 0L
      var shuffleRemoteBytesRead = 0L
      var shuffleLocalBytesRead = 0L
      var shuffleRecordsRead = 0L
      var shuffleBytesWritten = 0L
      var shuffleRecordsWritten = 0L
      val addTime = item.submissionTime.map(f => f.getTime).getOrElse(-1L)
      val executors = new java.util.HashSet[String]()
      item.stageIds.map { id =>
        statusStore.stageData(id).foreach {
          stage =>
            stage.tasks.foreach { tasks =>
              tasks.foreach { task =>
                val taskData = task._2
                if (taskData.taskMetrics.isDefined) {
                  diskBytesSpilled += taskData.taskMetrics.get.diskBytesSpilled
                  shuffleRemoteBytesRead += taskData.taskMetrics.get.shuffleReadMetrics.remoteBytesRead
                  shuffleLocalBytesRead += taskData.taskMetrics.get.shuffleReadMetrics.localBytesRead
                  shuffleRecordsRead += taskData.taskMetrics.get.shuffleReadMetrics.recordsRead
                  shuffleBytesWritten += taskData.taskMetrics.get.shuffleWriteMetrics.bytesWritten
                  shuffleRecordsWritten += taskData.taskMetrics.get.shuffleWriteMetrics.recordsWritten
                  executors.add(taskData.executorId)
                }
              }
            }
        }
      }
      WJob(-1, groupId, executors.asScala.toList.mkString(","), diskBytesSpilled, shuffleLocalBytesRead, shuffleLocalBytesRead, shuffleRecordsRead, shuffleBytesWritten, shuffleRecordsWritten, addTime)

    }.toList
    (executorItems,jobItems)
  }
}
