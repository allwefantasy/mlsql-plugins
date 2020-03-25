package tech.mlsql.plugins.mlsql_watcher

import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.{Executors, TimeUnit}

import com.google.common.util.concurrent.ThreadFactoryBuilder
import org.apache.spark.DataCompute
import tech.mlsql.common.utils.log.Logging
import tech.mlsql.plugins.mlsql_watcher.db.PluginDB.ctx
import tech.mlsql.plugins.mlsql_watcher.db.PluginDB.ctx._
import tech.mlsql.plugins.mlsql_watcher.db.{CustomDBWrapper, WExecutor, WJob}
import tech.mlsql.runtime.AppRuntimeStore

/**
 * 10/3/2020 WilliamZhu(allwefantasy@gmail.com)
 */
object SnapshotTimer extends Logging {


  private val timer =
    Executors.newSingleThreadScheduledExecutor(
      new ThreadFactoryBuilder()
        .setDaemon(true).setNameFormat(getClass.getSimpleName + "-%d").build())


  private[this] val snapshotSaver = new Runnable {
    override def run(): Unit = {
      if (AppRuntimeStore.store.store.count(classOf[CustomDBWrapper]) == 0) {
        return
      }
      val (executorItems, jobItems) = DataCompute.compute()
      logInfo("save to db....")
      try {
        ctx.run(liftQuery(executorItems).foreach(item => query[WExecutor].insert(item)))
        ctx.run(liftQuery(jobItems).foreach(item => query[WJob].insert(item)))
      } catch {
        case e: Exception =>
          logError("Fail to save data", e)
      }

    }
  }

  final val isRun = new AtomicBoolean(false)


  def start(): Unit = {
    synchronized {
      if (!isRun.get()) {
        logInfo(s"Scheduler MLSQL state every 3 seconds")
        timer.scheduleAtFixedRate(snapshotSaver, 10, 5, TimeUnit.SECONDS)
        isRun.set(true)
      }
    }
  }

  def stop(): Unit = {
    logInfo("Shut down MLSQL state scheduler")
    timer.shutdown()
  }
}
