package tech.mlsql.plugins.shell.app

import tech.mlsql.common.utils.log.Logging
import tech.mlsql.dsl.CommandCollection
import tech.mlsql.ets.register.ETRegister
import tech.mlsql.plugins.shell.ets.{CopyFromLocal, ShellExecute}
import tech.mlsql.version.VersionCompatibility

/**
 * 2/6/2021 WilliamZhu(allwefantasy@gmail.com)
 */
class MLSQLShell extends tech.mlsql.app.App with VersionCompatibility with Logging {
  override def run(args: Seq[String]): Unit = {
    ETRegister.register("ShellExecute", classOf[ShellExecute].getName)
    CommandCollection.refreshCommandMapping(Map("sh" ->
      """
        |run command as ShellExecute.`` where parameters='''{:all}'''
        |""".stripMargin))

    ETRegister.register("CopyFromLocal", classOf[CopyFromLocal].getName)
    CommandCollection.refreshCommandMapping(Map("copyFromLocal" ->
      """
        |run command as CopyFromLocal.`{1}` where src="{0}"
        |""".stripMargin))
  }


  override def supportedVersions: Seq[String] = {
    MLSQLShell.versions
  }
}

object MLSQLShell {
  val versions = Seq("2.1.0", "2.1.0-SNAPSHOT", "2.0.0", "2.0.1")
}
