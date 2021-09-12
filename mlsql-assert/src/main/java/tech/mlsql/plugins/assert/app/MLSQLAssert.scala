package tech.mlsql.plugins.assert.app

import tech.mlsql.common.utils.log.Logging
import tech.mlsql.dsl.CommandCollection
import tech.mlsql.ets.register.ETRegister
import tech.mlsql.plugins.assert.ets.{Assert, MLSQLThrow}
import tech.mlsql.version.VersionCompatibility

/**
 * 4/6/2021 WilliamZhu(allwefantasy@gmail.com)
 */
class MLSQLAssert extends tech.mlsql.app.App with VersionCompatibility with Logging {
  override def run(args: Seq[String]): Unit = {
    ETRegister.register("Assert", classOf[Assert].getName)
    ETRegister.register("Throw", classOf[MLSQLThrow].getName)
    CommandCollection.refreshCommandMapping(Map("assert" ->
      """
        |run command as Assert.`` where parameters='''{:all}'''
        |""".stripMargin))
    CommandCollection.refreshCommandMapping(Map("throw" ->
      """
        |run command as Throw.`` where msg='''{0}'''
        |""".stripMargin))
  }


  override def supportedVersions: Seq[String] = {
    MLSQLAssert.versions
  }
}

object MLSQLAssert {
  val versions = Seq("2.1.0", "2.1.0-SNAPSHOT", "2.0.0", "2.0.1")
}