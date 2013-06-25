/*
 * Copyright (C) 2012 The Regents of The University California.
 * All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package shark

import java.io._
import java.net.URLClassLoader
import java.util.ArrayList
import jline.{History, ConsoleReader}
import scala.collection.JavaConversions._

import org.apache.commons.lang.StringUtils
import org.apache.commons.logging.LogFactory
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hive.cli.{CliDriver, CliSessionState, OptionsProcessor}
import org.apache.hadoop.hive.common.LogUtils
import org.apache.hadoop.hive.common.LogUtils.LogInitializationException
import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hadoop.hive.ql.Driver
import org.apache.hadoop.hive.ql.exec.Utilities
import org.apache.hadoop.hive.ql.processors.{CommandProcessor, CommandProcessorFactory}
import org.apache.hadoop.hive.ql.session.SessionState
import org.apache.hadoop.hive.shims.ShimLoader
import org.apache.hadoop.io.IOUtils
import scala.Some
import org.apache.hadoop.hive.metastore.api.FieldSchema


trait DriverHelper extends LogHelper {
  val sessionState: Option[SessionState] = Option(SessionState.get())
  val conf: Configuration = if (!sessionState.isEmpty) sessionState.get.getConf else new Configuration()

  val out = sessionState.get.out
  val start:Long = System.currentTimeMillis()

  val LOG = LogFactory.getLog("CliDriver")
  val console: SessionState.LogHelper = new SessionState.LogHelper(LOG)

  def isRemote: Boolean = sessionState.asInstanceOf[CliSessionState].isRemoteMode

  def init() {
    SharkConfVars.initializeWithDefaults(conf)

    // Force initializing SharkEnv. This is put here but not object SharkCliDriver
    // because the Hive unit tests do not go through the main() code path.
    SharkEnv.init()
  }

  def recordTiming() {
    val end:Long = System.currentTimeMillis()
    if (end > start) {
      val timeTaken:Double = (end - start) / 1000.0
      console.printInfo("Time taken: %s seconds" format timeTaken)
    }
  }

  def printHeader(fieldSchemas: Option[List[FieldSchema]]) {
    // really should consider just returning the logging data
    // or async log it all
    if (HiveConf.getBoolVar(conf, HiveConf.ConfVars.HIVE_CLI_PRINT_HEADER)) {
      // Print the column names.
      fieldSchemas match {
        case Some(schemas) => out.println(schemas.map(_.getName).mkString("\t"))
        case _ => // do nothing
      }
    }
  }

  def log(action:() => Unit) {
    sessionState match {
      case Some(state) if(state.getIsVerbose) => action()
      case _ => /* Do Nothing */
    }
  }

  def setThreadContext() {
    if (System.getenv("TEST_WITH_ANT") == "1") {
      Thread.currentThread.setContextClassLoader(
        new URLClassLoader( Array(),
          Thread.currentThread.getContextClassLoader)
      )
    }
  }

  def getDriver(hiveConf: HiveConf, proc: CommandProcessor): Driver = {
    if (SharkConfVars.getVar(conf, SharkConfVars.EXEC_MODE) == "shark") {
      new SharkDriver(hiveConf)
    } else {
      proc.asInstanceOf[Driver]
    }
  }

  def runCommand(driver:Driver, commandInput: String): Option[Int] = {
    driver.run(commandInput).getResponseCode match {
      case resCode:Int if (resCode != 0) => {
        driver.close
        Some(resCode)
      }
      case _ => None
    }
  }

  def destroyDriver(driver: Driver) {
    recordTiming()
    // Destroy the driver to release all the locks.
    if (driver.isInstanceOf[SharkDriver]) driver.destroy()
  }
}

object SharkCliDriver {

  var prompt  = "shark"
  var prompt2 = "     " // when ';' is not yet seen.

  def main(args: Array[String]) {
    val hiveArgs = args.filterNot(_.equals("-loadRdds"))
    val loadRdds = hiveArgs.length < args.length
    val oproc = new OptionsProcessor()
    if (!oproc.process_stage1(hiveArgs)) {
      System.exit(1)
    }

    // NOTE: It is critical to do this here so that log4j is reinitialized
    // before any of the other core hive classes are loaded
    var logInitFailed = false
    var logInitDetailMessage: String = null
    try {
      logInitDetailMessage = LogUtils.initHiveLog4j()
    } catch {
      case e: LogInitializationException =>
        logInitFailed = true
        logInitDetailMessage = e.getMessage()
    }

    var ss = new CliSessionState(new HiveConf(classOf[SessionState]))
    ss.in = System.in
    try {
      ss.out = new PrintStream(System.out, true, "UTF-8")
      ss.info = new PrintStream(System.err, true, "UTF-8");
      ss.err = new PrintStream(System.err, true, "UTF-8")
    } catch {
      case e: UnsupportedEncodingException => System.exit(3)
    }

    if (!oproc.process_stage2(ss)) {
      System.exit(2)
    }

    if (!ss.getIsSilent()) {
      if (logInitFailed) System.err.println(logInitDetailMessage)
      else SessionState.getConsole().printInfo(logInitDetailMessage)
    }

    // Set all properties specified via command line.
    val conf: HiveConf = ss.getConf()
    ss.cmdProperties.entrySet().foreach { item: java.util.Map.Entry[Object, Object] =>
      conf.set(item.getKey().asInstanceOf[String], item.getValue().asInstanceOf[String])
      ss.getOverriddenConfigurations().put(
        item.getKey().asInstanceOf[String], item.getValue().asInstanceOf[String])
    }

    SessionState.start(ss)

    // Clean up after we exit
    Runtime.getRuntime().addShutdownHook(
      new Thread() {
        override def run() {
          SharkEnv.stop()
        }
      }
    )

    // "-h" option has been passed, so connect to Shark Server.
    if (ss.getHost() != null) {
      ss.connect()
      if (ss.isRemoteMode()) {
        prompt = "[" + ss.getHost + ':' + ss.getPort + "] " + prompt
        val spaces = Array.tabulate(prompt.length)(_ => ' ')
        prompt2 = new String(spaces)
      }
    }

    if (!ss.isRemoteMode() && !ShimLoader.getHadoopShims().usesJobShell()) {
      // Hadoop-20 and above - we need to augment classpath using hiveconf
      // components.
      // See also: code in ExecDriver.java
      var loader = conf.getClassLoader()
      val auxJars = HiveConf.getVar(conf, HiveConf.ConfVars.HIVEAUXJARS)
      if (StringUtils.isNotBlank(auxJars)) {
        loader = Utilities.addToClassPath(loader, StringUtils.split(auxJars, ","))
      }
      conf.setClassLoader(loader)
      Thread.currentThread().setContextClassLoader(loader)
    }

    var cli = new SharkCliDriver(loadRdds)
    cli.setHiveVariables(oproc.getHiveVariables())

    // Execute -i init files (always in silent mode)
    cli.processInitFiles(ss)

    if (ss.execString != null) {
      System.exit(cli.processLine(ss.execString))
    }

    try {
      if (ss.fileName != null) {
        System.exit(cli.processFile(ss.fileName))
      }
    } catch {
      case e: FileNotFoundException =>
        System.err.println("Could not open input file for reading. (" + e.getMessage() + ")")
        System.exit(3)
    }

    var reader = new ConsoleReader()
    reader.setBellEnabled(false)
    // reader.setDebug(new PrintWriter(new FileWriter("writer.debug", true)))
    reader.addCompletor(CliDriver.getCommandCompletor())

    var line: String = null
    val HISTORYFILE = ".hivehistory"
    val historyDirectory = System.getProperty("user.home")
    try {
      if ((new File(historyDirectory)).exists()) {
        val historyFile = historyDirectory + File.separator + HISTORYFILE
        reader.setHistory(new History(new File(historyFile)))
      } else {
        System.err.println("WARNING: Directory for Hive history file: " + historyDirectory +
                           " does not exist.   History will not be available during this session.")
      }
    } catch {
      case e: Exception =>
        System.err.println("WARNING: Encountered an error while trying to initialize Hive's " +
                           "history file.  History will not be available during this session.")
        System.err.println(e.getMessage())
    }

    // Use reflection to get access to the two fields.
    val getFormattedDbMethod = classOf[CliDriver].getDeclaredMethod(
      "getFormattedDb", classOf[HiveConf], classOf[CliSessionState])
    getFormattedDbMethod.setAccessible(true)

    val spacesForStringMethod = classOf[CliDriver].getDeclaredMethod(
      "spacesForString", classOf[String])
    spacesForStringMethod.setAccessible(true)

    var ret = 0

    var prefix = ""
    var curDB = getFormattedDbMethod.invoke(null, conf, ss).asInstanceOf[String]
    var curPrompt = SharkCliDriver.prompt + curDB
    var dbSpaces = spacesForStringMethod.invoke(null, curDB).asInstanceOf[String]

    line = reader.readLine(curPrompt + "> ")
    while (line != null) {
      if (!prefix.equals("")) {
        prefix += '\n'
      }
      if (line.trim().endsWith(";") && !line.trim().endsWith("\\;")) {
        line = prefix + line
        ret = cli.processLine(line)
        prefix = ""
        val sharkMode = SharkConfVars.getVar(conf, SharkConfVars.EXEC_MODE) == "shark"
        curPrompt = if (sharkMode) SharkCliDriver.prompt else CliDriver.prompt
      } else {
        prefix = prefix + line
        val sharkMode = SharkConfVars.getVar(conf, SharkConfVars.EXEC_MODE) == "shark"
        curPrompt = if (sharkMode) SharkCliDriver.prompt2 else CliDriver.prompt2
        curPrompt += dbSpaces
      }
      line = reader.readLine(curPrompt + "> ")
    }

    ss.close()

    System.exit(ret)
  }
}


class SharkCliDriver(loadRdds: Boolean = false)
  extends CliDriver
  with LogHelper
  with DriverHelper {

  if(loadRdds) CachedTableRecovery.loadAsRdds(processCmd(_))

  def this() = this(false)

  override def processCmd(commandInput: String): Int = {
    val cmd: String = commandInput.trim()
    val firstToken: String = cmd.split("\\s+")(0)
    val firstCommand: String = cmd.substring(firstToken.length()).trim()

    def exitCmd: Boolean = cmd.toLowerCase.equals("quit") ||
      cmd.toLowerCase.equals("exit") ||
      cmd.startsWith("!")

    def properCmd: Boolean = firstToken.equalsIgnoreCase("source") ||
      firstToken.toLowerCase.equals("list")

    def inspectState(driver: Driver): Either[Int, Int] = {
      import scala.collection.JavaConverters._
      printHeader( Option(driver.getSchema.getFieldSchemas.asScala.toList) )

      try {
        val res:ArrayList[String] = new ArrayList[String]()
        while (!out.checkError() && driver.getResults(res)) {
          res.foreach(out.println(_))
          res.clear()
        }
        Right(0)
      } catch {
        case e:IOException => {
          console.printError("Failed with exception %s:%s\n%s" format(e.getClass.getName,
            e.getMessage,
            org.apache.hadoop.util.StringUtils.stringifyException(e))
          )
          Left(1)
        }
      }
    }

    def handleDriver(hiveConf: HiveConf, proc: CommandProcessor): Int = {
      // There is a small overhead here to create a new instance of
      // SharkDriver for every command. But it saves us the hassle of
      // hacking CommandProcessorFactory.
      val driver: Driver = getDriver(hiveConf, proc)
      driver.init()

      logInfo("Execution Mode: %s" format SharkConfVars.getVar(conf, SharkConfVars.EXEC_MODE))
      log {() => out.println(commandInput)}

      runCommand(driver, commandInput) getOrElse {
        inspectState(driver) match {
          case Right(r) => {
            val ret = driver.close()
            destroyDriver(driver)
            ret
          }
          case Left(l) => {
            destroyDriver(driver)
            0
          }
        }
      }

    }

    def alternativeProcess(): Int = {
      val hiveConf = conf.asInstanceOf[HiveConf]

      Option(CommandProcessorFactory.get(firstToken, hiveConf)) match {
        case Some(proc: CommandProcessor) if (proc.isInstanceOf[Driver]) => {
          setThreadContext()
          handleDriver(hiveConf, proc)
        }
        case Some(proc: CommandProcessor) => {
          setThreadContext()

          log {() => out.println("%s %s" format(firstToken,firstCommand))}
          proc.run(firstCommand).getResponseCode
        }
        case _ => 0
      }
    }

    // super lazy
    Stream[Boolean](exitCmd, properCmd, isRemote) match {
      case true #:: true #:: true #:: _ => super.processCmd(commandInput)
      case _ => alternativeProcess()
    }

  }

  override def processFile(fileName: String): Int = {
    def process(bufferedReader: BufferedReader): Int = {
      //TODO: should have a catch
      try {
        val rc = processReader(bufferedReader)
        bufferedReader.close()
        rc
      }
      finally {
        IOUtils.closeStream(bufferedReader)
      }
    }

    def processS3Reader():Int = {
      Utils.setAwsCredentials(conf)
      Utils.createReaderForS3(fileName, conf) match {
        case Some(bufferedReader) => process(bufferedReader)
        case _ => 0
      }
    }

    if (Utils.isS3File(fileName)) processS3Reader()
    else super.processFile(fileName) // For non-S3 file, just use Hive's processFile.
  }
}
