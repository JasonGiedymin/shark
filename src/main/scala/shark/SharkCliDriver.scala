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
  val out = sessionState.get.out

  val conf: Configuration = if (!sessionState.isEmpty) sessionState.get.getConf else new Configuration()
  val hiveConf = conf.asInstanceOf[HiveConf]

  val LOG = LogFactory.getLog("CliDriver")
  val console: SessionState.LogHelper = new SessionState.LogHelper(LOG)

  def isRemote: Boolean = sessionState.map { _.asInstanceOf[CliSessionState].isRemoteMode }.getOrElse(false)

  def init() {
    SharkConfVars.initializeWithDefaults(conf)

    // Force initializing SharkEnv. This is put here but not object SharkCliDriver
    // because the Hive unit tests do not go through the main() code path.
    SharkEnv.init()
  }

  def recordTiming(start:Long) {
    val end:Long = System.currentTimeMillis()
    if (end > start) {
      val timeTaken:Double = (end - start) / 1000.0
      console.printInfo("Time taken: %s seconds" format timeTaken)
    }
  }

  def printHeader(fieldSchemas: List[FieldSchema]) {
    // really should consider just returning the logging data
    // or async log it all
    if (HiveConf.getBoolVar(conf, HiveConf.ConfVars.HIVE_CLI_PRINT_HEADER)) {
      // Print the column names.
      out.println(fieldSchemas.map(_.getName).mkString("\t"))
    }
  }

  def log(action:() => Unit) {
    sessionState match {
      case Some(state) if(state.getIsVerbose) => action()
      case _ => /* Do Nothing */
    }
  }

  def usingSessionState(action:() => Unit) {
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
      val driver = new SharkDriver(hiveConf)
      driver.init()
      driver
    } else {
      proc.asInstanceOf[Driver]
    }
  }

  def runCommand(driver:Driver, commandInput: String): Either[Int, (Driver, Int)] = {
    driver.run(commandInput).getResponseCode match {
      case resCode:Int if (resCode != 0) => {
        println("--> in case resCode, %s" format resCode)
        driver.close
        Right(driver, resCode)
      }
      case _ => Right(driver, 0)
    }
  }

  def inspectState(driver: Driver)(implicit start:Long): Int = {
    printHeader( Option(driver.getSchema.getFieldSchemas).map(_.toList).getOrElse(List()) )

    try {
      val res:java.util.ArrayList[String] = new java.util.ArrayList[String]()
      while (!out.checkError() && driver.getResults(res)) {
        res.foreach(out.println(_))
        res.clear()
      }
      val ret = driver.close()
      destroyDriver(driver)
      ret
    } catch {
      case e:IOException => {
        console.printError("Failed with exception %s:%s\n%s" format(e.getClass.getName,
          e.getMessage,
          org.apache.hadoop.util.StringUtils.stringifyException(e))
        )
        destroyDriver(driver)
        0
      }
    }
  }

  def destroyDriver(driver: Driver)(implicit start:Long) {
    recordTiming(start)
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
    if (!oproc.process_stage1(hiveArgs)) System.exit(1)


    def initHiveLogs():(Boolean, String) = {
      try {
        (false, LogUtils.initHiveLog4j)
      } catch {
        case e: LogInitializationException => (true, e.getMessage)
      }
    }

    def buildSessionState(): Option[CliSessionState] = {
      val sessionState = new CliSessionState(new HiveConf(classOf[SessionState]))

      try {
        sessionState.in = System.in
        sessionState.out = new PrintStream(System.out, true, "UTF-8")
        sessionState.info = new PrintStream(System.err, true, "UTF-8")
        sessionState.err = new PrintStream(System.err, true, "UTF-8")
        Some(sessionState)
      } catch {
        case e: UnsupportedEncodingException => None
      }
    }

    def addHooks() {
      // Clean up after we exit
      Runtime.getRuntime.addShutdownHook(
        new Thread() {
          override def run() {
            SharkEnv.stop()
          }
        }
      )
    }

    case class DriverState(conf:HiveConf, session:CliSessionState)

    def saveKeys(driverState:DriverState): DriverState = {
      driverState.session.cmdProperties.entrySet().foreach { item: java.util.Map.Entry[Object, Object] =>
        driverState.conf.set(item.getKey.asInstanceOf[String], item.getValue.asInstanceOf[String])
        driverState.session.getOverriddenConfigurations.put(
          item.getKey.asInstanceOf[String], item.getValue.asInstanceOf[String])
      }
      driverState
    }

    def startSession(driverState:DriverState): DriverState = {
      SessionState.start(driverState.session)
      driverState
    }

    def handleHost(driverState:DriverState): DriverState = {
      // "-h" option has been passed, so connect to Shark Server.
      Option(driverState.session.getHost) match {
        case Some(host:String) if driverState.session.isRemoteMode => {
          driverState.session.connect()
          prompt = "[" + driverState.session.getHost + ':' + driverState.session.getPort + "] " + prompt
          val spaces = Array.tabulate(prompt.length)(_ => ' ')
          prompt2 = new String(spaces)
        }
        case Some(host:String) => driverState.session.connect()
      }

      driverState
    }

    // NOTE: It is critical to do this here so that log4j is reinitialized
    // before any of the other core hive classes are loaded
    val (logInitFailed, logInitDetailMessage) = initHiveLogs()
    def logErrors() {
      if (logInitFailed) System.err.println(logInitDetailMessage)
      else SessionState.getConsole.printInfo(logInitDetailMessage)
    }

    val sessionState = buildSessionState()
    sessionState match {
      case Some(session) if (!oproc.process_stage2(session)) => System.exit(2)
      case Some(session) if (!session.getIsSilent) => { // SEE: TODO: start ripping from here
        logErrors()
        // Set all properties specified via command line.
        val conf: HiveConf = session.getConf
        val driverState:DriverState = DriverState(conf, session)

        val work = saveKeys _ andThen
          startSession _ andThen
          handleHost _

        work(driverState)

        //saveKeys(session, conf)
        //SessionState.start(session)

        addHooks()

        //handleHost()

        if (!sessionState.isRemoteMode() && !ShimLoader.getHadoopShims().usesJobShell()) {
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

      }
      case None => System.exit(3)
    }

    // TODO: start ripping from here
    if (!sessionState.isRemoteMode() && !ShimLoader.getHadoopShims().usesJobShell()) {
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
    cli.processInitFiles(sessionState)

    if (sessionState.execString != null) {
      System.exit(cli.processLine(sessionState.execString))
    }

    try {
      if (sessionState.fileName != null) {
        System.exit(cli.processFile(sessionState.fileName))
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
    var curDB = getFormattedDbMethod.invoke(null, conf, sessionState).asInstanceOf[String]
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

    sessionState.close()

    System.exit(ret)
  }
}


class SharkCliDriver(loadRdds: Boolean = false)
  extends CliDriver
  with LogHelper
  with DriverHelper {

  SharkConfVars.initializeWithDefaults(conf)

  // Force initializing SharkEnv. This is put here but not object SharkCliDriver
  // because the Hive unit tests do not go through the main() code path.
  SharkEnv.init()

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

    def alternativeProcess():Int = {
      Option(CommandProcessorFactory.get(firstToken, hiveConf)) match {
        case Some(proc:CommandProcessor) if (proc.isInstanceOf[Driver]) => {
          logInfo("Execution Mode: " + SharkConfVars.getVar(conf, SharkConfVars.EXEC_MODE))
          log{ () => out.println(cmd)}

          setThreadContext()
          implicit val start:Long = System.currentTimeMillis()
          runCommand( getDriver(hiveConf, proc), commandInput ) match {
            case Right((driver, ret)) => inspectState(driver)
            case Left(l) => 0
          }

        }
        case Some(proc:CommandProcessor) => {
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
