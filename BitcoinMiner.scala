package remote

import akka.actor._
import util.control.Breaks._
import java.security.MessageDigest
import com.typesafe.config.ConfigFactory
import scala.collection._

case class AssignWork(seqCount: Int, leadZeroes: Int)
case class SetMaster(ip: String, port: String, masterSystem: String, masterName: String)
case class Bitcoin(input_str: String, hash_hex: String, worker_num: Int)
case class SetVars(seqFactor: Int, leadZeroes: Int)
case class MineBitcoins(seq_factor: Int, lead_zeroes: Int, seq_start_char: String, seq_end_char: String)
case class BiggestCoin(coin: String, totalCoins: Int)

class Miner extends Actor {
  val NOUNCE = "amansingh7;"
  val SEQ_FACTOR = 7 // dup in Main
 
  def generate_sequence(str: String, index: Int, seqStartChar: Char, seqEndChar: Char) : String = {
    if (index < 0) {
      return ""
    }
    
    var charArr = str.toCharArray()
    var charAtIndex = charArr.charAt(index)
    
    if(charAtIndex == seqEndChar) {
      charArr.update(index, seqStartChar)
      generate_sequence(new String(charArr), index-1, seqStartChar, seqEndChar)
      
    } else {
      charArr.update(index, (charAtIndex.toInt+1).toChar)
      new String(charArr) 
    }
  } 
  
  def receive = {
    case msg: String => println("Override method")
  }  
  
}

class MasterActor extends Miner {
  var seqFactor     = 0
  var leadZeroes    = 0
  var maxLeadZeroes = 0
  var biggestCoin   = "": String
  var masterMinersCount = 3
  var masterMinersReplied = 0
  var actorRefs     = mutable.MutableList[ActorRef]()
  var totalCoins = 0

  // executed by masterListener
  def setVars(seqFact: Int, leadZero: Int) = {
    seqFactor  = seqFact
    leadZeroes = leadZero
    
    // CREATE MINERS (MAke sure to change masterMinerCount accordingly)
    val masterMiner1 = context.actorOf(Props[MasterActor], name = "MasterMiner1")
//    println("Creating master miner 1...")
    masterMiner1 ! MineBitcoins(seqFact, leadZero, "a", "i")
    
    val masterMiner2 = context.actorOf(Props[MasterActor], name = "MasterMiner2")
//    println("Creating master miner 2...")
    masterMiner2 ! MineBitcoins(seqFact, leadZero, "j", "q") 
    
    val masterMiner3 = context.actorOf(Props[MasterActor], name = "MasterMiner3")
//    println("Creating master miner 3...")
    masterMiner3 ! MineBitcoins(seqFact, leadZero, "r", "z")
  }
  
  // executed by masterListener
  def assignWork = {
    seqFactor = seqFactor + 1
    sender ! AssignWork(seqFactor, leadZeroes)
    actorRefs += sender
  }

  def findBiggestCoin(hashHex: String) = {    
    var charArr = hashHex.toCharArray
    var zeroCount = 0 
    
    breakable {
      for(i <- 0 until charArr.length) {
        if(charArr(i) == '0') { 
          zeroCount = zeroCount + 1 
        } else {
          break
        }
      }
    }
           
    if(zeroCount > maxLeadZeroes) {
      maxLeadZeroes = zeroCount
      biggestCoin   = hashHex  
    }
    
  }
  
  
  // masterMiner executes this
  // masterListener executes this when it receives bitcoin from worker
  def processBitcoin(inputStr: String, hashHex: String, machine: String) = {
    println("["+machine+"] : \t" + inputStr + "\t" + hashHex)
    findBiggestCoin(hashHex)
  }

   // masterListener executes this 
  def shutdownWorkerSystems = {
    for(i<-0 until actorRefs.length) {
      var workerRef = actorRefs(i)
      workerRef ! "KILL"
    }
  }
  
  // masterListener executes this
  def shutdownMasterSystem = {
    context.system.shutdown
  }
  
  // masterListener executes this
  
  // masterMiners reply to masterListener with this message
  def updateBiggestCoin(coin: String, totalCoinsMiner: Int) = {
    findBiggestCoin(coin)
    totalCoins = totalCoins + totalCoinsMiner
    
    // check how many masterMiners replied
    masterMinersReplied = masterMinersReplied + 1
    if(masterMinersReplied == masterMinersCount) { // All miners have mined and replied back to the masterListener
      
//      println("All masterMiners have finished mining and replied back to the masterListener")
      println("Bitcoins Mined = " + totalCoins)
      println("Bitcoin with maximum leading zeroes found = " + biggestCoin)
      
      shutdownWorkerSystems // shutdown all worker systems
      shutdownMasterSystem  // shutdown master system
    } 
  }
  
  // masterMiner executes this
  def mine(seq_factor: Int, lead_zeroes: Int, seq_start_char: String, seq_end_char: String) = {
    var seq       = seq_start_char * seq_factor
    var seq_end   = seq_end_char   * seq_factor
    var input_str = NOUNCE + seq
    var hash_hex  = ""
    var sha       = MessageDigest.getInstance("SHA-256")
    var count     = 0
    
    while(seq < seq_end){
      hash_hex = sha.digest(input_str.getBytes).map{ b => String.format("%02X", new java.lang.Integer(b & 0xff)) }.mkString
      
      if(hash_hex.startsWith("0" * lead_zeroes, 0)) { // Bitcoin found
        processBitcoin(input_str, hash_hex, "MASTER")
        count = count+1
        totalCoins = totalCoins + 1
      } 
      
      var start_char = seq_start_char.toCharArray().charAt(0)
      var end_char = seq_end_char.toCharArray().charAt(0)

      seq = generate_sequence(seq, seq.length -1, start_char, end_char)
      input_str = NOUNCE + seq
    } // while
    
//    println("Bitcoin with max lead zeroes for this master actor = " + biggestCoin)
    
  } // def mine  
  
  override def receive = {
    case SetVars(s, l) => setVars(s, l) // sent to masterListener and masterMiner
    
    case MineBitcoins(seqFactor, leadZeroes, seqStartChar, seqEndChar)  => // handled by masterMiners
      mine(seqFactor, leadZeroes, seqStartChar, seqEndChar) // mine and print
      sender ! BiggestCoin(biggestCoin, totalCoins) // sent to masterListener
      
    case "WORKER" => assignWork // sent to masterListener
    
    case Bitcoin(inputStr, hashHex, workerNum) => // sent to masterListener. called by worker
      totalCoins = totalCoins + 1
      processBitcoin(inputStr, hashHex, "WORKER - "+workerNum)
      
    case BiggestCoin(coin, totalCoins) => // sent to masterListener. This also checks and shuts down the system accordingly
      updateBiggestCoin(coin, totalCoins)
      
  }  
}

class WorkerActor extends Miner {
  var masterListener = context.actorSelection("")
  var workerNum = 0

  def setMaster(ip: String, port: String, system: String, name: String) = {
    var masterStr = "akka.tcp://"+system+"@" + ip + ":" + port + "/user/"+name
    masterListener = context.actorSelection(masterStr)
  }
  
  // workerListener creates new workerMiners here!
  def startMiningWorker(seq_factor: Int, lead_zeroes: Int) = {
    var workerMiner = context.actorOf(Props[WorkerActor], name = "WorkerMiner")
    workerMiner ! MineBitcoins(seq_factor, lead_zeroes, "a", "z")
  }
  
  def pingMasterListener = {
    masterListener ! "WORKER"
  }
 
  // workerMiner sends the bitcoin to masterListener for printing thru the workerListener
  def mine(seq_factor: Int, lead_zeroes: Int, seq_start_char: String, seq_end_char: String) = {
//    println("[Inside worker mine] : SEQ_FACTOR = " + seq_factor + ", LEAD_ZEROES = " + lead_zeroes)
    
    var seq       = seq_start_char * seq_factor
    var seq_end   = seq_end_char   * seq_factor
    var input_str = NOUNCE + seq
    var hash_hex  = ""
    var sha       = MessageDigest.getInstance("SHA-256")
    var count     = 0
    workerNum     = seq_factor - SEQ_FACTOR // since seq factor is incremented for every new worker actor system
    var workerListener = sender
    
    while(seq < seq_end){
      hash_hex = sha.digest(input_str.getBytes).map{ b => String.format("%02X", new java.lang.Integer(b & 0xff)) }.mkString
      
      if(hash_hex.startsWith("0"*lead_zeroes, 0)) { // Bitcoin found
        workerListener ! Bitcoin(input_str, hash_hex, workerNum)
        count = count+1
      } 
      
      var start_char = seq_start_char.toCharArray().charAt(0)
      var end_char = seq_end_char.toCharArray().charAt(0)
      
      seq = generate_sequence(seq, seq.length -1, start_char, end_char)
      
      input_str = NOUNCE + seq
    } // while
   
  } // def mine  
  
  def shutdownSystem = {
    context.system.shutdown
  }
  
  override def receive = {
    case SetMaster(ip, port, system, name)     => setMaster(ip, port, system, name) // sent to workerListener
    case "START"                               => pingMasterListener // sent to workerListener
    case AssignWork(seq_factor, lead_zeroes)   => startMiningWorker(seq_factor, lead_zeroes) // sent to workerListener
    case MineBitcoins(seq_factor, lead_zeroes, seq_start_char, seq_end_char) => mine(seq_factor, lead_zeroes, seq_start_char, seq_end_char) // sent to workerMiner
    case Bitcoin(inputStr, hashHex, workerNum) => masterListener ! Bitcoin(inputStr, hashHex, workerNum) // sent to workerListener
    case "KILL"                                => shutdownSystem
    case _ => println("huh?")
  }
    
}

object Main extends App {
  val PORT = "2555"
  val SEQ_FACTOR = 7 // dup at top
  
	def get_config(ip: String, port: String) : String = {
      var config = """
      akka {
      loglevel = "OFF"
      
      actor {
      provider = "akka.remote.RemoteActorRefProvider"
      }
      remote {
      enabled-transports = ["akka.remote.netty.tcp"]
      netty.tcp {
      hostxname = """ + ip +
      """
      port = """ + port +
      """
      }
      }
      
      log-sent-messages = off
      log-received-messages = off
      }""" : String
      
      return config
	}
  
  val userInput = args(0)
  
  if(userInput.count(_ == '.') == 3) { // IP Address passed - Call Worker
    val worker_system = ActorSystem("WorkerSystem", ConfigFactory.parseString(get_config(userInput, "0")))
    val workerActor = worker_system.actorOf(Props[WorkerActor], name = "WorkerActor")
    workerActor ! SetMaster(userInput, PORT, "MasterSystem", "MasterActor")
    workerActor ! "START"  
        
  } else { // K passed - Call Master
    val master_system = ActorSystem("MasterSystem", ConfigFactory.parseString(get_config("localhost", PORT)))
    val masterListener = master_system.actorOf(Props[MasterActor], name = "MasterActor")

    
    // start master listener (this actor just communicates with worker actors)
    masterListener ! SetVars(SEQ_FACTOR, userInput.toInt)      
    
  }

}
