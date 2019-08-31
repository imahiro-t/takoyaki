import java.io.PrintWriter

import akka.NotUsed
import akka.actor.{ActorSystem, Cancellable}
import akka.stream._
import akka.stream.scaladsl._
import akka.stream.stage.{GraphStage, GraphStageLogic, InHandler, OutHandler}

import scala.collection.immutable.Queue
import scala.concurrent.duration._

final case class OrderCount(value: Int)
final case class OrderNumber(value: Int)
final case class Order(number: OrderNumber, count: OrderCount)
trait Takoball
final case class NormalTakoball() extends Takoball
final case class BugTakoball() extends Takoball
object Takoball {
  def create: Takoball =
    if (scala.util.Random.nextInt(100) == 0) BugTakoball() else NormalTakoball()
}
final case class Pack(private val takoballs: Seq[Takoball]) {
  def valid: Boolean = takoballs.forall(_.isInstanceOf[NormalTakoball])
  override def toString: String = s"[${takoballs.map(_ => "●").mkString(",")}]"
}
object Pack {
  val MAX_TAKOBALL_COUNT = 8
}
final case class EmptyPack() {
  def fill(takoballs: Seq[Takoball]): Pack = Pack(takoballs)
}
final case class Teppan(packs: Seq[EmptyPack])
object Teppan {
  val LANE_SIZE: Int = 4
}
trait Band
final case class NormalBand() extends Band
final case class TopBand(order: Order) extends Band

final case class BufferQueue[T](private var size: Int = 0) {
  private var queue: Queue[T] = Queue.empty[T]
  def setSize(size: Int): Unit = {
    this.size = size
  }
  def add(value: T): List[T] = {
    this.queue = this.queue.enqueue(value)
    this.queue.toList
  }
  def clear(): Unit = {
    this.queue = Queue.empty[T]
  }
  def isEmpty: Boolean = this.queue.isEmpty
  def isComplete: Boolean = !isEmpty && this.queue.size == this.size
}

final case class BugInsideException() extends RuntimeException {
  override def printStackTrace(s: PrintWriter): Unit = s.println("sorry... bug inside... retrying to cook...")
}

object Order {
  private val ORDER_MAX = 6
  private var number = 0
  def create: Order = {
    number += 1
    Order(OrderNumber(number), OrderCount(scala.util.Random.nextInt(ORDER_MAX) + 1))
  }
}

//class OrderSource extends GraphStage[SourceShape[Order]] {
//  val out: Outlet[Order] = Outlet("OrderSource")
//  override val shape: SourceShape[Order] = SourceShape(out)
//  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
//    new GraphStageLogic(shape) {
//      setHandler(out, new OutHandler {
//        override def onPull(): Unit = {
//          Thread.sleep(scala.util.Random.nextInt(5))
//          push(out, Order.create)
//        }
//      })
//    }
//}

class PackingFlow extends GraphStage[FlowShape[(Band, Pack), Seq[Pack]]] {
  val in: Inlet[(Band, Pack)] = Inlet("PackingFlowIn")
  val out: Outlet[Seq[Pack]] = Outlet("PackingFlowOut")
  override val shape: FlowShape[(Band, Pack), Seq[Pack]] = FlowShape(in, out)
  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
    new GraphStageLogic(shape) {
      private val queue: BufferQueue[Pack] = BufferQueue[Pack]()
      setHandler(out, new OutHandler {
        override def onPull(): Unit = {
          pull(in)
        }
      })
      setHandler(in, new InHandler {
        override def onPush(): Unit = {
          val grabin = grab(in)
          grabin._1 match {
            case b: TopBand =>
              queue.setSize(b.order.count.value)
            case _ =>
              ()
          }
          val list = queue.add(grabin._2)
          println(s"${Seq.fill(list.size)(".").mkString}")
          if (queue.isComplete) {
            push(out, list)
            queue.clear()
          } else {
            pull(in)
          }
        }
      })
    }
}

object Main extends App {
  implicit val system = ActorSystem("TakoyakiAS")
  implicit val materializer = ActorMaterializer()

//  lazy private val order: Source[Order, NotUsed] = Source.fromGraph(new OrderSource)
  lazy private val order: Source[Order, Cancellable] = Source.tick(2.seconds, (scala.util.Random.nextInt(10) + 1).seconds, Order).map(_ => Order.create)
    .wireTap(x => println(s"${x.count.value} pack(s) ordered [${"%010d".format(x.number.value)}]"))

  lazy private val packing: Flow[(Band, Pack), Seq[Pack], NotUsed] = Flow.fromGraph(new PackingFlow)

  lazy private val serve: Sink[(Band, Pack), NotUsed] = Flow[(Band, Pack)].via(packing)
    .wireTap(x => println(s"serve ${x.size} pack(s) {${x.mkString(" ")}}")).to(Sink.ignore)

  lazy private val checkout: Flow[Order, Band, NotUsed] = Flow[Order].mapConcat(x => TopBand(x) +: List.fill(x.count.value - 1)(NormalBand()))

  lazy private val INIT_COOK_COUNT = 3
  lazy private val prepareForFirstCook: Source[Seq[EmptyPack], NotUsed] = Source.single(Seq.fill(INIT_COOK_COUNT)(EmptyPack()))

  lazy private val prepareForCook: Flow[Order, Seq[EmptyPack], NotUsed] = Flow[Order].map(x => List.fill(x.count.value)(EmptyPack()))

  lazy private val fillTakoball: EmptyPack => Pack = emptyPack => {
    emptyPack.fill(Seq.fill(Pack.MAX_TAKOBALL_COUNT)(Takoball.create))
  }

  lazy private val cookPerPack: Flow[EmptyPack, Pack, NotUsed] =
    Flow[EmptyPack].flatMapConcat(x => RestartSource.onFailuresWithBackoff(
      minBackoff = 1.seconds,
      maxBackoff = 2.seconds,
      randomFactor = 0.2,
      maxRestarts = 20
    ) {
      () => Source.single(x).delay(3.seconds).map(fillTakoball).map(x => if (x.valid) x else throw BugInsideException())
    })

  lazy private val cook: Flow[Seq[EmptyPack], Pack, NotUsed] = Flow.fromGraph(GraphDSL.create() { implicit builder: GraphDSL.Builder[NotUsed] =>
    import GraphDSL.Implicits._
    lazy val preparePack = builder.add(Flow[Seq[EmptyPack]].mapConcat(x => x.toList))
    lazy val balance = builder.add(Balance[EmptyPack](Teppan.LANE_SIZE).async)
    lazy val merge = builder.add(Merge[Pack](Teppan.LANE_SIZE))
    preparePack ~> balance
    for (i <- 1 to Teppan.LANE_SIZE) {
      balance ~> cookPerPack.async ~> merge
    }
    FlowShape(preparePack.in, merge.out)
  })

  lazy private val graph: RunnableGraph[NotUsed] = RunnableGraph.fromGraph(GraphDSL.create() { implicit builder: GraphDSL.Builder[NotUsed] =>
    import GraphDSL.Implicits._
    lazy val broadcast = builder.add(Broadcast[Order](2))
    lazy val merge = builder.add(Merge[Seq[EmptyPack]](2))
    lazy val zip = builder.add(ZipWith[Pack, Band, (Band, Pack)]((x, y) => (y, x)))
    order ~> broadcast ~> prepareForCook ~> merge ~> cook ~> zip.in0
    prepareForFirstCook ~> merge
    broadcast ~> checkout ~> zip.in1
    zip.out ~> serve
    ClosedShape
  })

  graph.run()
}

