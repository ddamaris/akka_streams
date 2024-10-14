import akka.{NotUsed, actor}
import akka.actor.typed.{ActorSystem, Behavior, Props}
import akka.actor.typed.scaladsl.{ActorContext, Behaviors}
import akka.persistence.cassandra.query.scaladsl.CassandraReadJournal
import akka.persistence.query.{EventEnvelope, PersistenceQuery}
import akka.persistence.typed.PersistenceId
import akka.persistence.typed.scaladsl.{Effect, EventSourcedBehavior}
import akka.stream.{ClosedShape, Graph}
import akka.stream.alpakka.slick.scaladsl.{Slick, SlickSession}
import akka.stream.scaladsl.{Broadcast, Flow, GraphDSL, RunnableGraph, Sink, Source}
import akka_typed.TypedCalculatorWriteSide.{
  Add,
  Added,
  Command,
  Divide,
  Divided,
  Multiplied,
  Multiply
}
import scalikejdbc.DB.using
import scalikejdbc.{ConnectionPool, ConnectionPoolSettings, DB}
import akka_typed.CalculatorRepository.{
  Result,
  getLatestsOffsetAndResult,
  initDatabase,
  session,
  updateResultAndOffsetSlick,
  updatedResultAndOffset
}
import slick.jdbc.GetResult

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.DurationInt
import scala.concurrent.Await
import scala.language.postfixOps

object akka_typed {

  trait CborSerialization

  val persId = PersistenceId.ofUniqueId("001")

  object TypedCalculatorWriteSide {
    sealed trait Command
    case class Add(amount: Double)      extends Command
    case class Multiply(amount: Double) extends Command
    case class Divide(amount: Double)   extends Command

    sealed trait Event
    case class Added(id: Int, amount: Double)      extends Event
    case class Multiplied(id: Int, amount: Double) extends Event
    case class Divided(id: Int, amount: Double)    extends Event

    final case class State(value: Double) extends CborSerialization {
      def add(amount: Double): State      = copy(value = value + amount)
      def multiply(amount: Double): State = copy(value = value * amount)
      def divide(amount: Double): State   = copy(value = value / amount)
    }

    object State {
      val empty: State = State(0)
    }

    def handleCommand(
        persistenceId: String,
        state: State,
        command: Command,
        ctx: ActorContext[Command]
    ): Effect[Event, State] =
      command match {
        case Add(amount) =>
          ctx.log.info(s"receive adding  for number: $amount and state is ${state.value}")
          val added = Added(persistenceId.toInt, amount)
          Effect
            .persist(added)
            .thenRun { x =>
              ctx.log.info(s"The state result is ${x.value}")
            }
        case Multiply(amount) =>
          ctx.log.info(s"receive multiplying  for number: $amount and state is ${state.value}")
          val multiplied = Multiplied(persistenceId.toInt, amount)
          Effect
            .persist(multiplied)
            .thenRun { x =>
              ctx.log.info(s"The state result is ${x.value}")
            }
        case Divide(amount) =>
          ctx.log.info(s"receive dividing  for number: $amount and state is ${state.value}")
          val divided = Divided(persistenceId.toInt, amount)
          Effect
            .persist(divided)
            .thenRun { x =>
              ctx.log.info(s"The state result is ${x.value}")
            }
      }

    def handleEvent(state: State, event: Event, ctx: ActorContext[Command]): State =
      event match {
        case Added(_, amount) =>
          ctx.log.info(s"Handling event Added is: $amount and state is ${state.value}")
          state.add(amount)
        case Multiplied(_, amount) =>
          ctx.log.info(s"Handling event Multiplied is: $amount and state is ${state.value}")
          state.multiply(amount)
        case Divided(_, amount) =>
          ctx.log.info(s"Handling event Divided is: $amount and state is ${state.value}")
          state.divide(amount)
      }

    def apply(): Behavior[Command] =
      Behaviors.setup { ctx =>
        EventSourcedBehavior[Command, Event, State](
          persistenceId = persId,
          State.empty,
          (state, command) => handleCommand("001", state, command, ctx),
          (state, event) => handleEvent(state, event, ctx)
        )
      }

  }

  case class TypedCalculatorReadSide(system: ActorSystem[NotUsed]) {

    initDatabase

    implicit val materializer: actor.ActorSystem = system.classicSystem
    var (offset, latestCalculatedResult)         = getLatestsOffsetAndResult
    val startOffset: Int                         = if (offset == 1) 1 else offset + 1

    val readJournal =
      PersistenceQuery(system).readJournalFor[CassandraReadJournal](CassandraReadJournal.Identifier)

    val source: Source[EventEnvelope, NotUsed] =
      readJournal.eventsByPersistenceId("001", startOffset, Long.MaxValue)

    def updateState(event: Any, seqNum: Long): Result = {
      val newState = event match {
        case Added(_, amount) =>
          latestCalculatedResult + amount
        case Multiplied(_, amount) =>
          latestCalculatedResult * amount
        case Divided(_, amount) =>
          latestCalculatedResult / amount
      }
      Result(newState, seqNum)
    }

    val graph: Graph[ClosedShape.type, NotUsed] = GraphDSL.create() {
      implicit builder: GraphDSL.Builder[NotUsed] =>
        val input = builder.add(source)
        val stateUpdater =
          builder.add(Flow[EventEnvelope].map(e => updateState(e.event, e.sequenceNr)))
        val localSaveOutput = builder.add(Sink.foreach[Result] { r =>
          latestCalculatedResult = r.state
          println("something to print")
        })

        val dbSaveOutput = builder.add(
          Slick.sink[Result](r => updateResultAndOffsetSlick(r))
        )

        val broadcast = builder.add(Broadcast[Result](2))

        import GraphDSL.Implicits._
        input ~> stateUpdater ~> broadcast

        broadcast.out(0) ~> dbSaveOutput
        broadcast.out(1) ~> localSaveOutput

        ClosedShape
    }

    source
      .map { x =>
        println(x.toString())
        x
      }
      .runForeach { event =>
        event.event match {
          case Added(_, amount) =>
            latestCalculatedResult += amount
            updatedResultAndOffset(latestCalculatedResult, event.sequenceNr)
            println(s"Log from Added: $latestCalculatedResult")
          case Multiplied(_, amount) =>
            latestCalculatedResult *= amount
            updatedResultAndOffset(latestCalculatedResult, event.sequenceNr)
            println(s"Log from Multiplied: $latestCalculatedResult")
          case Divided(_, amount) =>
            latestCalculatedResult /= amount
            updatedResultAndOffset(latestCalculatedResult, event.sequenceNr)
            println(s"Log from Divided: $latestCalculatedResult")
        }
      }
  }

  object CalculatorRepository {

    implicit lazy val session: SlickSession = SlickSession.forConfig("slick-postgres")
    import session.profile.api._

    def initDatabase(): Unit = {
      Class.forName("org.postgresql.Driver")
      val poolSettings = ConnectionPoolSettings(initialSize = 10, maxSize = 100)
      ConnectionPool.singleton(
        "jdbc:postgresql://localhost:5432/demo",
        "docker",
        "docker",
        poolSettings
      )
    }

    case class Result(state: Double, offset: Long)
    def getLatestsOffsetAndResult(implicit session: SlickSession): Result = {
      import session.profile.api._
      implicit val getResult: GetResult[Result] =
        GetResult(row => Result(row.nextDouble(), row.nextLong()))
      val query = sql"select * from public.result where id = 1;"
        .as[Result]
        .headOption
      val res = session.db.run(query).map(v => v.flatMap(r => Some(Result(r.state, r.offset))))
      Await.result(res, 10000.nanos).getOrElse(throw new RuntimeException("not found"))
    }

    def getLatestsOffsetAndResult: (Int, Double) = {
      val entities =
        DB readOnly { session =>
          session.list("select * from public.result where id = 1;") { row =>
            (row.int("write_side_offset"), row.double("calculated_value"))
          }
        }
      entities.head
    }

    def updatedResultAndOffset(calculated: Double, offset: Long): Unit = {
      using(DB(ConnectionPool.borrow())) { db =>
        db.autoClose(true)
        db.localTx {
          _.update(
            "update public.result set calculated_value = ?, write_side_offset = ? where id = 1",
            calculated,
            offset
          )
        }
      }
    }

    def updateResultAndOffsetSlick(result: Result) =
      sqlu"update public.result set calculated_value = ${result.state}, write_side_offset = ${result.offset} where id = 1"

  }

  def apply(): Behavior[NotUsed] =
    Behaviors.setup { ctx =>
      val writeAcorRef = ctx.spawn(TypedCalculatorWriteSide(), "Calc", Props.empty)
      writeAcorRef ! Add(10)
      writeAcorRef ! Multiply(2)
      writeAcorRef ! Divide(5)

      Behaviors.same
    }

  def execute(command: Command): Behavior[NotUsed] =
    Behaviors.setup { ctx =>
      val writeAcorRef = ctx.spawn(TypedCalculatorWriteSide(), "Calc", Props.empty)
      writeAcorRef ! command
      Behaviors.same
    }

  def main(args: Array[String]): Unit = {
    val value                                 = akka_typed()
    implicit val system: ActorSystem[NotUsed] = ActorSystem(value, "akka_typed")
    RunnableGraph.fromGraph(TypedCalculatorReadSide(system).graph).run()
  }

}
