package net.degoes

import zio.*
import zio.test.*
import dataset6.*
import scala.util.Random
import zio.test.Assertion.*

object FastDatasetSpec extends ZIOSpecDefault {

  def row(int: Int, dec: Double): Row = Row(
    Map(
      "text" -> Value.Text(s"Row - $int - $dec"),
      "int"  -> Value.Integer(int),
      "dec"  -> Value.Decimal(dec),
      "na"   -> Value.NA
    )
  )

  def spec = suite("FastDatasetSpec")(
    test("Constructing a dataset from rows and then getting rows should return the same rows") {
      val dataset = Dataset.fromRows(rows)
      assertTrue(dataset.rows == rows)
    },
    test("Select ints field") {

      val dataset = Dataset.fromRows(rows)
      val ints    = dataset(Field("int"))

      val expected = Chunk(
        Row(
          Map(
            "int" -> Value.Integer(1)
          )
        ),
        Row(
          Map(
            "int" -> Value.Integer(2)
          )
        ),
        Row(
          Map(
            "int" -> Value.Integer(3)
          )
        ),
        Row(Map.empty)
      )

      assertTrue(ints.rows == expected)
    },
    test("Select na field") {

      val dataset = Dataset.fromRows(rows)
      val ints    = dataset(Field("na"))

      val expected = Chunk(
        Row(
          Map(
            "na" -> Value.NA
          )
        ),
        Row(
          Map(
            "na" -> Value.NA
          )
        ),
        Row(Map.empty),
        Row(
          Map(
            "na" -> Value.NA
          )
        )
      )

      assertTrue(ints.rows == expected)
    },
    test("Add two datasets - test 1") {

      val rows = Chunk(
        Row(
          Map(
            "int" -> Value.Integer(1),
            "dec" -> Value.Decimal(1.0)
          )
        )
      )

      val dataset1 = Dataset.fromRows(rows)
      val dataset2 = Dataset.fromRows(rows)

      val sum = dataset1 + dataset2

      val expected = Chunk(
        Row(
          Map(
            "int + dec" -> Value.Decimal(2.0),
            "int + int" -> Value.Integer(2),
            "dec + int" -> Value.Decimal(2.0),
            "dec + dec" -> Value.Decimal(2.0)
          )
        )
      )

      assertTrue(sum.rows == expected)
    },
    test("Add two datasets - test 2") {

      val rows1 = Chunk(
        Row(
          Map(
            "int" -> Value.Integer(1),
            "dec" -> Value.Decimal(1.0)
          )
        )
      )

      val rows2 = Chunk(
        Row(
          Map(
            "int" -> Value.Integer(1),
            "na"  -> Value.NA
          )
        )
      )

      val dataset1 = Dataset.fromRows(rows1)
      val dataset2 = Dataset.fromRows(rows2)

      val sum = dataset1 + dataset2

      val expected = Chunk(
        Row(
          Map(
            "dec + na"  -> Value.NA,
            "int + int" -> Value.Integer(2),
            "dec + int" -> Value.Decimal(2.0),
            "int + na"  -> Value.NA
          )
        )
      )

      assertTrue(sum.rows == expected)
    },
    test("Add two datasets - test 3") {

      val rows1 = Chunk(
        Row(
          Map(
            "int" -> Value.Integer(1),
            "dec" -> Value.Decimal(1.0)
          )
        )
      )

      val rows2 = Chunk(
        Row(
          Map(
            "int" -> Value.Integer(1)
          )
        )
      )

      val dataset1 = Dataset.fromRows(rows1)
      val dataset2 = Dataset.fromRows(rows2)

      val sum = dataset1 + dataset2

      val expected = Chunk(
        Row(
          Map(
            "int + int" -> Value.Integer(2),
            "dec + int" -> Value.Decimal(2.0)
          )
        )
      )

      assertTrue(sum.rows == expected)
    },
    test("Add two datasets - test 4") {

      val rows1 = Chunk(
        Row(
          Map(
            "text" -> Value.Text("T1"),
            "int"  -> Value.Integer(1)
          )
        )
      )

      val rows2 = Chunk(
        Row(
          Map(
            "text" -> Value.Text("T2"),
            "int"  -> Value.Integer(1)
          )
        )
      )

      val dataset1 = Dataset.fromRows(rows1)
      val dataset2 = Dataset.fromRows(rows2)

      val sum = dataset1 + dataset2

      val expected = Chunk(
        Row(
          Map(
            "text + text" -> Value.NA,
            "int + text"  -> Value.NA,
            "int + int"   -> Value.Integer(2),
            "text + int"  -> Value.NA
          )
        )
      )

      assertTrue(sum.rows == expected)
    },
    test("Performance benchmark has no errors") {

      val start: Field  = Field("start")
      val end: Field    = Field("end")
      val netPay: Field = Field("netPay")
      val rng: Random   = new Random(0L)

      val dataset = Dataset.fromRows(Chunk.fill(10) {
        val start  = rng.between(0, 360)
        val end    = rng.between(start, 360)
        val netPay = rng.between(20000, 60000)

        Row(
          Map(
            "start"  -> Value.Integer(start),
            "end"    -> Value.Integer(end),
            "netPay" -> Value.Integer(netPay)
          )
        )
      })

      val result = (dataset(start) + dataset(end)) / dataset(netPay)

      assert(result.rows)(isNonEmpty)
    }
  )

  private def rows = Chunk(
    Row(
      Map(
        "text" -> Value.Text(s"Row 1"),
        "int"  -> Value.Integer(1),
        "dec"  -> Value.Decimal(1.0),
        "na"   -> Value.NA
      )
    ),
    Row(
      Map(
        "text" -> Value.Text(s"Row 2"),
        "int"  -> Value.Integer(2),
        "dec"  -> Value.Decimal(2.0),
        "na"   -> Value.NA
      )
    ),
    Row(
      Map(
        "text" -> Value.Text(s"Row 3"),
        "int"  -> Value.Integer(3)
      )
    ),
    Row(
      Map(
        "text" -> Value.Text(s"Row 4"),
        "dec"  -> Value.Decimal(2.0),
        "na"   -> Value.NA
      )
    )
  )

}
