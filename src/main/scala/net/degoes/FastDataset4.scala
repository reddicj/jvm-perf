package net.degoes

import zio.Chunk
import scala.jdk.CollectionConverters.*

object dataset5 {

  sealed trait Value
  object Value {
    final case class Text(value: String)    extends Value
    final case class Integer(value: Long)   extends Value
    final case class Decimal(value: Double) extends Value
    case object NA                          extends Value
  }

  final case class Field(name: String) extends AnyVal

  final case class Row(map: Map[String, Value]) {
    def apply(field: Field): Value = map(field.name)
  }

  object Row {
    val empty = Row(Map.empty)
  }

  object Dataset {

    def fromRows(rows: Chunk[Row]): Dataset = {

      val fieldNames = rows.flatMap(_.map.keys).distinct.sorted
      val ints       = new Array[Long](rows.length * fieldNames.length)
      val decs       = new Array[Double](rows.length * fieldNames.length)
      val text       = new Array[String](rows.length * fieldNames.length)
      val na         = new Array[Boolean](rows.length * fieldNames.length)
      val types      = new Array[Char](rows.length * fieldNames.length)

      var rowIndex = 0
      while (rowIndex < rows.length) {
        val row        = rows(rowIndex)
        var fieldIndex = 0
        while (fieldIndex < fieldNames.length) {

          val fieldName = fieldNames(fieldIndex)
          val i         = index(rowIndex, fieldIndex, fieldNames.length)

          row.map.get(fieldName) match {

            case Some(Value.Integer(v)) =>
              ints(i) = v
              decs(i) = v.toDouble
              types(i) = 'I'

            case Some(Value.Decimal(v)) =>
              decs(i) = v
              types(i) = 'D'

            case Some(Value.Text(v)) =>
              text(i) = v
              types(i) = 'T'

            case Some(Value.NA) =>
              na(i) = true
              types(i) = 'X'

            case None => ()
          }
          fieldIndex += 1
        }
        rowIndex += 1
      }

      new Dataset(rows.length, fieldNames.toArray, ints, decs, text, na, types)
    }

    private def index(rowIndex: Int, fieldIndex: Int, fieldSize: Int): Int =
      (rowIndex * fieldSize) + fieldIndex
  }

  final class Dataset(
    private val size: Int,
    private val fieldNames: Array[String],
    private val ints: Array[Long],
    private val decs: Array[Double],
    private val text: Array[String],
    private val na: Array[Boolean],
    private val types: Array[Char]
  ) { self =>

    import Dataset.*

    // def print = {
    //   println(s"size: $size")
    //   println(s"fieldNames: ${fieldNames.mkString(", ")}")
    //   println(s"ints: ${ints.mkString(", ")}")
    //   println(s"decs: ${decs.mkString(", ")}")
    //   println(s"text: ${text.mkString(", ")}")
    //   println(s"na: ${na.mkString(", ")}")
    // }

    lazy val rows: Chunk[Row] = {

      val rows     = new Array[Row](size)
      var rowIndex = 0

      while (rowIndex < size) {

        var fieldIndex = 0
        val map        = new java.util.HashMap[String, Value]()

        while (fieldIndex < fieldNames.length) {

          lazy val fieldName = fieldNames(fieldIndex)
          val i              = index(rowIndex, fieldIndex, fieldNames.length)
          val fieldType      = types(i)

          if (fieldType == 'I') map.put(fieldName, Value.Integer(ints(i)))
          else if (fieldType == 'D') map.put(fieldName, Value.Decimal(decs(i)))
          else if (fieldType == 'T') map.put(fieldName, Value.Text(text(i)))
          else if (fieldType == 'X') map.put(fieldName, Value.NA)

          rows(rowIndex) = Row(map.asScala.toMap)

          fieldIndex += 1
        }
        rowIndex += 1
      }

      Chunk.fromArray(rows)
    }

    def apply(field: Field): Dataset = {

      val fieldIndex = fieldNames.indexOf(field.name)
      if (fieldIndex < 0) throw new RuntimeException(s"Field ${field.name} not found")

      val newFieldNames = Array[String](field.name)
      val newInts       = new Array[Long](size)
      val newDecs       = new Array[Double](size)
      val newText       = new Array[String](size)
      val newNA         = new Array[Boolean](size)
      val newTypes      = new Array[Char](size)

      var rowIndex = 0
      while (rowIndex < size) {

        val fieldType = types(index(rowIndex, fieldIndex, fieldNames.length))

        if (fieldType == 'I') {
          val v = self.ints(index(rowIndex, fieldIndex, fieldNames.length))
          newInts(index(rowIndex, 0, 1)) = v
          newDecs(index(rowIndex, 0, 1)) = v.toDouble
          newTypes(index(rowIndex, 0, 1)) = 'I'
        } else if (fieldType == 'D') {
          newDecs(index(rowIndex, 0, 1)) = self.decs(index(rowIndex, fieldIndex, fieldNames.length))
          newTypes(index(rowIndex, 0, 1)) = 'D'
        } else if (fieldType == 'T') {
          newText(index(rowIndex, 0, 1)) = self.text(index(rowIndex, fieldIndex, fieldNames.length))
          newTypes(index(rowIndex, 0, 1)) = 'T'
        } else if (fieldType == 'X') {
          newNA(index(rowIndex, 0, 1)) = true
          newTypes(index(rowIndex, 0, 1)) = 'X'
        }

        rowIndex += 1
      }

      new Dataset(size, newFieldNames, newInts, newDecs, newText, newNA, newTypes)
    }

    def +(that: Dataset): Dataset = binary(that, '+')
    def -(that: Dataset): Dataset = binary(that, '-')
    def *(that: Dataset): Dataset = binary(that, '*')
    def /(that: Dataset): Dataset = binary(that, '/')

    private def binary(that: Dataset, symbol: Char): Dataset = {

      val newFieldNames: Array[String] =
        for {
          left  <- self.fieldNames
          right <- that.fieldNames
        } yield s"$left $symbol $right"

      val newInts  = new Array[Long](newFieldNames.length * size)
      val newDecs  = new Array[Double](newFieldNames.length * size)
      val newNA    = new Array[Boolean](newFieldNames.length * size)
      val newText  = Array.empty[String]
      val newTypes = new Array[Char](newFieldNames.length * size)

      var rowIndex = 0
      while (rowIndex < self.size) {
        var newIndexValue  = 0
        var leftFieldIndex = 0
        while (leftFieldIndex < self.fieldNames.length) {
          var rightFieldIndex = 0
          while (rightFieldIndex < that.fieldNames.length) {

            val leftIndex  = index(rowIndex, leftFieldIndex, self.fieldNames.length)
            val rightIndex = index(rowIndex, rightFieldIndex, that.fieldNames.length)
            val newIndex   = index(rowIndex, newIndexValue, newFieldNames.length)

            lazy val leftType  = self.types(leftIndex)
            lazy val rightType = that.types(rightIndex)

            val isInteger = leftType == 'I' && rightType == 'I'
            val isNumeric = (leftType == 'I' || leftType == 'D') && (rightType == 'I' || rightType == 'D')

            if (isInteger) {
              val v = intOp(
                self.ints(leftIndex),
                that.ints(rightIndex),
                symbol
              )
              newInts(newIndex) = v
              newTypes(newIndex) = 'I'
            } else if (isNumeric) {
              val v = decOp(
                self.decs(leftIndex),
                that.decs(rightIndex),
                symbol
              )
              newDecs(newIndex) = v
              newTypes(newIndex) = 'D'
            } else {
              newNA(newIndex) = true
              newTypes(newIndex) = 'X'
            }

            rightFieldIndex += 1
            newIndexValue += 1
          }
          leftFieldIndex += 1
        }
        rowIndex += 1
      }

      new Dataset(size, newFieldNames, newInts, newDecs, newText, newNA, newTypes)
    }

    private def intOp(
      left: Long,
      right: Long,
      symbol: Char
    ): Long =
      if (symbol == '+') left + right
      else if (symbol == '-') left - right
      else if (symbol == '*') left * right
      else if (symbol == '/') left / right
      else throw new RuntimeException(s"Invalid symbol - $symbol")

    private def decOp(
      left: Double,
      right: Double,
      symbol: Char
    ): Double =
      if (symbol == '+') left + right
      else if (symbol == '-') left - right
      else if (symbol == '*') left * right
      else if (symbol == '/') left / right
      else throw new RuntimeException(s"Invalid symbol - $symbol")
  }
}
