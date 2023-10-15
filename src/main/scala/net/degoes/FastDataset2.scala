package net.degoes

import zio.Chunk
import scala.jdk.CollectionConverters.*

object dataset3 {

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
      val ints       = new Array[Long](rows.length * fieldNames.length * 2)
      val decs       = new Array[Double](rows.length * fieldNames.length * 2)
      val text       = new Array[String](rows.length * fieldNames.length)
      val na         = new Array[Boolean](rows.length * fieldNames.length)

      val dataByFieldName = new Array[DatasetArrays](fieldNames.length)
      for (i <- 0 until fieldNames.length) dataByFieldName(i) = DatasetArrays.make(rows.length)

      var rowIndex = 0
      while (rowIndex < rows.length) {
        val row        = rows(rowIndex)
        var fieldIndex = 0
        while (fieldIndex < fieldNames.length) {
          val fieldName = fieldNames(fieldIndex)
          row.map.get(fieldName) match {

            case Some(Value.Integer(v)) =>
              ints(isNumberDefinedIndex(rowIndex, fieldIndex, fieldNames.length)) = 1
              ints(numberIndex(rowIndex, fieldIndex, fieldNames.length)) = v
              dataByFieldName(fieldIndex).ints(isNumberDefinedIndex(rowIndex, 0, 1)) = 1
              dataByFieldName(fieldIndex).ints(numberIndex(rowIndex, 0, 1)) = v

            case Some(Value.Decimal(v)) =>
              decs(isNumberDefinedIndex(rowIndex, fieldIndex, fieldNames.length)) = 1.0
              decs(numberIndex(rowIndex, fieldIndex, fieldNames.length)) = v
              dataByFieldName(fieldIndex).decs(isNumberDefinedIndex(rowIndex, 0, 1)) = 1.0
              dataByFieldName(fieldIndex).decs(numberIndex(rowIndex, 0, 1)) = v

            case Some(Value.Text(v)) =>
              text(textIndex(rowIndex, fieldIndex, fieldNames.length)) = v
              dataByFieldName(fieldIndex).text(textIndex(rowIndex, 0, 1)) = v

            case Some(Value.NA) =>
              na(naIndex(rowIndex, fieldIndex, fieldNames.length)) = true
              dataByFieldName(fieldIndex).na(naIndex(rowIndex, 0, 1)) = true

            case None => ()
          }
          fieldIndex += 1
        }
        rowIndex += 1
      }

      new Dataset(rows.length, fieldNames.toArray, ints, decs, text, na, dataByFieldName)
    }

    private def isNumberDefinedIndex(rowIndex: Int, fieldIndex: Int, fieldSize: Int): Int =
      ((rowIndex * fieldSize) + fieldIndex) * 2

    private def numberIndex(rowIndex: Int, fieldIndex: Int, fieldSize: Int): Int =
      isNumberDefinedIndex(rowIndex, fieldIndex, fieldSize) + 1

    private def textIndex(rowIndex: Int, fieldIndex: Int, fieldSize: Int): Int =
      (rowIndex * fieldSize) + fieldIndex

    private def naIndex(rowIndex: Int, fieldIndex: Int, fieldSize: Int): Int =
      (rowIndex * fieldSize) + fieldIndex

    private final case class DatasetArrays(
      ints: Array[Long],
      decs: Array[Double],
      text: Array[String],
      na: Array[Boolean]
    )

    private object DatasetArrays {

      def make(size: Int): DatasetArrays =
        DatasetArrays(
          new Array[Long](size * 2),
          new Array[Double](size * 2),
          new Array[String](size),
          new Array[Boolean](size)
        )
    }
  }

  final class Dataset(
    private val size: Int,
    private val fieldNames: Array[String],
    private val ints: Array[Long],
    private val decs: Array[Double],
    private val text: Array[String],
    private val na: Array[Boolean],
    private val byField: Array[Dataset.DatasetArrays]
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

          lazy val fieldName     = fieldNames(fieldIndex)
          lazy val isIntDefined  = isIntDefinedAt(self.ints, rowIndex, fieldIndex, fieldNames.length)
          lazy val isDecDefined  = isDecDefinedAt(self.decs, rowIndex, fieldIndex, fieldNames.length)
          lazy val isTextDefined = isTextDefinedAt(self.text, rowIndex, fieldIndex, fieldNames.length)
          lazy val isNaDefined   = isNaDefinedAt(self.na, rowIndex, fieldIndex, fieldNames.length)

          if (isIntDefined) map.put(fieldName, Value.Integer(ints(numberIndex(rowIndex, fieldIndex, fieldNames.length))))
          else if (isDecDefined) map.put(fieldName, Value.Decimal(decs(numberIndex(rowIndex, fieldIndex, fieldNames.length))))
          else if (isTextDefined) map.put(fieldName, Value.Text(text(textIndex(rowIndex, fieldIndex, fieldNames.length))))
          else if (isNaDefined) map.put(fieldName, Value.NA)

          rows(rowIndex) = Row(map.asScala.toMap)

          fieldIndex += 1
        }
        rowIndex += 1
      }

      Chunk.fromArray(rows)
    }

    // def apply(field: Field): Dataset = {

    //   val fieldIndex = fieldNames.indexOf(field.name)
    //   if (fieldIndex < 0) throw new RuntimeException(s"Field ${field.name} not found")

    //   val newFieldNames = Array[String](field.name)
    //   val newInts       = new Array[Long](size * 2)
    //   val newDecs       = new Array[Double](size * 2)
    //   val newText       = new Array[String](size)
    //   val newNA         = new Array[Boolean](size)

    //   var rowIndex = 0
    //   while (rowIndex < size) {

    //     lazy val isIntDefined  = isIntDefinedAt(self.ints, rowIndex, fieldIndex, fieldNames.length)
    //     lazy val isDecDefined  = isDecDefinedAt(self.decs, rowIndex, fieldIndex, fieldNames.length)
    //     lazy val isTextDefined = isTextDefinedAt(self.text, rowIndex, fieldIndex, fieldNames.length)
    //     lazy val isNaDefined   = isNaDefinedAt(self.na, rowIndex, fieldIndex, fieldNames.length)

    //     if (isIntDefined) {
    //       newInts(isNumberDefinedIndex(rowIndex, 0, 1)) = 1
    //       newInts(numberIndex(rowIndex, 0, 1)) = self.ints(numberIndex(rowIndex, fieldIndex, fieldNames.length))
    //     } else if (isDecDefined) {
    //       newDecs(isNumberDefinedIndex(rowIndex, 0, 1)) = 1.0
    //       newDecs(numberIndex(rowIndex, 0, 1)) = self.decs(numberIndex(rowIndex, fieldIndex, fieldNames.length))
    //     } else if (isTextDefined)
    //       newText(textIndex(rowIndex, 0, 1)) = self.text(textIndex(rowIndex, fieldIndex, fieldNames.length))
    //     else if (isNaDefined)
    //       newNA(naIndex(rowIndex, 0, 1)) = true

    //     rowIndex += 1
    //   }

    //   new Dataset(size, newFieldNames, newInts, newDecs, newText, newNA)
    // }

    def apply(field: Field): Dataset = {
      val fieldIndex = fieldNames.indexOf(field.name)
      if (fieldIndex < 0) throw new RuntimeException(s"Field ${field.name} not found")
      val arrays = byField(fieldIndex)
      new Dataset(size, Array(field.name), arrays.ints, arrays.decs, arrays.text, arrays.na, byField)
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

      val dataByFieldName = new Array[DatasetArrays](newFieldNames.length)
      for (i <- 0 until newFieldNames.length) dataByFieldName(i) = DatasetArrays.make(size)

      val newInts = new Array[Long](newFieldNames.length * 2 * size)
      val newDecs = new Array[Double](newFieldNames.length * 2 * size)
      val newNA   = new Array[Boolean](newFieldNames.length * size)
      val newText = Array.empty[String]

      var rowIndex = 0
      while (rowIndex < self.size) {
        var newIndexValue = 0
        var leftIndex     = 0
        while (leftIndex < self.fieldNames.length) {
          var rightIndex = 0
          while (rightIndex < that.fieldNames.length) {

            lazy val isLeftIntDefined  = isIntDefinedAt(self.ints, rowIndex, leftIndex, self.fieldNames.length)
            lazy val isLeftDecDefined  = isDecDefinedAt(self.decs, rowIndex, leftIndex, self.fieldNames.length)
            lazy val isRightIntDefined = isIntDefinedAt(that.ints, rowIndex, rightIndex, that.fieldNames.length)
            lazy val isRightDecDefined = isDecDefinedAt(that.decs, rowIndex, rightIndex, that.fieldNames.length)

            if (isLeftIntDefined) {

              if (isRightIntDefined) {
                val v = intOp(
                  self.ints(numberIndex(rowIndex, leftIndex, self.fieldNames.length)),
                  that.ints(numberIndex(rowIndex, rightIndex, that.fieldNames.length)),
                  symbol
                )
                newInts(isNumberDefinedIndex(rowIndex, newIndexValue, newFieldNames.length)) = 1
                newInts(numberIndex(rowIndex, newIndexValue, newFieldNames.length)) = v
                dataByFieldName(newIndexValue).ints(isNumberDefinedIndex(rowIndex, 0, 1)) = 1
                dataByFieldName(newIndexValue).ints(numberIndex(rowIndex, 0, 1)) = v
              } else if (isRightDecDefined) {
                val v = decOp(
                  self.ints(numberIndex(rowIndex, leftIndex, self.fieldNames.length)).toDouble,
                  that.decs(numberIndex(rowIndex, rightIndex, that.fieldNames.length)),
                  symbol
                )
                newDecs(isNumberDefinedIndex(rowIndex, newIndexValue, newFieldNames.length)) = 1.0
                newDecs(numberIndex(rowIndex, newIndexValue, newFieldNames.length)) = v
                dataByFieldName(newIndexValue).decs(isNumberDefinedIndex(rowIndex, 0, 1)) = 1.0
                dataByFieldName(newIndexValue).decs(numberIndex(rowIndex, 0, 1)) = v
              } else {
                newNA(newIndexValue) = true
                dataByFieldName(newIndexValue).na(naIndex(rowIndex, 0, 1)) = true
              }
            } else if (isLeftDecDefined) {
              if (isRightIntDefined) {
                val v = decOp(
                  self.decs(numberIndex(rowIndex, leftIndex, self.fieldNames.length)),
                  that.ints(numberIndex(rowIndex, rightIndex, that.fieldNames.length)).toDouble,
                  symbol
                )
                newDecs(isNumberDefinedIndex(rowIndex, newIndexValue, newFieldNames.length)) = 1.0
                newDecs(numberIndex(rowIndex, newIndexValue, newFieldNames.length)) = v
                dataByFieldName(newIndexValue).decs(isNumberDefinedIndex(rowIndex, 0, 1)) = 1.0
                dataByFieldName(newIndexValue).decs(numberIndex(rowIndex, 0, 1)) = v
              } else if (isRightDecDefined) {
                val v = decOp(
                  self.decs(numberIndex(rowIndex, leftIndex, self.fieldNames.length)),
                  that.decs(numberIndex(rowIndex, rightIndex, that.fieldNames.length)),
                  symbol
                )
                newDecs(isNumberDefinedIndex(rowIndex, newIndexValue, newFieldNames.length)) = 1.0
                newDecs(numberIndex(rowIndex, newIndexValue, newFieldNames.length)) = v
                dataByFieldName(newIndexValue).decs(isNumberDefinedIndex(rowIndex, 0, 1)) = 1.0
                dataByFieldName(newIndexValue).decs(numberIndex(rowIndex, 0, 1)) = v
              } else {
                newNA(newIndexValue) = true
                dataByFieldName(newIndexValue).na(naIndex(rowIndex, 0, 1)) = true
              }
            } else {
              newNA(newIndexValue) = true
              dataByFieldName(newIndexValue).na(naIndex(rowIndex, 0, 1)) = true
            }

            rightIndex += 1
            newIndexValue += 1
          }
          leftIndex += 1
        }
        rowIndex += 1
      }

      new Dataset(size, newFieldNames, newInts, newDecs, newText, newNA, dataByFieldName)
    }

    private def isIntDefinedAt(array: Array[Long], rowIndex: Int, fieldIndex: Int, fieldSize: Int): Boolean = {
      val index = isNumberDefinedIndex(rowIndex, fieldIndex, fieldSize)
      canAccessArray(array, index) && (array(index) > 0)
    }

    private def isDecDefinedAt(array: Array[Double], rowIndex: Int, fieldIndex: Int, fieldSize: Int): Boolean = {
      val index = isNumberDefinedIndex(rowIndex, fieldIndex, fieldSize)
      canAccessArray(array, index) && (array(index) > 0)
    }

    private def isTextDefinedAt(array: Array[String], rowIndex: Int, fieldIndex: Int, fieldSize: Int): Boolean = {
      val index = (rowIndex * fieldSize) + fieldIndex
      canAccessArray(array, index) && (array(index) ne null)
    }

    private def isNaDefinedAt(array: Array[Boolean], rowIndex: Int, fieldIndex: Int, fieldSize: Int): Boolean = {
      val index = (rowIndex * fieldSize) + fieldIndex
      canAccessArray(array, index) && array(index)
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

    private def cannotAccessArray(array: Array[?], index: Int): Boolean =
      (array eq null) ||
        (index < 0) ||
        (index >= array.length)

    private def canAccessArray(array: Array[?], index: Int): Boolean =
      !cannotAccessArray(array, index)
  }
}
