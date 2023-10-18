package net.degoes

import zio.Chunk

import scala.collection.mutable.Map as MutableMap

object dataset7 {

  sealed trait Value
  object Value {
    final case class Text(value: String)    extends Value
    final case class Integer(value: Long)   extends Value
    final case class Decimal(value: Double) extends Value
    case object NA                          extends Value
  }

  final case class Field(name: String) extends AnyVal

  final case class Row(map: Map[String, Value])

  object Row {
    val empty = Row(Map.empty)
  }

  object Dataset {

    private sealed trait ValueType

    private object ValueType {

      case object Integer extends ValueType
      case object Decimal extends ValueType
      case object Text    extends ValueType
      case object NA      extends ValueType

      def fromValue(v: Value): ValueType =
        v match {
          case Value.Integer(_) => Integer
          case Value.Decimal(_) => Decimal
          case Value.Text(_)    => Text
          case Value.NA         => NA
        }
    }

    def fromRows(rows: Chunk[Row]): Dataset = {

      val fields: Map[Field, ValueType] =
        rows.flatMap(r => r.map.map { case (k, v) => Field(k) -> ValueType.fromValue(v) }).toMap

      val columns = new java.util.HashMap[Field, Array[?]]()

      def updateColumns(field: Field, fieldType: ValueType, row: Row, rowIndex: Int): Unit = {

        val fieldValue = row.map.getOrElse(field.name, null)
        if (fieldValue eq null) return
        var column = columns.get(field)

        if (column eq null) {
          if (fieldType eq ValueType.Integer) {
            column = new Array[Long](rows.length * 2)
            columns.put(field, column)
          } else if (fieldType eq ValueType.Decimal) {
            column = new Array[Double](rows.length * 2)
            columns.put(field, column)
          } else if (fieldType eq ValueType.Text) {
            column = new Array[String](rows.length)
            columns.put(field, column)
          } else if (fieldType eq ValueType.NA) {
            column = new Array[Boolean](rows.length)
            columns.put(field, column)
          }
        }

        if (fieldType eq ValueType.Integer) {
          val existsIndex = rowIndex * 2
          val valueIndex  = existsIndex + 1
          column.asInstanceOf[Array[Long]](existsIndex) = 1L
          column.asInstanceOf[Array[Long]](valueIndex) = fieldValue.asInstanceOf[Value.Integer].value
        } else if (fieldType eq ValueType.Decimal) {
          val existsIndex = rowIndex * 2
          val valueIndex  = existsIndex + 1
          column.asInstanceOf[Array[Double]](existsIndex) = 1.0
          column.asInstanceOf[Array[Double]](valueIndex) = fieldValue.asInstanceOf[Value.Decimal].value
        } else if (fieldType eq ValueType.Text) {
          column.asInstanceOf[Array[String]](rowIndex) = fieldValue.asInstanceOf[Value.Text].value
        } else if (fieldType eq ValueType.NA) {
          column.asInstanceOf[Array[Boolean]](rowIndex) = true
        }
      }

      for {
        (row, index)       <- rows.zipWithIndex
        (field, fieldType) <- fields
      } yield updateColumns(field, fieldType, row, index)

      new Dataset(rows.size, columns)
    }
  }

  final class Dataset(
    private val size: Int,
    private val columns: java.util.HashMap[Field, Array[?]]
  ) { self =>

    lazy val rows: Chunk[Row] = {

      def update(map: MutableMap[String, Value], rowIndex: Int, field: Field, column: Array[?]): Unit =
        if (column.isInstanceOf[Array[Long]]) {
          val existsIndex = rowIndex * 2
          val exists      = column.asInstanceOf[Array[Long]](existsIndex) > 0L
          if (exists) {
            val valueIndex = existsIndex + 1
            map.put(field.name, Value.Integer(column.asInstanceOf[Array[Long]](valueIndex))): Unit
          }
        } else if (column.isInstanceOf[Array[Double]]) {
          val existsIndex = rowIndex * 2
          val exists      = column.asInstanceOf[Array[Double]](existsIndex) > 0.0
          if (exists) {
            val valueIndex = existsIndex + 1
            map.put(field.name, Value.Decimal(column.asInstanceOf[Array[Double]](valueIndex))): Unit
          }
        } else if (column.isInstanceOf[Array[String]]) {
          val x = column.asInstanceOf[Array[String]](rowIndex)
          if (x ne null) map.put(field.name, Value.Text(x)): Unit
        } else if (column.isInstanceOf[Array[Boolean]]) {
          val x = column.asInstanceOf[Array[Boolean]](rowIndex)
          if (x) map.put(field.name, Value.NA): Unit
        }

      val rows = new Array[Row](size)

      var rowIndex = 0
      while (rowIndex < size) {
        val map      = MutableMap.empty[String, Value]
        val iterator = columns.entrySet().iterator()
        while (iterator.hasNext()) {
          val entry  = iterator.next()
          val field  = entry.getKey
          val column = entry.getValue
          update(map, rowIndex, field, column)
        }
        rows(rowIndex) = Row(map.toMap)
        rowIndex += 1
      }

      Chunk.fromArray(rows)
    }

    def apply(field: Field): Dataset = {
      val newColumns = new java.util.HashMap[Field, Array[?]]()
      newColumns.put(field, columns.get(field).clone())
      new Dataset(size, newColumns)
    }

    def +(that: Dataset): Dataset = binary(that, '+')
    def -(that: Dataset): Dataset = binary(that, '-')
    def *(that: Dataset): Dataset = binary(that, '*')
    def /(that: Dataset): Dataset = binary(that, '/')

    private def binary(that: Dataset, symbol: Char): Dataset = {

      val newColumns = new java.util.HashMap[Field, Array[?]]()

      val leftIterator = self.columns.entrySet().iterator()
      while (leftIterator.hasNext()) {
        val leftEntry     = leftIterator.next()
        val leftField     = leftEntry.getKey
        val leftColumn    = leftEntry.getValue
        val rightIterator = that.columns.entrySet().iterator()
        while (rightIterator.hasNext()) {
          val rightEntry   = rightIterator.next()
          val rightField   = rightEntry.getKey
          val rightColumn  = rightEntry.getValue
          val newFieldName = s"${leftField.name} $symbol ${rightField.name}"
          newColumns.put(Field(newFieldName), arrayOp(leftColumn, rightColumn, symbol))
        }
      }

      new Dataset(size, newColumns)
    }

    private def arrayOp(
      left: Array[?],
      right: Array[?],
      symbol: Char
    ): Array[?] = {
      var result: Array[?] = null
      var rowIndex         = 0
      while (rowIndex < size) {

        if (left.isInstanceOf[Array[Long]] && right.isInstanceOf[Array[Long]]) {
          val existsIndex = rowIndex * 2
          val leftExists  = left.asInstanceOf[Array[Long]](existsIndex) > 0L
          val rightExists = right.asInstanceOf[Array[Long]](existsIndex) > 0L
          if (leftExists && rightExists) {
            if (result eq null) result = new Array[Long](size * 2)
            val leftValue  = left.asInstanceOf[Array[Long]](existsIndex + 1)
            val rightValue = right.asInstanceOf[Array[Long]](existsIndex + 1)
            result.asInstanceOf[Array[Long]](existsIndex) = 1L
            result.asInstanceOf[Array[Long]](existsIndex + 1) = intOp(leftValue, rightValue, symbol)
          } else {
            if (result eq null) result = new Array[Boolean](size)
            result.asInstanceOf[Array[Boolean]](rowIndex) = true
          }
        } else if (left.isInstanceOf[Array[Double]] && right.isInstanceOf[Array[Double]]) {
          val existsIndex = rowIndex * 2
          val leftExists  = left.asInstanceOf[Array[Double]](existsIndex) > 0.0
          val rightExists = right.asInstanceOf[Array[Double]](existsIndex) > 0.0
          if (leftExists && rightExists) {
            if (result eq null) result = new Array[Double](size * 2)
            val leftValue  = left.asInstanceOf[Array[Double]](existsIndex + 1)
            val rightValue = right.asInstanceOf[Array[Double]](existsIndex + 1)
            result.asInstanceOf[Array[Double]](existsIndex) = 1.0
            result.asInstanceOf[Array[Double]](existsIndex + 1) = decOp(leftValue, rightValue, symbol)
          } else {
            if (result eq null) result = new Array[Boolean](size)
            result.asInstanceOf[Array[Boolean]](rowIndex) = true
          }
        } else if (left.isInstanceOf[Array[Double]] && right.isInstanceOf[Array[Long]]) {
          val existsIndex = rowIndex * 2
          val leftExists  = left.asInstanceOf[Array[Double]](existsIndex) > 0.0
          val rightExists = right.asInstanceOf[Array[Long]](existsIndex) > 0L
          if (leftExists && rightExists) {
            if (result eq null) result = new Array[Double](size * 2)
            val leftValue  = left.asInstanceOf[Array[Double]](existsIndex + 1)
            val rightValue = right.asInstanceOf[Array[Long]](existsIndex + 1).toDouble
            result.asInstanceOf[Array[Double]](existsIndex) = 1.0
            result.asInstanceOf[Array[Double]](existsIndex + 1) = decOp(leftValue, rightValue, symbol)
          } else {
            if (result eq null) result = new Array[Boolean](size)
            result.asInstanceOf[Array[Boolean]](rowIndex) = true
          }
        } else if (left.isInstanceOf[Array[Long]] && right.isInstanceOf[Array[Double]]) {
          val existsIndex = rowIndex * 2
          val leftExists  = left.asInstanceOf[Array[Long]](existsIndex) > 0L
          val rightExists = right.asInstanceOf[Array[Double]](existsIndex) > 0.0
          if (leftExists && rightExists) {
            if (result eq null) result = new Array[Double](size * 2)
            val leftValue  = left.asInstanceOf[Array[Long]](existsIndex + 1).toDouble
            val rightValue = right.asInstanceOf[Array[Double]](existsIndex + 1)
            result.asInstanceOf[Array[Double]](existsIndex) = 1.0
            result.asInstanceOf[Array[Double]](existsIndex + 1) = decOp(leftValue, rightValue, symbol)
          } else {
            if (result eq null) result = new Array[Boolean](size)
            result.asInstanceOf[Array[Boolean]](rowIndex) = true
          }
        } else {
          if (result eq null) result = new Array[Boolean](size)
          result.asInstanceOf[Array[Boolean]](rowIndex) = true
        }

        rowIndex += 1
      }
      result
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
