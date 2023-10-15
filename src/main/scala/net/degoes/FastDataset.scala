package net.degoes

import zio.Chunk
import scala.jdk.CollectionConverters.*

object dataset2 {

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

  object Dataset {

    def fromRows(rows: Chunk[Row]): Dataset =
      new Dataset(MutableData.fromRows(rows))

    private object MutableData {

      def fromRows(rows: Chunk[Row]): MutableData = {

        val fieldNames: Chunk[String] = rows.flatMap(_.map.keys).distinct.sorted
        val data: MutableData         = allocate(fieldNames.length, rows.length)

        def updateRow(row: Row, rowIndex: Int): Unit = {

          val textRow             = data.textRows(rowIndex)
          val integerRow          = data.integerRows(rowIndex)
          val integerRowDefinedAt = data.integerRowsDefinedAt(rowIndex)
          val decimalRow          = data.decimalRows(rowIndex)
          val decimalRowDefinedAt = data.decimalRowsDefinedAt(rowIndex)
          val naRow               = data.naRows(rowIndex)

          fieldNames.zipWithIndex.foreach { case (fieldName, fieldIndex) =>
            data.fieldNames(fieldIndex) = fieldName
            row.map.get(fieldName) match {

              case None => ()

              case Some(Value.Text(v)) =>
                textRow(fieldIndex) = v

              case Some(Value.Integer(v)) =>
                integerRow(fieldIndex) = v
                integerRowDefinedAt(fieldIndex) = true

              case Some(Value.Decimal(v)) =>
                decimalRow(fieldIndex) = v
                decimalRowDefinedAt(fieldIndex) = true

              case Some(Value.NA) =>
                naRow(fieldIndex) = true
            }
          }

          data.textRows(rowIndex) = textRow
          data.integerRows(rowIndex) = integerRow
          data.integerRowsDefinedAt(rowIndex) = integerRowDefinedAt
          data.decimalRows(rowIndex) = decimalRow
          data.decimalRowsDefinedAt(rowIndex) = decimalRowDefinedAt
          data.naRows(rowIndex) = naRow
        }

        rows.zipWithIndex.foreach { case (row, rowIndex) =>
          updateRow(row, rowIndex)
        }

        data
      }

      def allocate(fieldSize: Int, rowSize: Int): MutableData = {

        val fieldNames           = new Array[String](fieldSize)
        val textRows             = new Array[Array[String]](rowSize)
        val integerRows          = new Array[Array[Long]](rowSize)
        val integerRowsDefinedAt = new Array[Array[Boolean]](rowSize)
        val decimalRows          = new Array[Array[Double]](rowSize)
        val decimalRowsDefinedAt = new Array[Array[Boolean]](rowSize)
        val naRows               = new Array[Array[Boolean]](rowSize)

        var rowIndex = 0
        while (rowIndex < rowSize) {
          textRows(rowIndex) = new Array[String](fieldSize)
          integerRows(rowIndex) = new Array[Long](fieldSize)
          integerRowsDefinedAt(rowIndex) = new Array[Boolean](fieldSize)
          decimalRows(rowIndex) = new Array[Double](fieldSize)
          decimalRowsDefinedAt(rowIndex) = new Array[Boolean](fieldSize)
          naRows(rowIndex) = new Array[Boolean](fieldSize)
          rowIndex = rowIndex + 1
        }

        new MutableData(
          fieldNames,
          textRows,
          integerRows,
          integerRowsDefinedAt,
          decimalRows,
          decimalRowsDefinedAt,
          naRows
        )
      }
    }

    private final case class MutableData(
      fieldNames: Array[String],
      textRows: Array[Array[String]],
      integerRows: Array[Array[Long]],
      integerRowsDefinedAt: Array[Array[Boolean]],
      decimalRows: Array[Array[Double]],
      decimalRowsDefinedAt: Array[Array[Boolean]],
      naRows: Array[Array[Boolean]] = Array.empty
    ) {

      // def print = {
      //   println("fieldNames: \n" + fieldNames.mkString(", "))
      //   println("textRows: \n" + textRows.map(_.mkString(", ")).mkString("\n"))
      //   println("integerRows: \n" + integerRows.map(_.mkString(", ")).mkString("\n"))
      //   println("integerRowsDefinedAt: \n" + integerRowsDefinedAt.map(_.mkString(", ")).mkString("\n"))
      //   println("decimalRows: \n" + decimalRows.map(_.mkString(", ")).mkString("\n"))
      //   println("decimalRowsDefinedAt: \n" + decimalRowsDefinedAt.map(_.mkString(", ")).mkString("\n"))
      //   println("naRows: \n" + naRows.map(_.mkString(", ")).mkString("\n"))
      // }

      def rows: Chunk[Row] = {

        val rows     = new Array[Row](textRows.length)
        var rowIndex = 0
        while (rowIndex < textRows.length) {
          rows(rowIndex) = toRow(rowIndex)
          rowIndex = rowIndex + 1
        }
        Chunk.fromArray(rows)
      }

      private def toRow(rowIndex: Int): Row = {

        val map = new java.util.HashMap[String, Value]()

        var fieldIndex = 0
        while (fieldIndex < fieldNames.length) {

          val fieldName = fieldNames(fieldIndex)

          if (isIntegerDefinedAt(rowIndex, fieldIndex)) {
            val value = getInteger(rowIndex, fieldIndex)
            map.put(fieldName, Value.Integer(value))
          }

          if (isDecimalDefinedAt(rowIndex, fieldIndex)) {
            val value = getDecimal(rowIndex, fieldIndex)
            map.put(fieldName, Value.Decimal(value))
          }

          val text = getText(rowIndex, fieldIndex)
          val na   = getNA(rowIndex, fieldIndex)
          if (text ne null) map.put(fieldName, Value.Text(text))
          if (na) map.put(fieldName, Value.NA)

          fieldIndex = fieldIndex + 1
        }

        new Row(map.asScala.toMap)
      }

      def setFieldName(that: MutableData, leftFieldIndex: Int, rightFieldIndex: Int, output: MutableData, outputFieldIndex: Int, symbol: Char): Unit = {
        val fieldName = s"${fieldNames(leftFieldIndex)} $symbol ${that.fieldNames(rightFieldIndex)}"
        output.fieldNames(outputFieldIndex) = fieldName
      }

      def addition(that: MutableData, rowIndex: Int, leftFieldIndex: Int, rightFieldIndex: Int, output: MutableData, outputFieldIndex: Int): Unit =
        operation(that, rowIndex, leftFieldIndex, rightFieldIndex, output, outputFieldIndex, _ + _, _ + _)

      def subtraction(that: MutableData, rowIndex: Int, leftFieldIndex: Int, rightFieldIndex: Int, output: MutableData, outputFieldIndex: Int): Unit =
        operation(that, rowIndex, leftFieldIndex, rightFieldIndex, output, outputFieldIndex, _ - _, _ - _)

      def multiplication(that: MutableData, rowIndex: Int, leftFieldIndex: Int, rightFieldIndex: Int, output: MutableData, outputFieldIndex: Int): Unit =
        operation(that, rowIndex, leftFieldIndex, rightFieldIndex, output, outputFieldIndex, _ * _, _ * _)

      def division(that: MutableData, rowIndex: Int, leftFieldIndex: Int, rightFieldIndex: Int, output: MutableData, outputFieldIndex: Int): Unit =
        operation(that, rowIndex, leftFieldIndex, rightFieldIndex, output, outputFieldIndex, _ / _, _ / _)

      private def operation(
        that: MutableData,
        rowIndex: Int,
        leftFieldIndex: Int,
        rightFieldIndex: Int,
        output: MutableData,
        outputFieldIndex: Int,
        integerOp: (Long, Long) => Long,
        decimalOp: (Double, Double) => Double
      ): Unit = {

        val isLeftIntegerDefined  = isIntegerDefinedAt(rowIndex, leftFieldIndex)
        val isRightIntegerDefined = that.isIntegerDefinedAt(rowIndex, rightFieldIndex)

        lazy val isLeftNumberDefined  = isNumberDefinedAt(rowIndex, leftFieldIndex)
        lazy val isRightNumberDefined = that.isNumberDefinedAt(rowIndex, rightFieldIndex)

        if (isLeftIntegerDefined && isRightIntegerDefined) {
          val leftInteger  = getInteger(rowIndex, leftFieldIndex)
          val rightInteger = that.getInteger(rowIndex, rightFieldIndex)
          updateInteger(output, rowIndex, outputFieldIndex, integerOp(leftInteger, rightInteger))
        } else if (isLeftNumberDefined && isRightNumberDefined) {
          val leftNumber  = getIntegerOrDecimal(rowIndex, leftFieldIndex)
          val rightNumber = that.getIntegerOrDecimal(rowIndex, rightFieldIndex)
          updateDecimal(output, rowIndex, outputFieldIndex, decimalOp(leftNumber, rightNumber))
        } else {
          updateNA(output, rowIndex, outputFieldIndex)
        }
      }

      private def cannotAccessArray(array: Array[?], index: Int): Boolean =
        (array eq null) ||
          (index < 0) ||
          (index >= array.length)

      private def isIntegerDefinedAt(
        rowIndex: Int,
        fieldIndex: Int
      ): Boolean = {
        if (cannotAccessArray(integerRowsDefinedAt, rowIndex)) return false
        val row = integerRowsDefinedAt(rowIndex)
        if (cannotAccessArray(row, fieldIndex)) return false
        row(fieldIndex)
      }

      private def isDecimalDefinedAt(
        rowIndex: Int,
        fieldIndex: Int
      ): Boolean = {
        if (cannotAccessArray(decimalRowsDefinedAt, rowIndex)) return false
        val row = decimalRowsDefinedAt(rowIndex)
        if (cannotAccessArray(row, fieldIndex)) return false
        row(fieldIndex)
      }

      private def isNumberDefinedAt(
        rowIndex: Int,
        fieldIndex: Int
      ): Boolean = isIntegerDefinedAt(rowIndex, fieldIndex) || isDecimalDefinedAt(rowIndex, fieldIndex)

      def getNA(rowIndex: Int, fieldIndex: Int): Boolean = {
        if (cannotAccessArray(naRows, rowIndex)) return false
        val row = naRows(rowIndex)
        if (cannotAccessArray(row, fieldIndex)) return false
        row(fieldIndex)
      }

      private def getText(rowIndex: Int, fieldIndex: Int): String = {
        if (cannotAccessArray(textRows, rowIndex)) return null
        val row = textRows(rowIndex)
        if (cannotAccessArray(row, fieldIndex)) return null
        row(fieldIndex)
      }

      private def getInteger(rowIndex: Int, fieldIndex: Int): Long = {
        if (cannotAccessArray(integerRows, rowIndex)) 0L
        val row = integerRows(rowIndex)
        if (cannotAccessArray(row, fieldIndex)) return 0L
        row(fieldIndex)
      }

      private def getDecimal(rowIndex: Int, fieldIndex: Int): Double = {
        if (cannotAccessArray(decimalRows, rowIndex)) 0.0
        val row = decimalRows(rowIndex)
        if (cannotAccessArray(row, fieldIndex)) return 0.0
        row(fieldIndex)
      }

      private def getIntegerOrDecimal(rowIndex: Int, fieldIndex: Int): Double =
        if (isIntegerDefinedAt(rowIndex, fieldIndex)) getInteger(rowIndex, fieldIndex).toDouble
        else getDecimal(rowIndex, fieldIndex)

      private def updateInteger(
        output: MutableData,
        rowIndex: Int,
        fieldIndex: Int,
        value: Long
      ): Unit = {
        output.integerRows(rowIndex)(fieldIndex) = value
        output.integerRowsDefinedAt(rowIndex)(fieldIndex) = true
      }

      private def updateDecimal(
        output: MutableData,
        rowIndex: Int,
        fieldIndex: Int,
        value: Double
      ): Unit = {
        output.decimalRows(rowIndex)(fieldIndex) = value
        output.decimalRowsDefinedAt(rowIndex)(fieldIndex) = true
      }

      private def updateNA(output: MutableData, rowIndex: Int, fieldIndex: Int): Unit =
        output.naRows(rowIndex)(fieldIndex) = true
    }
  }

  final class Dataset(private val data: Dataset.MutableData) { self =>

    lazy val rows: Chunk[Row] = data.rows

    private lazy val output: Dataset.MutableData =
      Dataset.MutableData.allocate(
        data.fieldNames.length * data.fieldNames.length,
        data.textRows.length
      )

    def apply(field: Field): Dataset = {

      val textRowsCopy             = new Array[Array[String]](data.textRows.length)
      val integerRowsCopy          = new Array[Array[Long]](data.integerRows.length)
      val integerRowsDefinedAtCopy = new Array[Array[Boolean]](data.integerRowsDefinedAt.length)
      val decimalRowsCopy          = new Array[Array[Double]](data.decimalRows.length)
      val decimalRowsDefinedAtCopy = new Array[Array[Boolean]](data.decimalRowsDefinedAt.length)
      val naRowsCopy               = new Array[Array[Boolean]](data.naRows.length)

      val fieldIndex = data.fieldNames.indexOf(field.name)
      var rowIndex   = 0

      while (rowIndex < data.textRows.length) {
        textRowsCopy(rowIndex) = Array[String](data.textRows(rowIndex)(fieldIndex))
        integerRowsCopy(rowIndex) = Array[Long](data.integerRows(rowIndex)(fieldIndex))
        integerRowsDefinedAtCopy(rowIndex) = Array[Boolean](data.integerRowsDefinedAt(rowIndex)(fieldIndex))
        decimalRowsCopy(rowIndex) = Array[Double](data.decimalRows(rowIndex)(fieldIndex))
        decimalRowsDefinedAtCopy(rowIndex) = Array[Boolean](data.decimalRowsDefinedAt(rowIndex)(fieldIndex))
        naRowsCopy(rowIndex) = Array[Boolean](data.naRows(rowIndex)(fieldIndex))
        rowIndex = rowIndex + 1
      }

      new Dataset(
        new Dataset.MutableData(
          Array(field.name),
          textRowsCopy,
          integerRowsCopy,
          integerRowsDefinedAtCopy,
          decimalRowsCopy,
          decimalRowsDefinedAtCopy,
          naRowsCopy
        )
      )
    }

    def +(that: Dataset): Dataset = binary(that, '+')
    def -(that: Dataset): Dataset = binary(that, '-')
    def *(that: Dataset): Dataset = binary(that, '*')
    def /(that: Dataset): Dataset = binary(that, '/')

    private def binary(that: Dataset, symbol: Char): Dataset = {

      var rowIndex = 0
      while (rowIndex < data.textRows.length) {

        var outputIndex    = 0
        var leftFieldIndex = 0
        while (leftFieldIndex < data.fieldNames.size) {
          var rightFieldIndex = 0
          while (rightFieldIndex < that.data.fieldNames.size) {
            data.setFieldName(that.data, leftFieldIndex, rightFieldIndex, output, outputIndex, symbol)
            if (symbol == '+') data.addition(that.data, rowIndex, leftFieldIndex, rightFieldIndex, output, outputIndex)
            else if (symbol == '-') data.subtraction(that.data, rowIndex, leftFieldIndex, rightFieldIndex, output, outputIndex)
            else if (symbol == '*') data.multiplication(that.data, rowIndex, leftFieldIndex, rightFieldIndex, output, outputIndex)
            else if (symbol == '/') data.division(that.data, rowIndex, leftFieldIndex, rightFieldIndex, output, outputIndex)
            else ()
            rightFieldIndex = rightFieldIndex + 1
            outputIndex = outputIndex + 1
          }
          leftFieldIndex = leftFieldIndex + 1
        }

        rowIndex = rowIndex + 1
      }

      new Dataset(output)
    }
  }
}
