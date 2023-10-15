package net.degoes

import zio.Chunk
import scala.jdk.CollectionConverters.*

object dataset6 {

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

    private def getType(value: Value): Char =
      value match {
        case Value.Integer(_) => 'I'
        case Value.Decimal(_) => 'D'
        case Value.Text(_)    => 'T'
        case Value.NA         => 'X'
      }

    private def getOrCreateInts(data: Array[Any], fieldIndex: Int, size: Int): Array[Long] =
      if (data(fieldIndex) == null) {
        val ints = new Array[Long](size)
        data(fieldIndex) = ints
        ints
      } else data(fieldIndex).asInstanceOf[Array[Long]]

    private def getOrCreateDecs(data: Array[Any], fieldIndex: Int, size: Int): Array[Double] =
      if (data(fieldIndex) == null) {
        val decs = new Array[Double](size)
        data(fieldIndex) = decs
        decs
      } else data(fieldIndex).asInstanceOf[Array[Double]]

    private def getOrCreateText(data: Array[Any], fieldIndex: Int, size: Int): Array[String] =
      if (data(fieldIndex) == null) {
        val text = new Array[String](size)
        data(fieldIndex) = text
        text
      } else data(fieldIndex).asInstanceOf[Array[String]]

    private def getOrCreateNA(data: Array[Any], fieldIndex: Int, size: Int): Array[Boolean] =
      if (data(fieldIndex) == null) {
        val na = new Array[Boolean](size)
        data(fieldIndex) = na
        na
      } else data(fieldIndex).asInstanceOf[Array[Boolean]]

    private def createTypesArray(rowSize: Int, fieldSize: Int): Array[Array[Char]] = {
      val array = new Array[Array[Char]](fieldSize)
      var i     = 0
      while (i < array.length) {
        array(i) = new Array[Char](rowSize)
        i += 1
      }
      array
    }

    def fromRows(rows: Chunk[Row]): Dataset = {

      val fieldNames: Array[String] = rows.flatMap(_.map.keys).distinct.sorted.toArray
      val fieldTypes: Array[Char]   = rows.flatMap(_.map.map { case (k, v) => k -> getType(v) }).distinct.sortBy(_._1).map(_._2).toArray
      val data: Array[Any]          = new Array[Any](fieldNames.length)
      val types: Array[Array[Char]] = createTypesArray(rows.length, fieldNames.length)

      var rowIndex = 0
      while (rowIndex < rows.length) {
        val row        = rows(rowIndex)
        var fieldIndex = 0
        while (fieldIndex < fieldNames.length) {

          val fieldName = fieldNames(fieldIndex)

          row.map.get(fieldName) match {

            case Some(Value.Integer(v)) =>
              val ints: Array[Long] = getOrCreateInts(data, fieldIndex, rows.length)
              ints(rowIndex) = v
              types(fieldIndex)(rowIndex) = 'I'

            case Some(Value.Decimal(v)) =>
              val decs: Array[Double] = getOrCreateDecs(data, fieldIndex, rows.length)
              decs(rowIndex) = v
              types(fieldIndex)(rowIndex) = 'D'

            case Some(Value.Text(v)) =>
              val text: Array[String] = getOrCreateText(data, fieldIndex, rows.length)
              text(rowIndex) = v
              types(fieldIndex)(rowIndex) = 'T'

            case Some(Value.NA) =>
              val na: Array[Boolean] = getOrCreateNA(data, fieldIndex, rows.length)
              na(rowIndex) = true
              types(fieldIndex)(rowIndex) = 'X'

            case None => ()
          }
          fieldIndex += 1
        }
        rowIndex += 1
      }

      new Dataset(rows.length, fieldNames, fieldTypes, types, data)
    }
  }

  final class Dataset(
    val size: Int,
    val fieldNames: Array[String],
    val fieldTypes: Array[Char],
    val types: Array[Array[Char]],
    val data: Array[Any]
  ) { self =>

    import Dataset.*

    def print =
      println(
        s"""
           |Dataset(
           |  size = $size,
           |  fieldNames = ${fieldNames.mkString("[", ",", "]")},
           |  fieldTypes = ${fieldTypes.mkString("[", ",", "]")},
           |  types = ${types.map(_.mkString("[", ",", "]")).mkString("[", ",", "]")},
           |  data = ${data.map(_.toString).mkString("[", ",", "]")}
           |)
           |""".stripMargin
      )

    lazy val rows: Chunk[Row] = {

      val rows     = new Array[Row](size)
      var rowIndex = 0

      while (rowIndex < size) {

        var fieldIndex = 0
        val map        = new java.util.HashMap[String, Value]()

        while (fieldIndex < fieldNames.length) {

          val fieldType      = types(fieldIndex)(rowIndex)
          lazy val fieldName = fieldNames(fieldIndex)

          // println(s"Row($rowIndex) - Field($fieldIndex) - $fieldName: $fieldType")

          if (fieldType == 'I') map.put(fieldName, Value.Integer(data(fieldIndex).asInstanceOf[Array[Long]](rowIndex)))
          else if (fieldType == 'D') map.put(fieldName, Value.Decimal(data(fieldIndex).asInstanceOf[Array[Double]](rowIndex)))
          else if (fieldType == 'T') map.put(fieldName, Value.Text(data(fieldIndex).asInstanceOf[Array[String]](rowIndex)))
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

      val fieldType = fieldTypes(fieldIndex)

      val newTypes: Array[Array[Char]] = {
        val dest = new Array[Char](size)
        System.arraycopy(types(fieldIndex), 0, dest, 0, size)
        Array[Array[Char]](dest)
      }

      val newData: Array[Any] =
        if (fieldType == 'I') {
          val dest = new Array[Long](size)
          System.arraycopy(data(fieldIndex).asInstanceOf[Array[Long]], 0, dest, 0, size)
          Array[Any](dest)
        } else if (fieldType == 'D') {
          val dest = new Array[Double](size)
          System.arraycopy(data(fieldIndex).asInstanceOf[Array[Double]], 0, dest, 0, size)
          Array[Any](dest)
        } else if (fieldType == 'T') {
          val dest = new Array[String](size)
          System.arraycopy(data(fieldIndex).asInstanceOf[Array[String]], 0, dest, 0, size)
          Array[Any](dest)
        } else if (fieldType == 'X') {
          val dest = new Array[Boolean](size)
          System.arraycopy(data(fieldIndex).asInstanceOf[Array[Boolean]], 0, dest, 0, size)
          Array[Any](dest)
        } else throw new RuntimeException(s"Invalid field type - $fieldType")

      new Dataset(
        size,
        Array(field.name),
        Array(fieldType),
        newTypes,
        newData
      )
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

      val newData: Array[Any]          = new Array[Any](newFieldNames.length)
      val newTypes: Array[Array[Char]] = createTypesArray(rows.length, newFieldNames.length)
      val newFieldTypes: Array[Char]   = new Array[Char](newFieldNames.length)

      var rowIndex = 0
      while (rowIndex < self.size) {
        var newFieldIndex  = 0
        var leftFieldIndex = 0
        while (leftFieldIndex < self.fieldNames.length) {
          var rightFieldIndex = 0
          while (rightFieldIndex < that.fieldNames.length) {

            lazy val leftType  = self.types(leftFieldIndex)(rowIndex)
            lazy val rightType = that.types(rightFieldIndex)(rowIndex)

            val isInteger = leftType == 'I' && rightType == 'I'
            val isNumeric = (leftType == 'I' || leftType == 'D') && (rightType == 'I' || rightType == 'D')

            if (isInteger) {
              val v = intOp(
                self.data(leftFieldIndex).asInstanceOf[Array[Long]](rowIndex),
                that.data(rightFieldIndex).asInstanceOf[Array[Long]](rowIndex),
                symbol
              )
              val newInts = getOrCreateInts(newData, newFieldIndex, size)
              newInts(rowIndex) = v
              newTypes(newFieldIndex)(rowIndex) = 'I'
              newFieldTypes(newFieldIndex) = 'I'
            } else if (isNumeric) {
              val v = decOp(
                self.getDecimal(leftFieldIndex, rowIndex),
                that.getDecimal(rightFieldIndex, rowIndex),
                symbol
              )
              val newDecs = getOrCreateDecs(newData, newFieldIndex, size)
              newDecs(rowIndex) = v
              newTypes(newFieldIndex)(rowIndex) = 'D'
              newFieldTypes(newFieldIndex) = 'D'
            } else {
              val newNA = getOrCreateNA(newData, newFieldIndex, size)
              newNA(rowIndex) = true
              newTypes(newFieldIndex)(rowIndex) = 'X'
              newFieldTypes(newFieldIndex) = 'X'
            }

            rightFieldIndex += 1
            newFieldIndex += 1
          }
          leftFieldIndex += 1
        }
        rowIndex += 1
      }

      new Dataset(size, newFieldNames, fieldTypes, newTypes, newData)
    }

    private def getDecimal(fieldIndex: Int, rowIndex: Int): Double =
      if (types(fieldIndex)(rowIndex) == 'I') data(fieldIndex).asInstanceOf[Array[Long]](rowIndex).toDouble
      else data(fieldIndex).asInstanceOf[Array[Double]](rowIndex)

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
