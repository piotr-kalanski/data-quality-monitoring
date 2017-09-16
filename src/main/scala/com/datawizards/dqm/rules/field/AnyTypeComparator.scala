package com.datawizards.dqm.rules.field

trait AnyTypeComparator {
  def compare(left: Any, right: Any): Int = {
    left match {
      case i:Int => left.asInstanceOf[Int].compareTo(right.asInstanceOf[Int])
      case l:Long => left.asInstanceOf[Long].compareTo(right.asInstanceOf[Long])
      case d:Double => left.asInstanceOf[Double].compareTo(right.asInstanceOf[Double])
      case f:Float => left.asInstanceOf[Float].compareTo(right.asInstanceOf[Float])
      case s:Short => left.asInstanceOf[Short].compareTo(right.asInstanceOf[Short])
      case b:Byte => left.asInstanceOf[Byte].compareTo(right.asInstanceOf[Byte])
      case b:Boolean => left.asInstanceOf[Boolean].compareTo(right.asInstanceOf[Boolean])
      case b:BigDecimal => left.asInstanceOf[BigDecimal].compare(right.asInstanceOf[BigDecimal])
      case b:BigInt => left.asInstanceOf[BigInt].compare(right.asInstanceOf[BigInt])
      case d:java.sql.Date => left.asInstanceOf[java.sql.Date].compareTo(right.asInstanceOf[java.sql.Date])
      case d:java.sql.Timestamp => left.asInstanceOf[java.sql.Timestamp].compareTo(right.asInstanceOf[java.sql.Timestamp])
      case s:String => left.toString.compareTo(right.toString)
      case s:Some[_] => compare(s.get, right)
      case _ => 0
    }
  }
}
