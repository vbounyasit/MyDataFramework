package com.vbounyasit.bigdata.config

import com.vbounyasit.bigdata.OutputTables

/**
  * This table will compute output tables based on
  * @param func1
  * @param func2
  * @tparam T
  * @tparam U
  * @tparam V
  */
case class OutputTablesGenerator[T, U, V](func1: Option[T => OutputTables], func2: Option[(U, V) => OutputTables]){
  def applyFunction1[Y](param: Y): OutputTables = func1.map(_(param.asInstanceOf[T])).get
  def applyFunction2[Y, Z](param1: Y, param2: Z): OutputTables = func2.map(_(param1.asInstanceOf[U], param2.asInstanceOf[V])).get
}

object OutputTablesGenerator {

  type ResultTables = OutputTablesGenerator[_, _, _]

  def apply[T](func1: T => OutputTables): OutputTablesGenerator[T, _, _] = OutputTablesGenerator(Some(func1), None)

  def apply[U, V](func2: (U, V) => OutputTables): OutputTablesGenerator[_, U, V] = OutputTablesGenerator(None, Some(func2))

}
