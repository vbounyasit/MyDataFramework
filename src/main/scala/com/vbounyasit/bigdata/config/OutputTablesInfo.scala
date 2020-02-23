package com.vbounyasit.bigdata.config

import com.vbounyasit.bigdata.OutputTables

case class OutputTablesInfo[T, U, V](func1: Option[T => OutputTables], func2: Option[(U, V) => OutputTables]){
  def applyFunction1[Y](param: Y): OutputTables = func1.map(_(param.asInstanceOf[T])).get
  def applyFunction2[Y, Z](param1: Y, param2: Z): OutputTables = func2.map(_(param1.asInstanceOf[U], param2.asInstanceOf[V])).get
}

object OutputTablesInfo {

  type ResultTables = OutputTablesInfo[_, _, _]

  def apply[T](func1: T => OutputTables): OutputTablesInfo[T, _, _] = OutputTablesInfo(Some(func1), None)

  def apply[U, V](func2: (U, V) => OutputTables): OutputTablesInfo[_, U, V] = OutputTablesInfo(None, Some(func2))

}
