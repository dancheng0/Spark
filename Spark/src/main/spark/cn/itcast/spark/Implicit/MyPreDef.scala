package cn.itcast.spark.Implicit

/**
  * Created by ZX on 2016/4/14.
  */
object MyPreDef {

  implicit def girlToOrdered(girl: Girl) = new Ordered[Girl]{
    override def compare(that: Girl): Int = {
      if(girl.faceValue == that.faceValue) {
        girl.size - that.size
      } else {
        girl.faceValue - that.faceValue
      }
    }
  }

  implicit object girlOrdering extends Ordering[Girl] {
    override def compare(x: Girl, y: Girl): Int = {
      if(x.faceValue == y.faceValue) {
        x.size - y.size
      } else {
        x.faceValue - y.faceValue
      }
    }
  }

}
