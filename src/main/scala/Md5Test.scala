import util.MD5Util

import scala.collection.mutable.ArrayBuffer

object Md5Test {

  def main(args: Array[String]): Unit = {
    println(additiveHash("cccc!!aa", 10))
    println(additiveHash("AABB332", 10))
    println(additiveHash("AABB332", 10))
  }

  def additiveHash(key: String, prime: Int): Int = {
    var hash = 0
    var i = 0
    hash = key.length
    i = 0
    while ( {
      i < key.length
    }) {
      hash += key.charAt(i)
      i += 1
    }
    hash % prime
  }
}