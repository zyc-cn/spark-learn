package graphtest

import org.apache.commons.codec.digest.DigestUtils

import java.math.BigInteger

object Test {

  def main(args: Array[String]): Unit = {
    println(new BigInteger(DigestUtils.md5Hex("似懂非懂"), 16))
    println(new BigInteger(DigestUtils.md5Hex("似懂非懂"), 16))
  }

}
