package shark

import org.scalatest.FreeSpec

class UtilsTest extends FreeSpec {

  "memoryBytesToString should" - {
    "translate Longs to their string representation to " in {
      assert( Utils.memoryBytesToString(1000009L) === "976.6 KB" )
      assert( Utils.memoryBytesToString(2000009L) === "1953.1 KB" )
      assert( Utils.memoryBytesToString(1005009L) === "981.5 KB" )
      assert( Utils.memoryBytesToString(6005009L) === "5.7 MB" )
      assert( Utils.memoryBytesToString(8000009L) === "7.6 MB" )
      assert( Utils.memoryBytesToString(3005009L) === "2.9 MB" )
      assert( Utils.memoryBytesToString(80000009L) === "76.3 MB" )
      assert( Utils.memoryBytesToString(80999000009L) === "75.4 GB" )
      assert( Utils.memoryBytesToString(99980999000009L) === "90.9 TB" )
      assert( Utils.memoryBytesToString(7780999000009L) === "7.1 TB" )
      assert( Utils.memoryBytesToString(56830999000009L) === "51.7 TB" )
      assert( Utils.memoryBytesToString(9956830999000009L) === "9055.7 TB" )
      assert( Utils.memoryBytesToString(9956830999000009L, legacy=false) === "8.8 PB" )
      assert( Utils.memoryBytesToString(1999568309990000092L) === "1818596.8 TB" )
      assert( Utils.memoryBytesToString(1999568309990000092L, legacy=false) === "1776.0 PB" ) // :-)
    }
  }

}
