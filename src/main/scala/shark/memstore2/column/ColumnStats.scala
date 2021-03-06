/*
 * Copyright (C) 2012 The Regents of The University California.
 * All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package shark.memstore2.column

import java.io.ObjectInput
import java.io.ObjectOutput
import java.io.Externalizable
import java.sql.Timestamp

import org.apache.hadoop.io.Text


/**
 * Column level statistics, including range (min, max).
 */
sealed trait ColumnStats[@specialized(Boolean, Byte, Short, Int, Long, Float, Double) T]
  extends Serializable {

  var _nullCount = 0

  def append(v: T)

  protected def _min: T
  protected def _max: T

  def appendNull() { _nullCount += 1 }

  def nullCount: Int = _nullCount
  def min: T = _min
  def max: T = _max

  override def toString = "[" + min + ", " + max + "]"
}


// For all columns, we track the range (i.e. min and max).
// For int columns, we try to do more since int is more common. Examples include
// ordering of the column and max deltas (max difference between two cells).
object ColumnStats {

  class BooleanColumnStats extends ColumnStats[Boolean] {
    protected var _max = false
    protected var _min = true
    override def append(v: Boolean) {
      if (v) _max = v
      else _min = v
    }
  }

  class ByteColumnStats extends ColumnStats[Byte] {
    protected var _max = Byte.MinValue
    protected var _min = Byte.MaxValue
    override def append(v: Byte) {
      if (v > _max) _max = v
      if (v < _min) _min = v
    }
  }

  class ShortColumnStats extends ColumnStats[Short] {
    protected var _max = Short.MinValue
    protected var _min = Short.MaxValue
    override def append(v: Short) {
      if (v > _max) _max = v
      if (v < _min) _min = v
    }
  }

  object IntColumnStats {
    private val UNINITIALIZED = 0  // Haven't seen the first value yet.
    private val INITIALIAZED = 1   // Seen first, and processing second value.
    private val ASCENDING = 2
    private val DESCENDING = 3
    private val UNORDERED = 4
  }

  class IntColumnStats extends ColumnStats[Int] {
    import IntColumnStats._
    private var _orderedState = UNINITIALIZED

    protected var _max = Int.MinValue
    protected var _min = Int.MaxValue
    private var _lastValue = 0
    private var _maxDelta = 0

    def isAscending = _orderedState != DESCENDING && _orderedState != UNORDERED
    def isDescending = _orderedState != ASCENDING && _orderedState != UNORDERED
    def isOrdered = isAscending || isDescending
    def maxDelta = _maxDelta

    override def append(v: Int) {
      if (v > _max) _max = v
      if (v < _min) _min = v

      if (_orderedState == UNINITIALIZED) {
        // First value.
        _orderedState = INITIALIAZED
        _lastValue = v
      } else if (_orderedState == INITIALIAZED) {
        // Second value.
        _orderedState = if (v >= _lastValue) ASCENDING else DESCENDING
        _maxDelta = math.abs(v - _lastValue)
        _lastValue = v
      } else if (_orderedState == ASCENDING) {
        if (v < _lastValue) _orderedState = UNORDERED
        else {
          if (v - _lastValue > _maxDelta) _maxDelta = v - _lastValue
          _lastValue = v
        }
      } else if (_orderedState == DESCENDING) {
        if (v > _lastValue) _orderedState = UNORDERED
        else {
          if (_lastValue - v > _maxDelta) _maxDelta = _lastValue - v
          _lastValue = v
        }
      }
    }
  }

  class LongColumnStats extends ColumnStats[Long] {
    protected var _max = Long.MinValue
    protected var _min = Long.MaxValue
    override def append(v: Long) {
      if (v > _max) _max = v
      if (v < _min) _min = v
    }
  }

  class FloatColumnStats extends ColumnStats[Float] {
    protected var _max = Float.MinValue
    protected var _min = Float.MaxValue
    override def append(v: Float) {
      if (v > _max) _max = v
      if (v < _min) _min = v
    }
  }

  class DoubleColumnStats extends ColumnStats[Double] {
    protected var _max = Double.MinValue
    protected var _min = Double.MaxValue
    override def append(v: Double) {
      if (v > _max) _max = v
      if (v < _min) _min = v
    }
  }

  class TimestampColumnStats extends ColumnStats[Timestamp] {
    protected var _max = new Timestamp(0)
    protected var _min = new Timestamp(Long.MaxValue)
    override def append(v: Timestamp) {
      if (v.compareTo(_max) > 0) _max = v
      if (v.compareTo(_min) < 0) _min = v
    }
  }

  class StringColumnStats extends ColumnStats[Text] with Externalizable {
    // Note: this is not Java serializable because Text is not Java serializable.
    protected var _max: Text = null
    protected var _min: Text = null

    override def append(v: Text) {
      // Need to make a copy of Text since Text is not immutable and we reuse
      // the same Text object in serializer to mitigate frequent GC.
      if (_max == null || v.compareTo(_max) > 0) _max = new Text(v)
      if (_min == null || v.compareTo(_min) < 0) _min = new Text(v)
    }

    override def readExternal(in: ObjectInput) {
      if (in.readBoolean()) {
        _max = new Text
        _max.readFields(in)
      }
      if (in.readBoolean()) {
        _min = new Text
        _min.readFields(in)
      }
    }

    override def writeExternal(out: ObjectOutput) {
      if (_max == null) {
        out.write(0)
      } else {
        out.write(1)
        _max.write(out)
      }
      if (_min == null) {
        out.write(0)
      } else {
        out.write(1)
        _min.write(out)
      }
    }
  }
}
