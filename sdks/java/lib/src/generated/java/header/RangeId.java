// automatically generated by the FlatBuffers compiler, do not modify

package header;

import com.google.flatbuffers.BaseVector;
import com.google.flatbuffers.BooleanVector;
import com.google.flatbuffers.ByteVector;
import com.google.flatbuffers.Constants;
import com.google.flatbuffers.DoubleVector;
import com.google.flatbuffers.FlatBufferBuilder;
import com.google.flatbuffers.FloatVector;
import com.google.flatbuffers.IntVector;
import com.google.flatbuffers.LongVector;
import com.google.flatbuffers.ShortVector;
import com.google.flatbuffers.StringVector;
import com.google.flatbuffers.Struct;
import com.google.flatbuffers.Table;
import com.google.flatbuffers.UnionVector;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

@SuppressWarnings("unused")
public final class RangeId extends Table {
  public static void ValidateVersion() { Constants.FLATBUFFERS_23_1_21(); }
  public static RangeId getRootAsRangeId(ByteBuffer _bb) { return getRootAsRangeId(_bb, new RangeId()); }
  public static RangeId getRootAsRangeId(ByteBuffer _bb, RangeId obj) { _bb.order(ByteOrder.LITTLE_ENDIAN); return (obj.__assign(_bb.getInt(_bb.position()) + _bb.position(), _bb)); }
  public void __init(int _i, ByteBuffer _bb) { __reset(_i, _bb); }
  public RangeId __assign(int _i, ByteBuffer _bb) { __init(_i, _bb); return this; }

  public long streamId() { int o = __offset(4); return o != 0 ? bb.getLong(o + bb_pos) : 0L; }
  public int rangeIndex() { int o = __offset(6); return o != 0 ? bb.getInt(o + bb_pos) : 0; }

  public static int createRangeId(FlatBufferBuilder builder,
      long streamId,
      int rangeIndex) {
    builder.startTable(2);
    RangeId.addStreamId(builder, streamId);
    RangeId.addRangeIndex(builder, rangeIndex);
    return RangeId.endRangeId(builder);
  }

  public static void startRangeId(FlatBufferBuilder builder) { builder.startTable(2); }
  public static void addStreamId(FlatBufferBuilder builder, long streamId) { builder.addLong(0, streamId, 0L); }
  public static void addRangeIndex(FlatBufferBuilder builder, int rangeIndex) { builder.addInt(1, rangeIndex, 0); }
  public static int endRangeId(FlatBufferBuilder builder) {
    int o = builder.endTable();
    return o;
  }

  public static final class Vector extends BaseVector {
    public Vector __assign(int _vector, int _element_size, ByteBuffer _bb) { __reset(_vector, _element_size, _bb); return this; }

    public RangeId get(int j) { return get(new RangeId(), j); }
    public RangeId get(RangeId obj, int j) {  return obj.__assign(__indirect(__element(j), bb), bb); }
  }
}

