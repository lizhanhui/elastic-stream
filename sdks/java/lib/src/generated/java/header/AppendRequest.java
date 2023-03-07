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
public final class AppendRequest extends Table {
  public static void ValidateVersion() { Constants.FLATBUFFERS_23_1_21(); }
  public static AppendRequest getRootAsAppendRequest(ByteBuffer _bb) { return getRootAsAppendRequest(_bb, new AppendRequest()); }
  public static AppendRequest getRootAsAppendRequest(ByteBuffer _bb, AppendRequest obj) { _bb.order(ByteOrder.LITTLE_ENDIAN); return (obj.__assign(_bb.getInt(_bb.position()) + _bb.position(), _bb)); }
  public void __init(int _i, ByteBuffer _bb) { __reset(_i, _bb); }
  public AppendRequest __assign(int _i, ByteBuffer _bb) { __init(_i, _bb); return this; }

  public int timeoutMs() { int o = __offset(4); return o != 0 ? bb.getInt(o + bb_pos) : 0; }
  public header.AppendInfo appendRequests(int j) { return appendRequests(new header.AppendInfo(), j); }
  public header.AppendInfo appendRequests(header.AppendInfo obj, int j) { int o = __offset(6); return o != 0 ? obj.__assign(__indirect(__vector(o) + j * 4), bb) : null; }
  public int appendRequestsLength() { int o = __offset(6); return o != 0 ? __vector_len(o) : 0; }
  public header.AppendInfo.Vector appendRequestsVector() { return appendRequestsVector(new header.AppendInfo.Vector()); }
  public header.AppendInfo.Vector appendRequestsVector(header.AppendInfo.Vector obj) { int o = __offset(6); return o != 0 ? obj.__assign(__vector(o), 4, bb) : null; }

  public static int createAppendRequest(FlatBufferBuilder builder,
      int timeoutMs,
      int appendRequestsOffset) {
    builder.startTable(2);
    AppendRequest.addAppendRequests(builder, appendRequestsOffset);
    AppendRequest.addTimeoutMs(builder, timeoutMs);
    return AppendRequest.endAppendRequest(builder);
  }

  public static void startAppendRequest(FlatBufferBuilder builder) { builder.startTable(2); }
  public static void addTimeoutMs(FlatBufferBuilder builder, int timeoutMs) { builder.addInt(0, timeoutMs, 0); }
  public static void addAppendRequests(FlatBufferBuilder builder, int appendRequestsOffset) { builder.addOffset(1, appendRequestsOffset, 0); }
  public static int createAppendRequestsVector(FlatBufferBuilder builder, int[] data) { builder.startVector(4, data.length, 4); for (int i = data.length - 1; i >= 0; i--) builder.addOffset(data[i]); return builder.endVector(); }
  public static void startAppendRequestsVector(FlatBufferBuilder builder, int numElems) { builder.startVector(4, numElems, 4); }
  public static int endAppendRequest(FlatBufferBuilder builder) {
    int o = builder.endTable();
    return o;
  }

  public static final class Vector extends BaseVector {
    public Vector __assign(int _vector, int _element_size, ByteBuffer _bb) { __reset(_vector, _element_size, _bb); return this; }

    public AppendRequest get(int j) { return get(new AppendRequest(), j); }
    public AppendRequest get(AppendRequest obj, int j) {  return obj.__assign(__indirect(__element(j), bb), bb); }
  }
}

