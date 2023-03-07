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
public final class CreateStreamsResponse extends Table {
  public static void ValidateVersion() { Constants.FLATBUFFERS_23_1_21(); }
  public static CreateStreamsResponse getRootAsCreateStreamsResponse(ByteBuffer _bb) { return getRootAsCreateStreamsResponse(_bb, new CreateStreamsResponse()); }
  public static CreateStreamsResponse getRootAsCreateStreamsResponse(ByteBuffer _bb, CreateStreamsResponse obj) { _bb.order(ByteOrder.LITTLE_ENDIAN); return (obj.__assign(_bb.getInt(_bb.position()) + _bb.position(), _bb)); }
  public void __init(int _i, ByteBuffer _bb) { __reset(_i, _bb); }
  public CreateStreamsResponse __assign(int _i, ByteBuffer _bb) { __init(_i, _bb); return this; }

  public int throttleTimeMs() { int o = __offset(4); return o != 0 ? bb.getInt(o + bb_pos) : 0; }
  public header.CreateStreamResult createResponses(int j) { return createResponses(new header.CreateStreamResult(), j); }
  public header.CreateStreamResult createResponses(header.CreateStreamResult obj, int j) { int o = __offset(6); return o != 0 ? obj.__assign(__indirect(__vector(o) + j * 4), bb) : null; }
  public int createResponsesLength() { int o = __offset(6); return o != 0 ? __vector_len(o) : 0; }
  public header.CreateStreamResult.Vector createResponsesVector() { return createResponsesVector(new header.CreateStreamResult.Vector()); }
  public header.CreateStreamResult.Vector createResponsesVector(header.CreateStreamResult.Vector obj) { int o = __offset(6); return o != 0 ? obj.__assign(__vector(o), 4, bb) : null; }
  public short errorCode() { int o = __offset(8); return o != 0 ? bb.getShort(o + bb_pos) : 0; }
  public String errorMessage() { int o = __offset(10); return o != 0 ? __string(o + bb_pos) : null; }
  public ByteBuffer errorMessageAsByteBuffer() { return __vector_as_bytebuffer(10, 1); }
  public ByteBuffer errorMessageInByteBuffer(ByteBuffer _bb) { return __vector_in_bytebuffer(_bb, 10, 1); }

  public static int createCreateStreamsResponse(FlatBufferBuilder builder,
      int throttleTimeMs,
      int createResponsesOffset,
      short errorCode,
      int errorMessageOffset) {
    builder.startTable(4);
    CreateStreamsResponse.addErrorMessage(builder, errorMessageOffset);
    CreateStreamsResponse.addCreateResponses(builder, createResponsesOffset);
    CreateStreamsResponse.addThrottleTimeMs(builder, throttleTimeMs);
    CreateStreamsResponse.addErrorCode(builder, errorCode);
    return CreateStreamsResponse.endCreateStreamsResponse(builder);
  }

  public static void startCreateStreamsResponse(FlatBufferBuilder builder) { builder.startTable(4); }
  public static void addThrottleTimeMs(FlatBufferBuilder builder, int throttleTimeMs) { builder.addInt(0, throttleTimeMs, 0); }
  public static void addCreateResponses(FlatBufferBuilder builder, int createResponsesOffset) { builder.addOffset(1, createResponsesOffset, 0); }
  public static int createCreateResponsesVector(FlatBufferBuilder builder, int[] data) { builder.startVector(4, data.length, 4); for (int i = data.length - 1; i >= 0; i--) builder.addOffset(data[i]); return builder.endVector(); }
  public static void startCreateResponsesVector(FlatBufferBuilder builder, int numElems) { builder.startVector(4, numElems, 4); }
  public static void addErrorCode(FlatBufferBuilder builder, short errorCode) { builder.addShort(2, errorCode, 0); }
  public static void addErrorMessage(FlatBufferBuilder builder, int errorMessageOffset) { builder.addOffset(3, errorMessageOffset, 0); }
  public static int endCreateStreamsResponse(FlatBufferBuilder builder) {
    int o = builder.endTable();
    return o;
  }

  public static final class Vector extends BaseVector {
    public Vector __assign(int _vector, int _element_size, ByteBuffer _bb) { __reset(_vector, _element_size, _bb); return this; }

    public CreateStreamsResponse get(int j) { return get(new CreateStreamsResponse(), j); }
    public CreateStreamsResponse get(CreateStreamsResponse obj, int j) {  return obj.__assign(__indirect(__element(j), bb), bb); }
  }
}

