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
public final class ListRangesResult extends Table {
  public static void ValidateVersion() { Constants.FLATBUFFERS_23_1_21(); }
  public static ListRangesResult getRootAsListRangesResult(ByteBuffer _bb) { return getRootAsListRangesResult(_bb, new ListRangesResult()); }
  public static ListRangesResult getRootAsListRangesResult(ByteBuffer _bb, ListRangesResult obj) { _bb.order(ByteOrder.LITTLE_ENDIAN); return (obj.__assign(_bb.getInt(_bb.position()) + _bb.position(), _bb)); }
  public void __init(int _i, ByteBuffer _bb) { __reset(_i, _bb); }
  public ListRangesResult __assign(int _i, ByteBuffer _bb) { __init(_i, _bb); return this; }

  public header.RangeCriteria rangeCriteria() { return rangeCriteria(new header.RangeCriteria()); }
  public header.RangeCriteria rangeCriteria(header.RangeCriteria obj) { int o = __offset(4); return o != 0 ? obj.__assign(__indirect(o + bb_pos), bb) : null; }
  public short errorCode() { int o = __offset(6); return o != 0 ? bb.getShort(o + bb_pos) : 0; }
  public String errorMessage() { int o = __offset(8); return o != 0 ? __string(o + bb_pos) : null; }
  public ByteBuffer errorMessageAsByteBuffer() { return __vector_as_bytebuffer(8, 1); }
  public ByteBuffer errorMessageInByteBuffer(ByteBuffer _bb) { return __vector_in_bytebuffer(_bb, 8, 1); }
  public header.Range ranges(int j) { return ranges(new header.Range(), j); }
  public header.Range ranges(header.Range obj, int j) { int o = __offset(10); return o != 0 ? obj.__assign(__indirect(__vector(o) + j * 4), bb) : null; }
  public int rangesLength() { int o = __offset(10); return o != 0 ? __vector_len(o) : 0; }
  public header.Range.Vector rangesVector() { return rangesVector(new header.Range.Vector()); }
  public header.Range.Vector rangesVector(header.Range.Vector obj) { int o = __offset(10); return o != 0 ? obj.__assign(__vector(o), 4, bb) : null; }

  public static int createListRangesResult(FlatBufferBuilder builder,
      int rangeCriteriaOffset,
      short errorCode,
      int errorMessageOffset,
      int rangesOffset) {
    builder.startTable(4);
    ListRangesResult.addRanges(builder, rangesOffset);
    ListRangesResult.addErrorMessage(builder, errorMessageOffset);
    ListRangesResult.addRangeCriteria(builder, rangeCriteriaOffset);
    ListRangesResult.addErrorCode(builder, errorCode);
    return ListRangesResult.endListRangesResult(builder);
  }

  public static void startListRangesResult(FlatBufferBuilder builder) { builder.startTable(4); }
  public static void addRangeCriteria(FlatBufferBuilder builder, int rangeCriteriaOffset) { builder.addOffset(0, rangeCriteriaOffset, 0); }
  public static void addErrorCode(FlatBufferBuilder builder, short errorCode) { builder.addShort(1, errorCode, 0); }
  public static void addErrorMessage(FlatBufferBuilder builder, int errorMessageOffset) { builder.addOffset(2, errorMessageOffset, 0); }
  public static void addRanges(FlatBufferBuilder builder, int rangesOffset) { builder.addOffset(3, rangesOffset, 0); }
  public static int createRangesVector(FlatBufferBuilder builder, int[] data) { builder.startVector(4, data.length, 4); for (int i = data.length - 1; i >= 0; i--) builder.addOffset(data[i]); return builder.endVector(); }
  public static void startRangesVector(FlatBufferBuilder builder, int numElems) { builder.startVector(4, numElems, 4); }
  public static int endListRangesResult(FlatBufferBuilder builder) {
    int o = builder.endTable();
    return o;
  }

  public static final class Vector extends BaseVector {
    public Vector __assign(int _vector, int _element_size, ByteBuffer _bb) { __reset(_vector, _element_size, _bb); return this; }

    public ListRangesResult get(int j) { return get(new ListRangesResult(), j); }
    public ListRangesResult get(ListRangesResult obj, int j) {  return obj.__assign(__indirect(__element(j), bb), bb); }
  }
  public ListRangesResultT unpack() {
    ListRangesResultT _o = new ListRangesResultT();
    unpackTo(_o);
    return _o;
  }
  public void unpackTo(ListRangesResultT _o) {
    if (rangeCriteria() != null) _o.setRangeCriteria(rangeCriteria().unpack());
    else _o.setRangeCriteria(null);
    short _oErrorCode = errorCode();
    _o.setErrorCode(_oErrorCode);
    String _oErrorMessage = errorMessage();
    _o.setErrorMessage(_oErrorMessage);
    header.RangeT[] _oRanges = new header.RangeT[rangesLength()];
    for (int _j = 0; _j < rangesLength(); ++_j) {_oRanges[_j] = (ranges(_j) != null ? ranges(_j).unpack() : null);}
    _o.setRanges(_oRanges);
  }
  public static int pack(FlatBufferBuilder builder, ListRangesResultT _o) {
    if (_o == null) return 0;
    int _rangeCriteria = _o.getRangeCriteria() == null ? 0 : header.RangeCriteria.pack(builder, _o.getRangeCriteria());
    int _errorMessage = _o.getErrorMessage() == null ? 0 : builder.createString(_o.getErrorMessage());
    int _ranges = 0;
    if (_o.getRanges() != null) {
      int[] __ranges = new int[_o.getRanges().length];
      int _j = 0;
      for (header.RangeT _e : _o.getRanges()) { __ranges[_j] = header.Range.pack(builder, _e); _j++;}
      _ranges = createRangesVector(builder, __ranges);
    }
    return createListRangesResult(
      builder,
      _rangeCriteria,
      _o.getErrorCode(),
      _errorMessage,
      _ranges);
  }
}
