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

public class FetchResultT {
  private long streamId;
  private int requestIndex;
  private short errorCode;
  private String errorMessage;
  private int batchLength;

  public long getStreamId() { return streamId; }

  public void setStreamId(long streamId) { this.streamId = streamId; }

  public int getRequestIndex() { return requestIndex; }

  public void setRequestIndex(int requestIndex) { this.requestIndex = requestIndex; }

  public short getErrorCode() { return errorCode; }

  public void setErrorCode(short errorCode) { this.errorCode = errorCode; }

  public String getErrorMessage() { return errorMessage; }

  public void setErrorMessage(String errorMessage) { this.errorMessage = errorMessage; }

  public int getBatchLength() { return batchLength; }

  public void setBatchLength(int batchLength) { this.batchLength = batchLength; }


  public FetchResultT() {
    this.streamId = 0L;
    this.requestIndex = 0;
    this.errorCode = 0;
    this.errorMessage = null;
    this.batchLength = 0;
  }
}
