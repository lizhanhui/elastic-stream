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

public class TrimStreamsResponseT {
  private int throttleTimeMs;
  private header.TrimStreamResultT[] trimResponses;
  private header.StatusT status;

  public int getThrottleTimeMs() { return throttleTimeMs; }

  public void setThrottleTimeMs(int throttleTimeMs) { this.throttleTimeMs = throttleTimeMs; }

  public header.TrimStreamResultT[] getTrimResponses() { return trimResponses; }

  public void setTrimResponses(header.TrimStreamResultT[] trimResponses) { this.trimResponses = trimResponses; }

  public header.StatusT getStatus() { return status; }

  public void setStatus(header.StatusT status) { this.status = status; }


  public TrimStreamsResponseT() {
    this.throttleTimeMs = 0;
    this.trimResponses = null;
    this.status = null;
  }
}

