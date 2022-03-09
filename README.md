Implements:
1. DTC Protocol for accessing Sierra Chart via its DTC API
   Uses google protocol buffers and the beginnings of some support for binary and binary vls.
2. Sierra Chart SCID and DLY file readers (useful if you want CME futures which are not supported by the API)

Note:
Binary and binary VLS implementatons are very incomplete.
The binary implementations are doomed to fail becuase the DTC protocol uses #pragma pack, which is compiler dependendent
and therefore frought with potential incompatibilities.

