"""AudioSocket protocol frame parsing (Asterisk res_audiosocket)."""
import asyncio
import struct

TYPE_TERMINATE = 0x00
TYPE_UUID = 0x01
TYPE_AUDIO = 0x10
TYPE_ERROR = 0xFF


async def read_frame(reader: asyncio.StreamReader):
    """Read one AudioSocket frame: 1 byte type + 2 byte big-endian length + payload."""
    header = await reader.readexactly(3)
    kind = header[0]
    length = struct.unpack(">H", header[1:3])[0]
    payload = await reader.readexactly(length) if length else b""
    return kind, payload
