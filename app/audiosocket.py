"""AudioSocket protocol frame parsing (Asterisk res_audiosocket / chan_audiosocket).

Audio frame types 0x10-0x18 each carry a different sample rate, extended in
Asterisk to cover every slin format (res_audiosocket PR #1492) — a
dialplan-driven connection (res_audiosocket) always uses 0x10/8kHz, but a
channel-driver-originated one (chan_audiosocket, used for ARI-based dual-leg
capture) sends whatever rate the bridge actually negotiated — confirmed
empirically as 0x18/192kHz for a Snoop-sourced leg. AUDIO_SAMPLE_RATES maps
every valid audio type byte to its sample rate so callers can resample
correctly regardless of which one shows up.
"""
import asyncio
import struct

TYPE_TERMINATE = 0x00
TYPE_UUID = 0x01
TYPE_AUDIO = 0x10  # kept for compatibility — the classic 8kHz-only type byte
TYPE_ERROR = 0xFF

AUDIO_SAMPLE_RATES = {
    0x10: 8000,
    0x11: 12000,
    0x12: 16000,
    0x13: 24000,
    0x14: 32000,
    0x15: 44100,
    0x16: 48000,
    0x17: 96000,
    0x18: 192000,
}


async def read_frame(reader: asyncio.StreamReader):
    """Read one AudioSocket frame: 1 byte type + 2 byte big-endian length + payload."""
    header = await reader.readexactly(3)
    kind = header[0]
    length = struct.unpack(">H", header[1:3])[0]
    payload = await reader.readexactly(length) if length else b""
    return kind, payload
