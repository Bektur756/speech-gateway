"""ARI controller — forks client and operator audio separately into the
Speech Gateway for calls arriving via freeswitch-trunk to an operator
extension (101 or 102).

Mechanism: the normal FreePBX dialplan handles the call untouched (Answer,
Dial, ring, bridge — nothing here interferes with that; client and
operator hear each other exactly as before). This service just watches
Asterisk's event stream via ARI (subscribeAll=true, so it sees events for
channels that never entered any Stasis app) for the moment a
freeswitch-trunk channel and an operator channel land in the same bridge.
At that point, for each of the two channels:

  1. Snoop it (ARI /channels/{id}/snoop) — spy=in is meant to capture the
     audio coming FROM that channel (i.e. that party's own voice, not
     what they hear). This has NOT been verified against a real call yet
     — if a real test shows client/operator transcripts are swapped or
     each side is hearing the other party's voice, flip SPY_DIRECTION to
     "out". The snoop channel enters this app's Stasis application.
  2. Once the snoop channel is confirmed in Stasis, originate an
     AudioSocket channel (chan_audiosocket) pointed at the gateway's
     client/operator port.
  3. Bridge the snoop channel and the AudioSocket channel together in a
     new 2-party mixing bridge. Asterisk's own bridging engine streams the
     snooped audio into AudioSocket from there — audio never passes
     through this process, only control-plane REST/WS calls do.

Before originating each leg's AudioSocket channel, its call_id is also
POSTed to the gateway's /conversations/{bridge_id}/legs endpoint (via
nginx — the gateway's own port isn't reachable from this box otherwise),
so the gateway can merge both legs' "final" transcripts into one
chronologically-ordered data/transcripts/conversations/{bridge_id}.jsonl
alongside each leg's own per-call file.

Language is hardcoded to ky for now (see gateway ports 9100/9101 — add ru
variants the same way if that becomes needed).

Run directly on the Asterisk box — ARI's HTTP server is bound to
127.0.0.1 only, not part of the gateway's docker-compose stack.
"""
import asyncio
import json
import logging
import os
import uuid

import aiohttp

log = logging.getLogger("ari-controller")
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")

ARI_BASE = os.environ.get("ARI_BASE", "http://127.0.0.1:8088/ari")
ARI_USER = os.environ["ARI_USER"]
ARI_PASS = os.environ["ARI_PASS"]
STASIS_APP = "speech-gateway-dual"

GATEWAY_HOST = os.environ.get("GATEWAY_HOST", "10.0.12.5")
CLIENT_PORT = int(os.environ.get("GATEWAY_CLIENT_PORT", 9100))
OPERATOR_PORT = int(os.environ.get("GATEWAY_OPERATOR_PORT", 9101))
PORTS = {"client": CLIENT_PORT, "operator": OPERATOR_PORT}
# HTTP API (via nginx — the gateway's own port 8000 isn't published to the
# host), used only to link both legs of a call into one merged transcript.
GATEWAY_HTTP_URL = os.environ.get("GATEWAY_HTTP_URL", f"http://{GATEWAY_HOST}")

TRUNK_PREFIX = "PJSIP/freeswitch-trunk-"
# Comma-separated in the env (e.g. "101,102") so new operator extensions can
# be added without a code change.
OPERATOR_EXTENSIONS = os.environ.get("OPERATOR_EXTENSIONS", "101,102").split(",")
OPERATOR_PREFIXES = tuple(f"PJSIP/{ext}-" for ext in OPERATOR_EXTENSIONS)

# See module docstring point 1 — unverified against a real call yet.
SPY_DIRECTION = os.environ.get("SPY_DIRECTION", "in")


def _role_for_channel_name(name: str) -> str | None:
    if name.startswith(TRUNK_PREFIX):
        return "client"
    if name.startswith(OPERATOR_PREFIXES):
        return "operator"
    return None


class Controller:
    def __init__(self, session: aiohttp.ClientSession):
        self.session = session
        self.auth = aiohttp.BasicAuth(ARI_USER, ARI_PASS)
        # bridge_id -> {"legs": {chan_id: role}, "started": bool}
        self.conversations: dict[str, dict] = {}
        # snoop channel id AND audiosocket channel id both point at the same
        # leg dict, so either one ending lets us find + tear down the other.
        # leg dict: {"bridge_id", "role", "snoop_id", "audiosocket_id",
        #            "leg_bridge_id"}
        self.leg_by_channel: dict[str, dict] = {}

    async def _rest(self, method: str, path: str, quiet: bool = False, **kwargs) -> dict:
        url = f"{ARI_BASE}{path}"
        async with self.session.request(method, url, auth=self.auth, **kwargs) as resp:
            text = await resp.text()
            if resp.status >= 300:
                if not quiet:
                    log.error("ARI %s %s -> %s: %s", method, path, resp.status, text)
                resp.raise_for_status()
            return json.loads(text) if text else {}

    async def handle_channel_entered_bridge(self, event: dict):
        bridge = event["bridge"]
        bridge_id = bridge["id"]
        channel_ids = bridge.get("channels", [])
        if len(channel_ids) < 2:
            return

        conv = self.conversations.setdefault(bridge_id, {"legs": {}, "started": False})
        if conv["started"]:
            return

        entered = event.get("channel")
        if entered:
            role = _role_for_channel_name(entered["name"])
            if role:
                conv["legs"][entered["id"]] = role

        for chan_id in channel_ids:
            if chan_id in conv["legs"]:
                continue
            try:
                chan = await self._rest("GET", f"/channels/{chan_id}")
            except Exception:
                continue
            role = _role_for_channel_name(chan.get("name", ""))
            if role:
                conv["legs"][chan_id] = role

        have_roles = set(conv["legs"].values())
        if {"client", "operator"} <= have_roles:
            conv["started"] = True
            log.info("[%s] client+operator bridged — starting dual capture", bridge_id)
            for chan_id, role in list(conv["legs"].items()):
                asyncio.create_task(self.start_leg_capture(bridge_id, chan_id, role))

    async def start_leg_capture(self, bridge_id: str, target_channel_id: str, role: str):
        snoop_id = f"snoop-{uuid.uuid4()}"
        try:
            snoop = await self._rest(
                "POST", f"/channels/{target_channel_id}/snoop",
                params={
                    "spy": SPY_DIRECTION,
                    "whisper": "none",
                    "app": STASIS_APP,
                    "appArgs": f"snoop,{role},{bridge_id}",
                    "snoopId": snoop_id,
                },
            )
            leg = {"bridge_id": bridge_id, "role": role, "snoop_id": snoop["id"],
                   "audiosocket_id": None, "leg_bridge_id": None}
            self.leg_by_channel[snoop["id"]] = leg
            log.info("[%s] %s: snoop channel %s created (spy=%s)",
                      bridge_id, role, snoop.get("id"), SPY_DIRECTION)
        except Exception:
            log.exception("[%s] %s: failed to create snoop channel", bridge_id, role)

    async def handle_stasis_start(self, event: dict):
        channel = event["channel"]
        args = event.get("args", [])
        if not args:
            return
        kind = args[0]

        if kind == "snoop" and len(args) >= 3:
            _, role, bridge_id = args[0], args[1], args[2]
            await self.spawn_audiosocket_leg(bridge_id, role, channel["id"])

        elif kind == "audiosocket" and len(args) >= 4:
            _, role, bridge_id, snoop_channel_id = args[0], args[1], args[2], args[3]
            await self.bridge_snoop_and_audiosocket(bridge_id, role, snoop_channel_id, channel["id"])

    async def spawn_audiosocket_leg(self, bridge_id: str, role: str, snoop_channel_id: str):
        call_uuid = str(uuid.uuid4())
        port = PORTS[role]
        # chan_audiosocket's outbound dial format is the REVERSE of the
        # dialplan application's (uuid,host:port) — verified against
        # Asterisk source (audiosocket_request in chan_audiosocket.c) and
        # confirmed live: AST_NONSTANDARD_APP_ARGS splits on '/' as
        # destination(host:port)/idStr(uuid)/options.
        endpoint = f"AudioSocket/{GATEWAY_HOST}:{port}/{call_uuid}"
        # The gateway derives its call_id from the raw UUID bytes' .hex()
        # (32 lowercase hex chars, no dashes) — must match exactly here so
        # the link registers against the call_id the gateway will actually
        # use once this channel starts sending audio. Link BEFORE
        # originating so there's no race with the first transcript event.
        gateway_call_id = call_uuid.replace("-", "")
        try:
            async with self.session.post(
                f"{GATEWAY_HTTP_URL}/conversations/{bridge_id}/legs",
                json={"role": role, "call_id": gateway_call_id},
            ):
                pass
        except Exception:
            log.exception("[%s] %s: failed to link conversation leg (merged transcript won't include it)",
                          bridge_id, role)
        try:
            chan = await self._rest(
                "POST", "/channels",
                params={
                    "endpoint": endpoint,
                    "app": STASIS_APP,
                    "appArgs": f"audiosocket,{role},{bridge_id},{snoop_channel_id}",
                },
            )
            leg = self.leg_by_channel.get(snoop_channel_id)
            if leg:
                leg["audiosocket_id"] = chan["id"]
                self.leg_by_channel[chan["id"]] = leg
            log.info("[%s] %s: audiosocket channel %s originated -> %s",
                      bridge_id, role, chan.get("id"), endpoint)
        except Exception:
            log.exception("[%s] %s: failed to originate AudioSocket channel", bridge_id, role)

    async def bridge_snoop_and_audiosocket(self, bridge_id: str, role: str,
                                            snoop_id: str, audiosocket_id: str):
        try:
            leg_bridge = await self._rest("POST", "/bridges", params={"type": "mixing"})
            leg_bridge_id = leg_bridge["id"]
            await self._rest(
                "POST", f"/bridges/{leg_bridge_id}/addChannel",
                params={"channel": f"{snoop_id},{audiosocket_id}"},
            )
            leg = self.leg_by_channel.get(snoop_id)
            if leg:
                leg["leg_bridge_id"] = leg_bridge_id
            log.info("[%s] %s: leg bridge %s connects snoop=%s <-> audiosocket=%s",
                      bridge_id, role, leg_bridge_id, snoop_id, audiosocket_id)
        except Exception:
            log.exception("[%s] %s: failed to bridge snoop+audiosocket", bridge_id, role)

    async def teardown_leg(self, ended_channel_id: str):
        leg = self.leg_by_channel.pop(ended_channel_id, None)
        if leg is None:
            return
        other_ids = [leg.get("snoop_id"), leg.get("audiosocket_id")]
        other_ids = [cid for cid in other_ids if cid and cid != ended_channel_id]
        log.info("[%s] %s: leg ended, tearing down remaining channels %s",
                  leg["bridge_id"], leg["role"], other_ids)
        for cid in other_ids:
            self.leg_by_channel.pop(cid, None)
            try:
                await self._rest("DELETE", f"/channels/{cid}")
            except Exception:
                pass  # likely already gone
        if leg["leg_bridge_id"]:
            try:
                await self._rest("DELETE", f"/bridges/{leg['leg_bridge_id']}")
            except Exception:
                pass  # bridge auto-destroys once its channels are gone anyway

        conv = self.conversations.get(leg["bridge_id"])
        if conv is not None:
            conv.setdefault("legs_torn_down", set()).add(leg["role"])
            if {"client", "operator"} <= conv["legs_torn_down"]:
                self.conversations.pop(leg["bridge_id"], None)

    async def handle_channel_left_bridge(self, event: dict):
        # Confirmed live (2026-08-28, conversation bea70bc1): a leg's snoop
        # channel can leave its 2-party capture bridge mid-call — cause
        # still unconfirmed on the Asterisk side — without any
        # StasisEnd/ChannelDestroyed following. Before this handler existed
        # that silently killed the leg's audio for the rest of the call:
        # both channels stayed alive (so teardown_leg, which only fires on
        # ChannelDestroyed, never ran) but no audio flowed since they were
        # no longer bridged together. Re-adding the channel recovers it
        # without needing to know why it happened.
        #
        # This event also fires as a completely normal part of a channel's
        # own hangup sequence (confirmed live 2026-09-01/02: ChannelLeftBridge
        # immediately followed by StasisEnd/ChannelDestroyed for the same
        # channel, right as the underlying call ends) — in that case the
        # re-add below races the channel's teardown and gets a 400 "Channel
        # not found", which is expected and not worth alarming on.
        channel = event.get("channel")
        bridge = event.get("bridge")
        if not channel or not bridge:
            return
        leg = self.leg_by_channel.get(channel["id"])
        if not leg or leg.get("leg_bridge_id") != bridge["id"]:
            return
        log.info("[%s] %s: channel %s left leg bridge %s — attempting recovery",
                  leg["bridge_id"], leg["role"], channel["id"], bridge["id"])
        try:
            await self._rest(
                "POST", f"/bridges/{bridge['id']}/addChannel",
                params={"channel": channel["id"]}, quiet=True,
            )
            log.info("[%s] %s: recovered leg bridge", leg["bridge_id"], leg["role"])
        except aiohttp.ClientResponseError as e:
            if e.status == 400:
                log.info("[%s] %s: channel already gone (likely normal call end), no recovery needed",
                          leg["bridge_id"], leg["role"])
            else:
                log.exception("[%s] %s: failed to recover leg bridge after channel left",
                              leg["bridge_id"], leg["role"])
        except Exception:
            log.exception("[%s] %s: failed to recover leg bridge after channel left",
                          leg["bridge_id"], leg["role"])

    async def handle_event(self, event: dict):
        etype = event.get("type")
        if etype == "ChannelEnteredBridge":
            await self.handle_channel_entered_bridge(event)
        elif etype == "ChannelLeftBridge":
            await self.handle_channel_left_bridge(event)
        elif etype == "StasisStart":
            await self.handle_stasis_start(event)
        elif etype == "StasisEnd":
            log.info("StasisEnd: %s", event["channel"]["name"])
        elif etype == "ChannelDestroyed":
            log.info("ChannelDestroyed: %s", event["channel"]["name"])
            await self.teardown_leg(event["channel"]["id"])


async def main():
    async with aiohttp.ClientSession() as session:
        controller = Controller(session)
        ws_scheme = "wss" if ARI_BASE.startswith("https://") else "ws"
        ws_host = ARI_BASE.split("://", 1)[1]
        ws_url = f"{ws_scheme}://{ws_host}/events?app={STASIS_APP}&subscribeAll=true"

        while True:
            try:
                log.info("connecting to ARI websocket...")
                async with session.ws_connect(ws_url, auth=controller.auth) as ws:
                    log.info("connected, app=%s", STASIS_APP)
                    async for msg in ws:
                        if msg.type == aiohttp.WSMsgType.TEXT:
                            event = json.loads(msg.data)
                            try:
                                await controller.handle_event(event)
                            except Exception:
                                log.exception("error handling event: %s", event.get("type"))
                        elif msg.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR):
                            break
            except Exception:
                log.exception("websocket connection failed, retrying in 5s")
            await asyncio.sleep(5)


if __name__ == "__main__":
    asyncio.run(main())
