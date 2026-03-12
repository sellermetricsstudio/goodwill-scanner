import asyncio
import sqlite3
import yaml
import discord
import re
import os

DB_PATH = "data/scanner.sqlite3"

# ---- Max Bid defaults (tweak whenever you want) ----
EBAY_FEE_RATE = 0.14   # 14% placeholder
TARGET_PROFIT = 20.0   # desired profit
BUFFER = 5.0           # cushion for unknowns
SUPPLIES = 1.0         # mailers/tape/etc

PRICE_EMOJI_DEFAULT = "💰"

# One active pricing prompt per user (keeps it simple/clear)
PENDING_PRICE: dict[int, dict] = {}


def load_cfg(path: str = "config.yaml") -> dict:
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def db_connect() -> sqlite3.Connection:
    return sqlite3.connect(DB_PATH)


def ensure_user_actions_table(conn: sqlite3.Connection):
    conn.execute("""
        CREATE TABLE IF NOT EXISTS user_actions (
            listing_id TEXT PRIMARY KEY,
            action TEXT NOT NULL,
            ts DATETIME DEFAULT CURRENT_TIMESTAMP
        )
    """)
    conn.commit()


def set_user_action(conn: sqlite3.Connection, listing_id: str, action: str):
    conn.execute(
        "INSERT OR REPLACE INTO user_actions (listing_id, action, ts) VALUES (?, ?, CURRENT_TIMESTAMP)",
        (str(listing_id), action),
    )
    conn.commit()


def ensure_price_notes_table(conn: sqlite3.Connection):
    conn.execute("""
        CREATE TABLE IF NOT EXISTS price_notes (
            listing_id TEXT PRIMARY KEY,
            expected_sell REAL NOT NULL,
            sgw_ship REAL NOT NULL,
            max_bid REAL NOT NULL,
            ts DATETIME DEFAULT CURRENT_TIMESTAMP
        )
    """)
    conn.commit()


def set_price_note(conn: sqlite3.Connection, listing_id: str, expected_sell: float, sgw_ship: float, max_bid: float):
    conn.execute(
        "INSERT OR REPLACE INTO price_notes (listing_id, expected_sell, sgw_ship, max_bid, ts) VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP)",
        (str(listing_id), float(expected_sell), float(sgw_ship), float(max_bid)),
    )
    conn.commit()


LISTING_ID_RE = re.compile(r"listing_id\s*=\s*(\d+)", re.IGNORECASE)


def extract_listing_id(msg: discord.Message) -> str | None:
    # 1) Plain content (fallback)
    try:
        txt = msg.content or ""
        m = LISTING_ID_RE.search(txt)
        if m:
            return m.group(1)
    except Exception:
        pass

    # 2) Embed description + footer
    try:
        for emb in msg.embeds or []:
            desc = emb.description or ""
            m = LISTING_ID_RE.search(desc)
            if m:
                return m.group(1)

            footer = emb.footer
            footer_text = footer.text if footer else ""
            m = LISTING_ID_RE.search(footer_text or "")
            if m:
                return m.group(1)
    except Exception:
        pass

    return None


MONEY_RE = re.compile(r"(-?\d+(?:\.\d+)?)")


def parse_two_money_values(text: str) -> tuple[float | None, float | None]:
    """
    Accepts: "75, 12.34" or "75 12.34"
    Returns: (sell, sgw_ship)
    """
    if not text:
        return (None, None)
    cleaned = text.replace("$", "").replace(",", " ")
    nums = MONEY_RE.findall(cleaned)
    if len(nums) < 2:
        return (None, None)
    try:
        return (float(nums[0]), float(nums[1]))
    except Exception:
        return (None, None)


def compute_max_bid(expected_sell: float, sgw_ship: float) -> float:
    net = expected_sell * (1.0 - EBAY_FEE_RATE)
    max_bid = net - sgw_ship - TARGET_PROFIT - BUFFER - SUPPLIES
    return round(max(0.0, max_bid), 2)


def get_best_link_from_embed(msg: discord.Message) -> str | None:
    """Uses the embed URL (SGW link) if present."""
    try:
        if msg.embeds:
            emb = msg.embeds[0]
            url = getattr(emb, "url", None)
            return str(url) if url else None
    except Exception:
        pass
    return None


def get_title_from_embed_or_content(msg: discord.Message) -> str:
    """Best-effort title extraction for DM clarity."""
    try:
        if msg.embeds:
            emb = msg.embeds[0]
            desc = getattr(emb, "description", "") or ""
            t = getattr(emb, "title", "") or ""
            first = desc.splitlines()[0].strip() if desc else ""
            if first.startswith("**") and first.endswith("**") and len(first) > 4:
                return first.strip("*")
            return t or "Saved listing"
    except Exception:
        pass
    return (msg.content or "Saved listing").splitlines()[0][:200]

def clean_comps_query(title: str) -> str:
    """
    Basic cleanup for eBay sold search.
    Removes common lot noise.
    """
    if not title:
        return ""
    t = title.lower()
    noise = ["lot", "untested", "as is", "for parts", "bundle"]
    for n in noise:
        t = t.replace(n, "")
    return " ".join(t.split())


def ebay_sold_url(query: str) -> str:
    """
    Builds an eBay SOLD listings search URL.
    """
    if not query:
        return "https://www.ebay.com/sch/i.html?LH_Sold=1&LH_Complete=1"
    from urllib.parse import quote_plus
    return (
        "https://www.ebay.com/sch/i.html?"
        f"_nkw={quote_plus(query)}&LH_Sold=1&LH_Complete=1"
    )


async def main():
    cfg = load_cfg("config.yaml")

    token = (cfg.get("discord_bot") or {}).get("token", "").strip()
    if not token:
        raise RuntimeError("Missing discord_bot.token in config.yaml")

    mod = cfg.get("moderation") or {}
    if not mod.get("enabled", False):
        raise RuntimeError("moderation.enabled is false in config.yaml")

    browse_channel_id = int(mod["browse_channel_id"])
    best_channel_id = int(mod["best_channel_id"])
    saved_channel_id = int(mod["saved_channel_id"])

    save_emoji = str(mod.get("save_emoji", "✅"))
    dismiss_emoji = str(mod.get("dismiss_emoji", "❌"))
    price_emoji = str(mod.get("price_emoji", PRICE_EMOJI_DEFAULT))

    # Emoji aliases to avoid “variant mismatch”
    SAVE_ALIASES = {save_emoji, "✅", "💾", "🔖", "☑️", "✔️"}
    DISMISS_ALIASES = {dismiss_emoji, "❌", "🗑️"}
    PRICE_ALIASES = {price_emoji, "💰", "🪙"}

    valid_channels = {browse_channel_id, best_channel_id, saved_channel_id}

    intents = discord.Intents.default()
    intents.guilds = True
    intents.messages = True
    intents.reactions = True
    intents.message_content = True  # requires Message Content Intent enabled in the portal

    client = discord.Client(intents=intents)

    @client.event
    async def on_ready():
        print(f"[BOT] Logged in as {client.user}")
        print(f"[BOT] Watching channels: {valid_channels} | saved_channel_id={saved_channel_id}")
        print(f"[BOT] Save emojis: {sorted(SAVE_ALIASES)} | Dismiss emojis: {sorted(DISMISS_ALIASES)} | Price emojis: {sorted(PRICE_ALIASES)}")

    @client.event
    async def on_message(msg: discord.Message):
        # ---- DM replies for pricing ----
        if isinstance(msg.channel, discord.DMChannel) and msg.author and not msg.author.bot:
            state = PENDING_PRICE.get(msg.author.id)
            if not state:
                return

            sell, sgw_ship = parse_two_money_values(msg.content or "")
            if sell is None or sgw_ship is None or sell <= 0 or sgw_ship < 0:
                await msg.channel.send("Reply with: `sell, sgw_ship`  (example: `75, 12.34`)")
                return

            max_bid = compute_max_bid(sell, sgw_ship)

            # Store in DB
            try:
                conn = db_connect()
                ensure_price_notes_table(conn)
                set_price_note(conn, state["listing_id"], sell, sgw_ship, max_bid)
                conn.close()
            except Exception as e:
                await msg.channel.send(f"Saved your numbers, but DB write failed: {e}")

            # Edit the saved embed to show ONLY Max Bid
            try:
                saved_ch = await client.fetch_channel(state["saved_channel_id"])
                saved_msg = await saved_ch.fetch_message(state["saved_message_id"])

                if saved_msg.embeds:
                    emb0 = saved_msg.embeds[0]
                    new_emb = discord.Embed.from_dict(emb0.to_dict())

                    # Remove any existing Max Bid field to avoid duplicates
                    kept = []
                    for f in new_emb.fields:
                        if (f.name or "").strip().lower() != "💰 max bid":
                            kept.append(f)

                    new_emb.clear_fields()
                    for f in kept:
                        new_emb.add_field(name=f.name, value=f.value, inline=f.inline)

                    new_emb.add_field(name="💰 Max Bid", value=f"**${max_bid:,.2f}**", inline=False)
                    await saved_msg.edit(embeds=[new_emb])
                else:
                    await saved_ch.send(f"💰 Max Bid: **${max_bid:,.2f}**\nlisting_id={state['listing_id']}")

            except Exception as e:
                await msg.channel.send(f"Couldn’t update the saved card: {e}")
                return

            PENDING_PRICE.pop(msg.author.id, None)
            await msg.channel.send(f"✅ Max Bid added for listing_id={state['listing_id']}.")
            return

        # ---- Auto-add reactions on new scanner posts (browse/best only) ----
        if msg.author and msg.author.id == client.user.id:
            return

        if msg.channel.id not in {browse_channel_id, best_channel_id}:
            return

        listing_id = extract_listing_id(msg)
        if not listing_id:
            return

        try:
            existing = {str(r.emoji) for r in (msg.reactions or [])}
            if save_emoji not in existing:
                await msg.add_reaction(save_emoji)
            if dismiss_emoji not in existing:
                await msg.add_reaction(dismiss_emoji)
            if price_emoji not in existing:
                await msg.add_reaction(price_emoji)
        except Exception as e:
            print(f"[BOT] Failed to auto-react message_id={msg.id}: {e}")

    @client.event
    async def on_raw_reaction_add(payload: discord.RawReactionActionEvent):
        if payload.user_id == client.user.id:
            return

        emoji = str(payload.emoji)
        print(f"[BOT] Reaction received: {emoji!r} channel_id={payload.channel_id} message_id={payload.message_id}")

        if payload.channel_id not in valid_channels:
            print("[BOT] Ignored: not a watched channel")
            return

        if emoji not in (SAVE_ALIASES | DISMISS_ALIASES | PRICE_ALIASES):
            print("[BOT] Ignored: emoji not recognized as save/dismiss/price")
            return

        channel = await client.fetch_channel(payload.channel_id)
        msg = await channel.fetch_message(payload.message_id)

        listing_id = extract_listing_id(msg)
        print(f"[BOT] listing_id parsed: {listing_id!r}")

        conn = db_connect()
        ensure_user_actions_table(conn)

        # PRICE (💰): auto-save (if needed), then DM for sell + SGW ship
        if emoji in PRICE_ALIASES:
            if not listing_id:
                print("[BOT] No listing_id found; cannot price reliably.")
                conn.close()
                return

            # Record save so scanner won't repost
            set_user_action(conn, listing_id, "save")
            print("[BOT] Action recorded: save (via price)")
            conn.close()

            # Fetch saved channel
            try:
                saved_ch = await client.fetch_channel(saved_channel_id)
            except Exception as e:
                print(f"[BOT] FAILED to fetch saved channel. Error: {e}")
                return

            # Ensure saved copy exists
            try:
                if payload.channel_id == saved_channel_id:
                    saved_msg = msg
                else:
                    if msg.embeds:
                        saved_msg = await saved_ch.send(content="✅ **Saved for later**", embeds=msg.embeds)
                    else:
                        content = msg.content or ""
                        saved_msg = await saved_ch.send(f"✅ **Saved for later**\n{content}")

                    # Add reactions on saved copy
                    await saved_msg.add_reaction(save_emoji)
                    await saved_msg.add_reaction(dismiss_emoji)
                    await saved_msg.add_reaction(price_emoji)

                    # Delete original
                    try:
                        await msg.delete()
                        print("[BOT] Deleted original message (price->save).")
                    except Exception as e:
                        print(f"[BOT] Failed to delete original after price-save: {e}")

            except Exception as e:
                print(f"[BOT] FAILED to create saved copy for pricing. Error: {e}")
                return

            # DM prompt (one active at a time)
            user = await client.fetch_user(payload.user_id)

            if user.id in PENDING_PRICE:
                try:
                    await user.send("⚠️ You already have a pending pricing request. Reply to that DM first, then click 💰 again.")
                except Exception:
                    pass
                return

            title = get_title_from_embed_or_content(saved_msg)
            link = get_best_link_from_embed(saved_msg)
            comps_q = clean_comps_query(title)
            comps_url = ebay_sold_url(comps_q)

            PENDING_PRICE[user.id] = {
                "listing_id": listing_id,
                "saved_channel_id": saved_ch.id,
                "saved_message_id": saved_msg.id,
            }

            try:
                link_line = f"\nLink: {link}" if link else ""
                await user.send(
                    "💰 **Price this item**\n"
                    f"**{title}**\n"
                    f"listing_id={listing_id}"
                    f"{link_line}\n"
                    f"eBay SOLD: {comps_url}\n\n"
                    "Reply with: `sell, sgw_ship`  (example: `75, 12.34`)\n"
                    "I’ll compute **Max Bid** and update the saved card (only Max Bid will be shown)."
                )
                print("[BOT] DM prompt sent for pricing.")
            except Exception as e:
                print(f"[BOT] Could not DM user for pricing: {e}")

            return

        # DISMISS
        if emoji in DISMISS_ALIASES:
            if listing_id:
                set_user_action(conn, listing_id, "dismiss")
                print("[BOT] Action recorded: dismiss")
            else:
                print("[BOT] No listing_id found; dismiss will delete message but cannot blacklist forever.")
            conn.close()

            try:
                await msg.delete()
                print("[BOT] Deleted message (dismiss).")
            except Exception as e:
                print(f"[BOT] Failed to delete message: {e}")
            return

        # SAVE
        if emoji in SAVE_ALIASES:
            if listing_id:
                set_user_action(conn, listing_id, "save")
                print("[BOT] Action recorded: save")
            else:
                print("[BOT] No listing_id found; save will still copy, but cannot prevent re-show reliably.")
            conn.close()

            try:
                saved_ch = await client.fetch_channel(saved_channel_id)
            except Exception as e:
                print(f"[BOT] FAILED to fetch saved channel. Check saved_channel_id. Error: {e}")
                return

            # Copy message to saved channel (preserve embeds) + add reactions on saved copy
            try:
                if msg.embeds:
                    saved_msg = await saved_ch.send(content="✅ **Saved for later**", embeds=msg.embeds)
                else:
                    content = msg.content or ""
                    saved_msg = await saved_ch.send(f"✅ **Saved for later**\n{content}")

                await saved_msg.add_reaction(save_emoji)
                await saved_msg.add_reaction(dismiss_emoji)
                await saved_msg.add_reaction(price_emoji)

                print("[BOT] Posted to #saved-for-later + added reactions.")
            except Exception as e:
                print(f"[BOT] FAILED to post to saved channel (permissions/ID). Error: {e}")
                return

            # Delete original to keep feed clean
            try:
                await msg.delete()
                print("[BOT] Deleted original message (save).")
            except Exception as e:
                print(f"[BOT] Failed to delete original after save: {e}")
            return

    await client.start(token)


if __name__ == "__main__":

    asyncio.run(main())
