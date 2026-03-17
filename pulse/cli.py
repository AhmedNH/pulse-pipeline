"""
cli.py — Rich terminal dashboard for Pulse.

Displays live data from the local SQLite database.
"""

from __future__ import annotations

from datetime import datetime

from rich.columns import Columns
from rich.console import Console
from rich.panel import Panel
from rich.table import Table
from rich import box

from pulse import db

console = Console()

# ── Weather code descriptions (WMO standard) ───────────────────────────────────

WEATHER_DESCRIPTIONS: dict[int, str] = {
    0: "Clear sky",
    1: "Mainly clear",  2: "Partly cloudy",  3: "Overcast",
    45: "Fog",          48: "Icy fog",
    51: "Light drizzle",53: "Drizzle",       55: "Heavy drizzle",
    61: "Light rain",   63: "Rain",           65: "Heavy rain",
    71: "Light snow",   73: "Snow",           75: "Heavy snow",
    80: "Showers",      81: "Rain showers",   82: "Violent showers",
    95: "Thunderstorm", 99: "Hail storm",
}


def _weather_icon(code: int) -> str:
    if code == 0:              return "☀️"
    if code in (1, 2):         return "🌤️"
    if code == 3:              return "☁️"
    if code in (45, 48):       return "🌫️"
    if code in (51, 53, 55):   return "🌦️"
    if code in (61, 63, 65):   return "🌧️"
    if code in (71, 73, 75):   return "❄️"
    if code in (80, 81, 82):   return "🌦️"
    if code in (95, 99):       return "⛈️"
    return "🌡️"


def _fmt_change(change: float | None) -> str:
    if change is None:
        return "[dim]N/A[/dim]"
    color = "green" if change >= 0 else "red"
    arrow = "▲" if change >= 0 else "▼"
    return f"[{color}]{arrow} {abs(change):.2f}%[/{color}]"


def _fmt_price(price: float) -> str:
    if price >= 1000:
        return f"${price:,.2f}"
    if price >= 1:
        return f"${price:.4f}"
    return f"${price:.6f}"


def _fmt_large(n: float | None) -> str:
    if n is None:
        return "[dim]N/A[/dim]"
    if n >= 1e12:
        return f"${n/1e12:.2f}T"
    if n >= 1e9:
        return f"${n/1e9:.2f}B"
    if n >= 1e6:
        return f"${n/1e6:.2f}M"
    return f"${n:,.0f}"


# ── Weather dashboard ──────────────────────────────────────────────────────────

def show_weather() -> None:
    """Print a table of the latest weather snapshot per city."""
    from pulse.fetcher import CITIES

    table = Table(
        title    = "🌍  Current Weather",
        box      = box.ROUNDED,
        show_header = True,
        header_style = "bold cyan",
    )
    table.add_column("City",        style="bold white", min_width=12)
    table.add_column("Condition",   min_width=18)
    table.add_column("Temp (°C)",   justify="right")
    table.add_column("Wind (km/h)", justify="right")
    table.add_column("Updated",     style="dim",  min_width=20)

    for city in CITIES:
        row = db.get_latest_weather(city)
        if row is None:
            table.add_row(city, "[dim]No data[/dim]", "—", "—", "—")
            continue

        code  = row["weather_code"]
        desc  = WEATHER_DESCRIPTIONS.get(code, f"Code {code}")
        icon  = _weather_icon(code)
        temp  = row["temperature"]
        wind  = row["wind_speed"]
        ts    = row["fetched_at"]

        temp_color = "red" if temp > 30 else ("blue" if temp < 5 else "yellow")
        table.add_row(
            city,
            f"{icon} {desc}",
            f"[{temp_color}]{temp:.1f}[/{temp_color}]",
            f"{wind:.1f}",
            ts,
        )

    console.print(table)


def show_weather_stats(city: str) -> None:
    stats = db.get_weather_stats(city)
    if stats is None:
        console.print(f"[red]No data for '{city}'.[/red]")
        return

    panel_content = (
        f"[bold]Readings:[/bold]  {stats['readings']}\n"
        f"[bold]Min Temp:[/bold]  {stats['temp_min']}°C\n"
        f"[bold]Max Temp:[/bold]  {stats['temp_max']}°C\n"
        f"[bold]Avg Temp:[/bold]  {stats['temp_avg']}°C\n"
        f"[bold]Since:[/bold]     {stats['first_seen']}\n"
        f"[bold]Last:[/bold]      {stats['last_seen']}"
    )
    console.print(Panel(panel_content, title=f"📊 Stats — {city}", expand=False))


# ── Crypto dashboard ───────────────────────────────────────────────────────────

def show_crypto() -> None:
    """Print a table of the latest snapshot for every tracked coin."""
    rows = db.get_all_latest_crypto()
    if not rows:
        console.print("[red]No crypto data found. Run: python main.py run[/red]")
        return

    table = Table(
        title        = "₿  Crypto Markets",
        box          = box.ROUNDED,
        header_style = "bold cyan",
    )
    table.add_column("Coin",       style="bold white", min_width=10)
    table.add_column("Symbol",     justify="center",   min_width=6)
    table.add_column("Price",      justify="right",    min_width=14)
    table.add_column("24h",        justify="right",    min_width=10)
    table.add_column("Market Cap", justify="right",    min_width=12)
    table.add_column("Volume 24h", justify="right",    min_width=12)
    table.add_column("Updated",    style="dim",        min_width=20)

    for row in rows:
        table.add_row(
            row["coin_id"].capitalize(),
            row["symbol"],
            _fmt_price(row["price_usd"]),
            _fmt_change(row["change_24h"]),
            _fmt_large(row["market_cap"]),
            _fmt_large(row["volume_24h"]),
            row["fetched_at"],
        )

    console.print(table)


def show_crypto_stats(coin_id: str) -> None:
    stats = db.get_crypto_stats(coin_id)
    if stats is None:
        console.print(f"[red]No data for '{coin_id}'.[/red]")
        return

    panel_content = (
        f"[bold]Symbol:[/bold]    {stats['symbol']}\n"
        f"[bold]Readings:[/bold]  {stats['readings']}\n"
        f"[bold]Min Price:[/bold] {_fmt_price(stats['price_min'])}\n"
        f"[bold]Max Price:[/bold] {_fmt_price(stats['price_max'])}\n"
        f"[bold]Avg Price:[/bold] {_fmt_price(stats['price_avg'])}\n"
        f"[bold]Since:[/bold]     {stats['first_seen']}\n"
        f"[bold]Last:[/bold]      {stats['last_seen']}"
    )
    console.print(
        Panel(panel_content, title=f"📊 Stats — {coin_id.capitalize()}", expand=False)
    )


# ── Summary view ───────────────────────────────────────────────────────────────

def show_summary() -> None:
    """Show both weather and crypto tables side by side in the terminal."""
    console.rule("[bold cyan]Pulse — Live Data Dashboard[/bold cyan]")
    show_weather()
    console.print()
    show_crypto()
    console.print()
    console.print(
        f"[dim]Data sourced from Open-Meteo and CoinGecko · "
        f"Last rendered: {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')} UTC[/dim]"
    )
