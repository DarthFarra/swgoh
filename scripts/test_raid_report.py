from swgoh.bot.services.raid_report import (
    RaidHistoryRow,
    parse_raid_threshold,
    render_raid_report,
    raid_name,
)


def test_raid_report_header_and_sections():
    rows = [
        RaidHistoryRow("2026-11-15", "1", "Alpha", "droiddestruction", 11_000_000, 10, "", "Guild"),
        RaidHistoryRow("2026-11-15", "2", "Beta", "droiddestruction", 9_000_000, 10, "", "Guild"),
        RaidHistoryRow("2026-11-15", "3", "Gamma", "droiddestruction", 8_000_000, 7, "", "Guild"),
        RaidHistoryRow("2026-11-09", "1", "Alpha", "droiddestruction", 10_000_000, 8, "", "Guild"),
        RaidHistoryRow("2026-11-09", "2", "Beta", "droiddestruction", 7_000_000, 8, "", "Guild"),
        RaidHistoryRow("2026-11-09", "3", "Gamma", "droiddestruction", 8_000_000, 9, "", "Guild"),
    ]
    report = render_raid_report(
        "Guild",
        rows,
        {"alpha": "alpha_tg", "beta": None, "gamma": None},
        10_400_000,
        7_500_000,
    )

    assert report is not None
    assert report.startswith(
        "<b>Informe del último asalto; Destrucción droide – Guild</b>\n15/11/2026"
    )
    assert "2 (67%) han llegado a los 10,4M requeridos." not in report
    assert "<b>1 (33%)</b> han llegado a los 10,4M requeridos." in report
    assert "por quienes no llegan a los 10,4M: 2" in report
    assert "<b>Con 2 reliquias subidas</b>: Beta" in report
    assert "Beta" in report
    assert "Gamma" not in report.split(
        "Jugadores que no han llegado a 7,5M",
        1,
    )[1]


def test_raid_name_derivation():
    assert raid_name("order66") == "La venganza de los Sith"
    assert raid_name("droiddestruction") == "Destrucción droide"
    assert raid_name("unknown_raid") == "Unknown Raid"


def test_raid_threshold_parser():
    assert parse_raid_threshold("10,4M") == 10_400_000
    assert parse_raid_threshold("7.5M") == 7_500_000
    assert parse_raid_threshold("10400000") == 10_400_000


def test_discord_mentions_use_native_format():
    rows = [
        RaidHistoryRow("2026-11-15", "1", "Alpha", "order66", 7_000_000, 8, "", "Guild"),
        RaidHistoryRow("2026-11-09", "1", "Alpha", "order66", 8_000_000, 8, "", "Guild"),
    ]
    report = render_raid_report(
        "Guild",
        rows,
        {"alpha": "<@123456>"},
        10_400_000,
        7_500_000,
        "discord",
    )
    assert report is not None
    assert "Alpha <@123456>" in report
    assert "Alpha @123456" not in report


def test_missing_player_from_current_roster_is_not_rendered():
    rows = [
        RaidHistoryRow("2026-11-15", "old-id", "Former Member", "droiddestruction", 5_000_000, 100, "", "Guild"),
        RaidHistoryRow("2026-11-15", "active-id", "Active Member", "droiddestruction", 8_000_000, 100, "", "Guild"),
    ]
    known_player_ids = {"active-id"}
    filtered = [
        row for row in rows
        if row.member_id in known_player_ids
    ]
    report = render_raid_report(
        "Guild",
        filtered,
        {},
        10_400_000,
        7_500_000,
    )
    assert report is not None
    assert "Former Member" not in report
    assert "old-id" not in report
