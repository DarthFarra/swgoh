from swgoh.bot.services.gt_prep import (
    PlayerPrep,
    StatFailure,
    _combined_unit_stats,
    parse_requirement,
    render_player_prep,
    render_guild_prep,
    render_player_prep,
)


def test_parse_hard_and_recommended_requirement():
    assert parse_requirement("100000") == (100000.0, None)
    assert parse_requirement("310 (340)") == (310.0, 340.0)
    assert parse_requirement("(340)") == (None, 340.0)
    assert parse_requirement("") == (None, None)


def test_combined_unit_stats_add_base_and_mods():
    stats = {
        "base": {
            "UnitStat_Health": 50939,
            "UnitStat_MaxShield": 57732,
            "UnitStat_Speed": 164,
            "UnitStat_Accuracy": 0.59,
        },
        "mods": {
            "UnitStat_Health": 17813,
            "UnitStat_MaxShield": 13698,
            "UnitStat_Speed": 143,
            "UnitStat_Accuracy": 0.08492,
        },
    }
    actual = _combined_unit_stats({"stats": stats})
    assert actual["Health"] == 68752
    assert actual["Protection"] == 71430
    assert actual["Speed"] == 307
    assert abs(actual["Potency"] - 0.67492) < 1e-6


def test_player_prep_hard_and_recommendation():
    prep = PlayerPrep(
        "Mock Player",
        (
            StatFailure("Leia Organa", "Health", 68752, 100000, True),
            StatFailure("Jyn Erso", "Speed", 313, 340, False),
        ),
    )
    report = render_player_prep(prep, telegram=False)
    assert "Leia Organa:" in report
    assert "❌ Salud: 68.752" in report
    assert "Mínimo 100.000" in report
    assert "Jyn Erso:" in report
    assert "⚠️ Velocidad: 313 (Recomendado 340)" in report
    assert "Esta cuenta no se puede apuntar a GT hasta que no resuelva los stats marcados con ❌" in report


def test_guild_prep_marks_hard_only_in_red():
    preps = [
        PlayerPrep(
            "Alpha",
            (StatFailure("Leia Organa", "Health", 50000, 100000, True),),
        ),
        PlayerPrep(
            "Beta",
            (StatFailure("Jyn Erso", "Speed", 313, 340, False),),
        ),
    ]
    report = render_guild_prep("Guild", preps, telegram=False)
    assert "❌ Alpha" in report
    assert "Leia Organa — Salud" in report
    assert "✅ Beta" in report


def test_positive_and_recommended_end_messages():
    fully_ready = PlayerPrep("Ready", ())
    report = render_player_prep(fully_ready, telegram=False)
    assert "Esta cuenta se puede apuntar a GT" in report
    assert "👀" not in report

    only_recommended = PlayerPrep(
        "Almost",
        (StatFailure("Jyn Erso", "Speed", 313, 340, False),),
    )
    report = render_player_prep(only_recommended, telegram=False)
    assert "Esta cuenta se puede apuntar a GT pero ojo a los stats recomendados 👀" in report
